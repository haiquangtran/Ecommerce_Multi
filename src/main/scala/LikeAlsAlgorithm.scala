package org.template.ecommercerecommendation

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap
import io.prediction.data.storage.Event
import io.prediction.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

case class WotmParams(
  appName: String,
  unseenOnly: Boolean,
  seenEvents: List[String],
  similarEvents: List[String],
  rank: Int, // Number of latent features
  numIterations: Int,
  lambda: Double,   // Regularization parameter for MLlib ALS 
  alpha: Double,    // Confidence 
  seed: Option[Long],  // Random seed for ALS. Specify a fixed value if want to have deterministic result
  preferenceWeight: Double // Weights for content of preferences
) extends Params


case class ProductModel(
  item: Item,
  features: Option[Array[Double]], // features by ALS
  count: Double // popular count for default score
)

class WotmModel(
  val rank: Int,
  val userFeatures: Map[Int, Array[Double]],
  val productModels: Map[Int, ProductModel],
  val userStringIntMap: BiMap[String, Int],
  val itemStringIntMap: BiMap[String, Int]
) extends Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
    s" userFeatures: [${userFeatures.size}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productModels: [${productModels.size}]" +
    s"(${productModels.take(2).toList}...)" +
    s" userStringIntMap: [${userStringIntMap.size}]" +
    s"(${userStringIntMap.take(2).toString}...)]" +
    s" itemStringIntMap: [${itemStringIntMap.size}]" +
    s"(${itemStringIntMap.take(2).toString}...)]"
  }
}

/**
  * Use ALS to build item x feature matrix
  */
class LikeAlsAlgorithm(val ap: WotmParams)
  extends P2LAlgorithm[PreparedData, WotmModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): WotmModel = {
    require(!data.likeEvents.take(1).isEmpty,
      s"likeEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.users.take(1).isEmpty,
      s"users in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    // create User and item's String ID to integer index BiMap
    val userStringIntMap = BiMap.stringInt(data.users.keys)
    val itemStringIntMap = BiMap.stringInt(data.items.keys)

    val mllibRatings: RDD[MLlibRating] = genMLlibRating(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = data
    )

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
      " Please check if your events contain valid user and item ID.")

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    // use ALS to train feature vectors
    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = ap.alpha,
      seed = seed
    )

    val userFeatures = m.userFeatures.collectAsMap.toMap

    // convert ID to Int index
    val items = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }

    // join item with the trained productFeatures
    val productFeatures: Map[Int, (Item, Option[Array[Double]])] =
      items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    val productModels: Map[Int, ProductModel] = productFeatures
      .map { case (index, (item, features)) =>
        val pm = ProductModel(
          item = item,
          features = features,
          count = popularScore(item)  // Wilson Confidence Interval       
        )
        (index, pm)
      }

    new WotmModel(
      rank = m.rank,
      userFeatures = userFeatures,
      productModels = productModels,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap
    )
  }

  /** Generate MLlibRating from PreparedData.
    * You may customize this function if use different events or different aggregation method
    */
  def genMLlibRating(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): RDD[MLlibRating] = {

    val mllibRatings = data.likeEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), (r.rating, r.t))
      }
      .filter { case ((u, i), rating) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey { case (v1 ,v2) =>
        // use the latest value of the timestamp for same events on same items.
        val (rating1, t1) = v1
        val (rating2, t2) = v2
        // keep the latest value
        if (t1 > t2) v1 else v2
      }
      .map { case ((u, i), (rating, t)) =>
        // MLlibRating requires integer index for user and item
        // takes into account like events and dislike event ratings here
        MLlibRating(u, i, rating) 
      }
      .cache()

    mllibRatings
  }

  /** Train default model.
    * You may customize this function if use different events or
    * need different ways to count "popular" score or return default score for item.
    */
  def trainDefault(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): Map[Int, Int] = {
    // count number of likes
    // (item index, count)
    val likeCountsRDD: RDD[(Int, Int)] = data.likeEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        (uindex, iindex, 1)
      }
      .filter { case (u, i, v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .map { case (u, i, v) => (i, 1) } // key is item
      .reduceByKey{ case (a, b) => a + b } // count number of items occurrence

    likeCountsRDD.collectAsMap.toMap
  }

  def predict(model: WotmModel, query: Query): PredictedResult = {

    val userFeatures = model.userFeatures
    val productModels = model.productModels

    // content preferences
    val preferences: Option[Set[String]] = query.positivePreferences
    // determines if popular items are returned
    val isPopular: Option[Boolean] = query.popular

    // convert whiteList's string ID to integer index
    val whiteList: Option[Set[Int]] = query.whiteList.map( set =>
      set.flatMap(model.itemStringIntMap.get(_))
    )

    val finalBlackList: Set[Int] = genBlackList(query = query)
      // convert seen Items list from String ID to integer Index
      .flatMap(x => model.itemStringIntMap.get(x))

    val userFeature: Option[Array[Double]] =
      model.userStringIntMap.get(query.user).flatMap { userIndex =>
        userFeatures.get(userIndex)
      }

    val topScores: Array[(Int, Double)] = 
    if (isPopular.isDefined && isPopular.get) {
      // Recommend popular items
      predictPopular(
        productModels = productModels,
        query = query,
        whiteList = whiteList,
        blackList = finalBlackList,
        minPrice = query.minPrice,
        maxPrice = query.maxPrice
      )
    } else if (userFeature.isDefined && query.items.isEmpty) {
      // the user has feature vector
      predictKnownUser(
        userFeature = userFeature.get,
        productModels = productModels,
        query = query,
        preferences = preferences,
        whiteList = whiteList,
        blackList = finalBlackList,
        minPrice = query.minPrice,
        maxPrice = query.maxPrice
      )
    } else {
      // the user doesn't have feature vector.
      // For example, new user is created after model is trained.
      logger.info(s"No userFeature found for user ${query.user}.")

      // check if the user has recent events on some items
      val recentItems: Set[String] = 
      if (!query.items.isEmpty && query.items.isDefined) { 
        query.items.get
      } else { 
        getRecentItems(query)
      }
      val recentList: Set[Int] = recentItems.flatMap (x =>
        model.itemStringIntMap.get(x))

      val recentFeatures: Vector[Array[Double]] = recentList.toVector
        // productModels may not contain the requested item
        .map { i =>
          productModels.get(i).flatMap { pm => pm.features }
        }.flatten

      if (!recentFeatures.isEmpty) {
        // Recommend similar items
        predictSimilar(
          recentFeatures = recentFeatures,
          productModels = productModels,
          query = query,
          preferences = preferences, 
          whiteList = whiteList,
          blackList = finalBlackList,
          minPrice = query.minPrice,
          maxPrice = query.maxPrice
        )
      } else if (!preferences.isEmpty) {
        // Recommend based on content
        predictContent(
          productModels = productModels,
          query = query,
          preferences = preferences,
          whiteList = whiteList,
          blackList = finalBlackList,
          minPrice = query.minPrice,
          maxPrice = query.maxPrice
        )
      } else {
        // This is the default if user has no information we can use
        // Recommend popular items
        logger.info(s"No features vector for recent items ${recentItems}.")
        predictPopular(
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList,
          minPrice = query.minPrice,
          maxPrice = query.maxPrice
        )
      }
    }

    val itemScores = topScores.map { case (i, s) =>
      new ItemScore(
        // convert item int index back to string ID
        item = model.itemIntStringMap(i),
        name = productModels.get(i).get.item.name,
        score = s,
        categories = productModels.get(i).get.item.categories,
        price = productModels.get(i).get.item.price,
        likes = productModels.get(i).get.item.likes,
        dislikes = productModels.get(i).get.item.dislikes,
        average_rating = productModels.get(i).get.item.average_rating
      )
    }

    new PredictedResult(itemScores)
  }

  /** Generate final blackList based on other constraints */
  /** Final blacklist = seen items, unavailable items, and blacklist items */
  def genBlackList(query: Query): Set[String] = {
    // if unseenOnly is True, get all seen items
    val seenItems: Set[String] = if (ap.unseenOnly) {

      // get all user item events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user,
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some("item")),
          // set time limit to avoid super long DB access
          timeout = Duration(200, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get
        } catch {
          case e => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[String]()
    }

    // get the latest constraint unavailableItems $set event
    val unavailableItems: Set[String] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Set[String]]("items")
      } else {
        Set[String]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableItems event." +
          s" Empty list is used. ${e}")
        Set[String]()
      case e: Exception =>
        logger.error(s"Error when read set unavailableItems event: ${e}")
        throw e
    }

    // combine query's blackList,seenItems and unavailableItems
    // into final blackList.
    query.blackList.getOrElse(Set[String]()) ++ seenItems ++ unavailableItems
  }

  /** Get recent events of the user on items for recommending similar items */
  def getRecentItems(query: Query): Set[String] = {
    // get latest 10 user like item events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(ap.similarEvents),
        targetEntityType = Some(Some("item")),
        limit = Some(10),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }

  /** Prediction for user with known feature vector */
  def predictKnownUser(
    userFeature: Array[Double],
    productModels: Map[Int, ProductModel],
    query: Query,
    preferences: Option[Set[String]],
    whiteList: Option[Set[Int]],
    blackList: Set[Int],
    minPrice: Option[Double],
    maxPrice: Option[Double]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList,
          minPrice = query.minPrice,
          maxPrice = query.maxPrice
        )
      }
      .map { case (i, pm) =>
        // NOTE: features must be defined, so can call .get
        val score = dotProduct(userFeature, pm.features.get)
        val contentScore = getContentBasedScore(i, pm.item, preferences)
        
        (i, score + contentScore)
      }
      .filter(_._2 > 0) // only keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Popular prediction when know nothing about the user */
  def predictPopular(
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int],
    minPrice: Option[Double],
    maxPrice: Option[Double]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert back to sequential collection
      .filter { case (i, pm) =>
        isCandidateItem(
          i = i,
          item = pm.item,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList,
          minPrice = query.minPrice,
          maxPrice = query.maxPrice
        )
      }
      .map { case (i, pm) =>
        // Use popularity score
        (i, pm.count.toDouble)
      }
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Predict using content attributes */
  def predictContent(
    productModels: Map[Int, ProductModel],
    query: Query,
    preferences: Option[Set[String]],
    whiteList: Option[Set[Int]],
    blackList: Set[Int],
    minPrice: Option[Double],
    maxPrice: Option[Double]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert back to sequential collection
      .filter { case (i, pm) =>
        isCandidateItem(
          i = i,
          item = pm.item,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList,
          minPrice = query.minPrice,
          maxPrice = query.maxPrice
        )
      }
      .map { case (i, pm) =>
        // Use content based filtering
        val contentScore = getContentBasedScore(i, pm.item, preferences)

        (i, contentScore)
      }
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Return top similar items based on items user recently has action on */
  def predictSimilar(
    recentFeatures: Vector[Array[Double]],
    productModels: Map[Int, ProductModel],
    query: Query,
    preferences: Option[Set[String]],
    whiteList: Option[Set[Int]],
    blackList: Set[Int],
    minPrice: Option[Double],
    maxPrice: Option[Double]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList,
          minPrice = query.minPrice,
          maxPrice = query.maxPrice
        )
      }
      .map { case (i, pm) =>
        val score = recentFeatures.map{ rf =>
          // pm.features must be defined because of filter logic above
          cosine(rf, pm.features.get)
        }.reduce(_ + _)

        val contentScore = getContentBasedScore(i, pm.item, preferences)

        (i, score + contentScore)
      }
      .filter(_._2 > 0) // keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  private
  def getTopN[T](s: Iterable[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {
    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private
  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var result: Double = 0
    while (i < size) {
      result += v1(i) * v2(i)
      i += 1
    }

    result
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private
  def isCandidateItem(
    i: Int,
    item: Item,
    categories: Option[Set[String]],
    whiteList: Option[Set[Int]],
    blackList: Set[Int],
    minPrice: Option[Double],
    maxPrice: Option[Double]
  ): Boolean = {
    // can add other custom filtering here
    whiteList.map(_.contains(i)).getOrElse(true) &&
    !blackList.contains(i) &&
    item.price >= minPrice.getOrElse(0.0) && 
    item.price <= maxPrice.getOrElse(Double.PositiveInfinity) &&
    // filter categories
    categories.map { cat =>
      item.categories.map { itemCat =>
        // keep this item if has overlap categories with the query
        !(itemCat.toSet.intersect(cat).isEmpty)
      }.getOrElse(false) // discard this item if it has no categories
    }.getOrElse(true)
  }

  /* Wilsons Confidence Interval: used for popularity score.*/
  private
  def popularScore(
    item: Item
  ): Double = {
    val likes: Double = item.likes
    val dislikes: Double = item.dislikes
    val n: Double = likes + dislikes
    val z: Double = 1.96 // 95% confidence interval
    val pHat: Double = 1.0*(likes/n)
    val wilsonConfidenceInterval: Double = (pHat + z*z/(2*n) - z * Math.sqrt((pHat*(1-pHat)+z*z/(4*n))/n))/(1+z*z/n)

    if (n == 0) {
      return 0
    } else {
      return wilsonConfidenceInterval;
    }
  }

  def getContentBasedScore(
    i: Int,
    item: Item,
    preferences: Option[Set[String]]
  ): Double = {
    val weight = ap.preferenceWeight

    // boost items with preferences
    preferences.map { preference =>
      item.categories.map { itemCat =>
        val preferredDishes = itemCat.toSet.intersect(preference)

        // items with the users content preferences
        if (!(preferredDishes.isEmpty)) {
          // Content boost
          return (preferredDishes.size * weight)
        }
      } 
    }
    // No boost
    return 0.0
  }

}
