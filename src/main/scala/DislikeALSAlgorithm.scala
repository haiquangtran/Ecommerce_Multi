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

/**
  * Use ALS to build item x feature matrix
  */
class DislikeALSAlgorithm(ap: ECommAlgorithmParams) extends ECommAlgorithm(ap) {

  override
  def train(sc: SparkContext, data: PreparedData): ECommModel = {
    require(!data.dislikeEvents.take(1).isEmpty,
      s"dislikeEvents in PreparedData cannot be empty." +
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
        count = 0.0  // Wilson Confidence Interval       
      )

      (index, pm)
    }

    new ECommModel(
      rank = m.rank,
      userFeatures = userFeatures,
      productModels = productModels,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap
    )
  }

  /** 
    * Generate MLlibRating from PreparedData.
    */
  override
  def genMLlibRating(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): RDD[MLlibRating] = {

    val mllibRatings = data.dislikeEvents
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
      // if a user may rate same item with different value at different times,
      // use the latest value for this case.
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

  override
  def predict(model: ECommModel, query: Query): PredictedResult = {
    val userFeatures = model.userFeatures
    val productModels = model.productModels

    // negative content preferences
    val negativePreferences: Option[Set[String]] = query.negativePreferences

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
    if (userFeature.isDefined) {
      // the user has feature vector
      predictKnownUser(
        userFeature = userFeature.get,
        productModels = productModels,
        query = query,
        preferences = negativePreferences,  
        whiteList = whiteList,
        blackList = finalBlackList,
        minPrice = query.minPrice,
        maxPrice = query.maxPrice
        )
    } else if (!negativePreferences.isEmpty) {
      // recommendations based on content
      predictContent(
        productModels = productModels,
        query = query,
        preferences = negativePreferences,  
        whiteList = whiteList,
        blackList = finalBlackList,
        minPrice = query.minPrice,
        maxPrice = query.maxPrice
        )
    } else {
      // the user doesn't have feature vector.
      // For example, new user is created after model is trained.
      // logger.info(s"No userFeature found for user ${query.user}.")

      Array[(Int, Double)]()
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

}
