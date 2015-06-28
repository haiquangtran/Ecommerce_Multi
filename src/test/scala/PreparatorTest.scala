package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class PreparatorTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val preparator = new Preparator()
  val users = Map(
    "u0" -> User(),
    "u1" -> User()
  )

  val items = Map(
    "i0" -> Item(categories = Some(List("c0", "c1")), name = "dish1", price = 10.00, likes = 1, dislikes = 1, wants = 0, average_rating = 2.5),
    "i1" -> Item(categories = None, name = "dish2", price = 15.00, likes = 2, dislikes = 2, wants = 0, average_rating = 2.5)
  )

  val like = Seq(
    LikeEvent("u0", "i0", 1000010),
    LikeEvent("u0", "i1", 1000020),
    LikeEvent("u1", "i1", 1000030)
  )

  val dislike = Seq(
    DislikeEvent("u0", "i0", 1000020),
    DislikeEvent("u0", "i1", 1000030),
    DislikeEvent("u1", "i1", 1000040)
  )

  val rating = Seq(
    // Likes
    RatingEvent("u0", "i0", 1.0, 1000010),
    RatingEvent("u0", "i1", 1.0, 1000020),
    RatingEvent("u0", "i1", 1.0, 1000020),
    RatingEvent("u1", "i1", 1.0, 1000030),
    RatingEvent("u1", "i2", 1.0, 1000040),
    // Dislikes
    RatingEvent("u0", "i0", -1.0, 1000020),
    RatingEvent("u0", "i1", -1.0, 1000030),
    RatingEvent("u1", "i1", -1.0, 1000040)
  )

  // simple test for demonstration purpose
  "Preparator" should "prepare PreparedData" in {

    val trainingData = new TrainingData(
      users = sc.parallelize(users.toSeq),
      items = sc.parallelize(items.toSeq),
      likeEvents = sc.parallelize(like.toSeq),
      dislikeEvents = sc.parallelize(dislike.toSeq),
      ratingEvents = sc.parallelize(rating.toSeq)
    )

    val preparedData = preparator.prepare(sc, trainingData)

    preparedData.users.collect should contain theSameElementsAs users
    preparedData.items.collect should contain theSameElementsAs items
    preparedData.likeEvents.collect should contain theSameElementsAs like
    preparedData.dislikeEvents.collect should contain theSameElementsAs dislike
    preparedData.ratingEvents.collect should contain theSameElementsAs rating
  }
}
