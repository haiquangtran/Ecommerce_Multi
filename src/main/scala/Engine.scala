package org.template.ecommercerecommendation

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  user: String,
  num: Int,
  categories: Option[Set[String]],
  whiteList: Option[Set[String]],
  blackList: Option[Set[String]],
  minPrice: Option[Double],
  maxPrice: Option[Double]
) extends Serializable

case class PredictedResult(
  itemScores: Array[ItemScore]
) extends Serializable

case class ItemScore(
  name: String,
  item: String,
  score: Double,
  categories: Option[List[String]],
  price: Double,
  likes: Int,
  dislikes: Int,
  wants: Int,
  average_rating: Double
) extends Serializable

object ECommerceRecommendationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("ecomm" -> classOf[ECommAlgorithm]),
      classOf[Serving])
  }
}
