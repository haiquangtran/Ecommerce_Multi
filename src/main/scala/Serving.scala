package org.template.ecommercerecommendation

import io.prediction.controller.LServing
import breeze.stats.mean
import breeze.stats.meanAndVariance
import breeze.stats.MeanAndVariance
import io.prediction.controller.Params  

case class ServingParams(filepath: String) extends Params

class Serving
  extends LServing[Query, PredictedResult] {

  override
  def serve(query: Query, predictedResults: Seq[PredictedResult]): PredictedResult = {

    val standard: Seq[Array[ItemScore]] = if (query.num == 1) {
    	//if query 1 item
    	predictedResults.map(_.itemScores)
    } else {
    	predictedResults.zipWithIndex
    		.map { case (pr, i) =>
    			pr.itemScores.map { is => 
            		ItemScore(
            			is.name,
            			is.item, 
            			is.score,
            			is.categories,
            			is.price,
            			is.likes,
            			is.dislikes,
            			is.wants,
            			is.average_rating
            		)
          		}
    		}
    }

    // Array of ItemScore
    val itemArray = standard.flatten
	// sum the standardized score if same item
    val combined = itemArray.groupBy { case (item) => (item.name, item.item, item.categories, item.price, item.likes, item.dislikes, item.wants, item.average_rating) }
    	.mapValues(itemScores => itemScores.map(_.score).reduce(_ - _)).toArray // array of ((name, item id, categories, price, likes, dislikes, wants, average_rating), score)
    	.sortBy(_._2)(Ordering.Double.reverse)
    	.take(query.num)
    	.map { case (k,v) => ItemScore(k._1, k._2, v, k._3, k._4, k._5, k._6, k._7, k._8) }

    new PredictedResult(combined)
  }
}

