package controllers

import com.intel.analytics.bigdl.numeric.NumericFloat
import javax.inject._
import models.LoadModel
import play.api.libs.json._
import play.api.mvc._
import utilities.Helper

/**
  * This controller creates an `Action` to handle recommendation prediction.
  */

@Singleton
class ModelController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Helper with LoadModel {

  /**
    * Create an Action to return @ModelController status.
    * The configuration in the `routes` file means that this method
    * will be called when the application receives a `POST` request with
    * a path of `/wndModel`.
    */

  def wndModel: Action[JsValue] = Action(parse.json) { request =>
    try {
      val requestJson = request.body.toString()
      val requestMap = mapper.readValue(requestJson, classOf[Map[String, String]])
      println(requestMap)

      val (joined, atcMap) = assemblyFeature(
        requestMap,
        params.dmaArray.get,
        params.modelArray.get,
        params.hrArray.get,
        localColumnInfo,
        100
      )

      val train = createUserItemFeatureMap(joined.asInstanceOf[Array[Map[String, Any]]], localColumnInfo, "wide_n_deep")
      val trainSample = train.map(x => x.sample)
      println("Sample is created, ready to predict")

      val prediction = params.wndModel.get.predict(trainSample)
        .zipWithIndex.map( p => {
        val id = p._2.toString
        val _output = p._1.toTensor[Float]
        val predict: Int = _output.max(1)._2.valueAt(1).toInt
        val probability = Math.exp(_output.valueAt(predict).toDouble)
        Map("predict" -> predict, "probability" -> probability, "id" -> id)
      })
      println("predicting")

      val prediction1 = prediction.map( x => {
        val result = x.map{ case (k ,v) => (k, (v, atcMap.getOrElse(v.toString, "")))}
        val predict = result("predict")._1
        val probability = result("probability")._1
        val atcSku = result("id")._2
        Map("predict" -> predict, "probability" -> probability, "couponId" -> atcSku)
      }).filter(x => x("predict").toString == "2").sortWith(_.getOrElse("probability", 0.0).asInstanceOf[Double] > _.getOrElse("probability", 0.0).asInstanceOf[Double])

      val predictionJson = mapper.writeValueAsString(prediction1)

      Ok(Json.parse(predictionJson.toString))
    }

    catch{
      case e:Exception => BadRequest(e.printStackTrace().toString)
    }
  }
}
