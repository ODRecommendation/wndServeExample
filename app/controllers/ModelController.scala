package controllers

import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.optim.LocalPredictor
import com.intel.analytics.zoo.models.recommendation.ColumnFeatureInfo
import javax.inject._
import models.LoadModel
import play.api.libs.json._
import play.api.mvc._
import utilities.Helper

@Singleton
class ModelController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with Helper with LoadModel {

  def wndModel: Action[JsValue] = Action(parse.json) { request =>
    try {
      val requestJson = request.body.toString()
      val requestMap = mapper.readValue(requestJson, classOf[Map[String, String]])

      val (joined, atcMap) = assemblyFeature(requestMap, atcArray, localColumnInfo, 100)
      println(joined)

      val train = createUserItemFeatureMap(joined.toArray.asInstanceOf[Array[Map[String, Any]]], localColumnInfo, "wide_n_deep")
      val trainSample = train.map(x => x.sample)

      println("sample is created, ready to predict")

      val localPredictor = LocalPredictor(params.wndModel.get)
      val prediction = localPredictor.predict(trainSample)
        .zipWithIndex.map( p => {
        val id = p._2.toString
        val _output = p._1.toTensor[Float]
        val predict: Int = _output.max(1)._2.valueAt(1).toInt
        val probability = Math.exp(_output.valueAt(predict).toDouble)
        Map("predict" -> predict, "probability" -> probability, "id" -> id)
      })

      val prediction1 = prediction.map( x => {
        val result = x.map{ case (k ,v) => (k, (v, atcMap.getOrElse(v.toString, "")))}
        val predict = result("predict")._1
        val probability = result("probability")._1
        val atcSku = result("id")._2
        Map("predict" -> predict, "probability" -> probability, "atcSku" -> atcSku)
      })

      val predictionJson = mapper.writeValueAsString(prediction1)

      Ok(Json.parse(predictionJson.toString))
//      Ok(Json.obj("status" -> "ok"))
    }

    catch{
      case e:Exception => BadRequest("Nah nah nah nah nah...this request contains bad characters...")
    }
//    Ok(Json.obj("status" -> "ok"))
  }
}
