package controllers

import java.nio.file.{Files, Paths}

import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.optim.LocalPredictor
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
      val sku = requestMap("COOKIE_ID")
      val atc = requestMap("SKU_NUM")
      val uid = leapTransform(sku, "COOKIE_ID", "userId", params.userIndexerModel.get, mapper)
      val iid = leapTransform(atc, "SKU_NUM", "itemId", params.itemIndexerModel.get, mapper)

      println(Files.exists(Paths.get("./modelFiles/userIndexer.zip")))

      val requestMap2 = requestMap + ("userId" -> uid.toInt, "itemId" -> iid.toInt)
      println(requestMap2)

      val (joined, atcMap) = assemblyFeature(requestMap2, params.atcArray.get, localColumnInfo, 100)

      val train = createUserItemFeatureMap(joined.toArray.asInstanceOf[Array[Map[String, Any]]], localColumnInfo, "wide_n_deep")
      val trainSample = train.map(x => x.sample)
      println("Sample is created, ready to predict")

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
    }

    catch{
      case _:Exception => BadRequest("Nah nah nah nah nah...this request contains bad characters...")
    }
  }
}
