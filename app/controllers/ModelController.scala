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

      val localColumnInfo = ColumnFeatureInfo(
        wideBaseCols = Array("loyalty_ind", "hvb_flg", "agent_smb_flg", "customer_type_nm", "sales_flg", "atcSKU", "GENDER_CD"),
        wideBaseDims = Array(2, 2, 2, 3, 2, 10, 3),
        wideCrossCols = Array("loyalty-ct"),
        wideCrossDims = Array(100),
        indicatorCols = Array("customer_type_nm", "GENDER_CD"),
        indicatorDims = Array(3, 3),
        embedCols = Array("userId", "itemId"),
        embedInDims = Array(10, 10),
        embedOutDims = Array(20, 11),
        continuousCols = Array("interval_avg_day_cnt", "STAR_RATING_AVG", "reviews_cnt")
      )
      println("localColumnInfo is constructed")

      val joined = assemblyFeature(requestMap, atcArray, localColumnInfo, 100).toArray
      println(joined)

      val train = createUserItemFeatureMap(joined.asInstanceOf[Array[Map[String, Any]]], localColumnInfo, "wide_n_deep")
      val trainSample = train.map(x => x.sample)

      println("sample is created, ready to predict")

      val localPredictor = LocalPredictor(params.wndModel.get)
      val prediction = localPredictor.predict(trainSample).map( p => {
        val _output = p.toTensor[Float]
        val predict: Int = _output.max(1)._2.valueAt(1).toInt
        val probability = Math.exp(_output.valueAt(predict).toDouble)
        Map("predict" -> predict, "probability" -> probability)
      })


      val predictionJson = mapper.writeValueAsString(prediction)

      Ok(Json.parse(predictionJson.toString))
//      Ok(Json.obj("status" -> "ok"))
    }

    catch{
      case e:Exception => BadRequest("Nah nah nah nah nah...this request contains bad characters...")
    }
//    Ok(Json.obj("status" -> "ok"))
  }
}
