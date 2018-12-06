package utilities

import java.text.SimpleDateFormat

import com.intel.analytics.bigdl.dataset.{Sample, TensorSample}
import com.intel.analytics.bigdl.optim.LocalPredictor
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.T
import com.intel.analytics.zoo.models.recommendation.Utils.{getDeepTensor, getWideTensor}
import com.intel.analytics.zoo.models.recommendation.{ColumnFeatureInfo, Recommender, UserItemFeature, UserItemPrediction}
//import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.intel.analytics.bigdl.numeric.NumericFloat

//import scala.collection.mutable.Map
import scala.io.Source

trait Helper {

//  def recommendForUserlocal(
//                             model: Recommender[Float],
//                             featureArray: Array[UserItemFeature[Float]],
//                             maxItems: Int,
//                             batchPerCore: Int = 4
//                           ): Array[UserItemPrediction] = {
//    val pairPredictions = predictUserItemPairLocal(model, featureArray, batchPerCore)
//    pairPredictions
//      .groupBy(_.userId)
//      .flatMap(x => {
//        val ordered = x._2.toArray.sortBy(y => (-y.prediction, -y.probability)).take(maxItems)
//        ordered
//      }).toArray
//  }

//  def predictUserItemPairLocal(model: Recommender[Float], featureArray: Array[UserItemFeature[Float]], batchPerCore: Int = 4): Array[UserItemPrediction] = {
//    val inputCount = featureArray.length
//    val idPairs = featureArray.map(pair => (pair.userId, pair.itemId))
//    val features = featureArray.map(pair => pair.sample)
//    val localPredictor = LocalPredictor(model, batchPerCore = batchPerCore)
//    val raw = localPredictor.predict(features)
//    val predictProb = raw.map { x =>
//      val _output = x.toTensor[Float]
//      val predict: Int = _output.max(1)._2.valueAt(1).toInt
//      val probability = Math.exp(_output.valueAt(predict).asInstanceOf[Float])
//      (predict, probability)
//    }
//    val outArray: Array[UserItemPrediction] = idPairs.zip(predictProb)
//      .map(x => UserItemPrediction(x._1._1, x._1._2, x._2._1, x._2._2))
//
//    require(inputCount == outArray.length, s"count of features must equal to count of prediction")
//    outArray
//  }

//  def getWideTensorArray(
//                     input: Map[String, Any],
//                     wideBaseCols: Array[String],
//                     wideBaseDims: Array[Int],
//                     wideCrossCols: Array[String],
//                     wideCrossDims: Array[Int]
//                   ): Tensor[Float] = {
//    val wideColumns = wideBaseCols ++ wideCrossCols
//    val wideDims = wideBaseDims ++ wideCrossDims
//    val wideLength = wideColumns.length
//    var acc = 0
//    val indices: Array[Int] = (0 until wideLength).map(i => {
//      val index = input(wideColumns(i)).toString.toInt
//      if (i == 0) index
//      else {
//        acc = acc + wideDims(i - 1)
//        acc + index
//      }
//    }).toArray
//    val values = indices.map(_ + 1.0f)
//    val shape = Array(wideDims.sum)
//
//    Tensor.sparse(Array(indices), values, shape)
//  }

//  def getDeepTensorArray(
//                input: Map[String, Any],
//                indicatorCols: Array[String],
//                indicatorDims: Array[Int],
//                embedCols: Array[String],
//                continuousCols: Array[String]
//              ): Tensor[Float] = {
//    val deepColumns1 = indicatorCols
//    val deepColumns2 = embedCols ++ continuousCols
//    val deepLength = indicatorDims.sum + deepColumns2.length
//    val deepTensor = Tensor[Float](deepLength).fill(0)
//
//    var acc = 0
//    deepColumns1.indices.map {
//      i =>
//        val index = input(indicatorCols(i)).toString.toInt
//        val accIndex = if (i == 0) index
//        else {
//          acc = acc + indicatorDims(i - 1)
//          acc + index
//        }
//        deepTensor.setValue(accIndex + 1, 1)
//    }
//
//    deepColumns2.indices.map {
//      i =>
//        deepTensor.setValue(i + 1 + indicatorDims.sum,
//          input(deepColumns2(i)).toString.toFloat).toString.toDouble
//    }
//    deepTensor
//  }

//  def map2Sample(
//                  input: Map[String, Any],
//                  wideBaseCols: Array[String],
//                  wideBaseDims: Array[Int],
//                  wideCrossCols: Array[String],
//                  wideCrossDims: Array[Int],
//                  indicatorCols: Array[String],
//                  indicatorDims: Array[Int],
//                  embedCols: Array[String],
//                  continuousCols: Array[String]
//                ): Sample[Float] = {
//
//    val wideTensor: Tensor[Float] = getWideTensorArray(
//      input,
//      wideBaseCols,
//      wideBaseDims,
//      wideCrossCols,
//      wideCrossDims
//    )
//    val deepTensor: Tensor[Float] = getDeepTensorArray(
//      input,
//      indicatorCols,
//      indicatorDims,
//      embedCols,
//      continuousCols
//    )
//    val l = input("label").toString.toInt
//
//    val label = Tensor[Float](T(l))
//    label.resize(1, 1)
//
//    TensorSample[Float](Array(wideTensor, deepTensor))
//  }

//  def toSample(
//                dataFrame: DataFrame,
//                indicatorCols: Array[String],
//                indicatorDims: Array[Int],
//                embedCols: Array[String],
//                continuousCols: Array[String],
//                labelCol: String
//              ) = {
//    val features = dataFrame.rdd.map( r => {
//      //        val SKU_NUM = r.getAs[Double]("item").toFloat
//      ////        val STAR_RATING_AVG = x.getAs[Double]("STAR_RATING_AVG").toFloat
//      //        val img_flg = r.getAs[Int]("img_flg")
//      //        val sales = r.getAs[Int]("sales")
//      val sub_flg = r.getAs[Int](labelCol)
//      val label = Tensor[Float](T(sub_flg))
//      //        Sample(Tensor(Array(img_flg, sales, SKU_NUM), Array(3)), Tensor[Float](1,1).fill(sub_flg))
//      TensorSample(
//        Array(toTensor(
//          r,
//          indicatorCols,
//          indicatorDims,
//          embedCols,
//          continuousCols
//        )),
//        Array(label)
//      )
//    })
//
//    features
//  }

//  def loadPublicDataInference(sqlContext: SQLContext, dataPath: String):
//  (DataFrame, DataFrame, DataFrame) = {
//
//    val itemIndex = sqlContext.read.option("header", true).option("inferSchema", true).csv(dataPath + "/itemIndex.csv")
//    val userIndex = sqlContext.read.option("header", true).option("inferSchema", true).csv(dataPath + "/userIndex.csv")
//    val userDFI = sqlContext.read.option("header", true).option("inferSchema", true).csv(dataPath + "/newUserDF.csv")
//    val itemDFI = sqlContext.read.option("header", true).option("inferSchema", true).csv(dataPath + "/newItemDF.csv")
//    val ratingsDFI = userDFI.crossJoin(itemDFI).select(userDFI("userId"), itemDFI("itemId")).withColumn("label", lit(1))
//
//    (ratingsDFI, userDFI, itemDFI)
//  }

//  def replaceEmpty(input: Map[String, Any])={
//    input.updated(
//      "h",
//      if (input("h") == "") 0
//      else input("h")
//    ).updated(
//      "smb",
//      if (input("smb") == "") 0
//      else input("smb")
//    ).updated(
//      "sub_sub_category",
//      if (input("sub_sub_category") == "") "None"
//      else input("sub_sub_category")
//    )
//  }
//
//  def reviewTransformer(input: Map[String, Any])={
////    input.updated("reviews_cnt", Math.log(input("review_cnt").toString.toInt + 1).toInt)
//    input.updated("reviews_cnt", input("review_cnt"))
//  }
//
//
//  def ratingTransformer(input: Map[String, Any])={
////    val star_rating_avg = input("star_rating_avg").toString.toDouble
////    input.updated(
////      "star_rating_avg",
////      if (star_rating_avg >= 1 && star_rating_avg < 4) 1
////      else if (star_rating_avg >= 4 && star_rating_avg < 5) 2
////      else if (star_rating_avg == 5) 3
////      else 0
////    )
//    val star_rating_avg = input("star_rating_avg").toString.toDouble.toInt
//    input.updated("star_rating_avg", star_rating_avg)
//  }

//  def systemTransformer(input: Map[String, Any])={
//    val system = input("system").toString
//    input += (
//      "system_windows_new" ->
//        (
//          if (system contains("Windows NT 10")) 1
//          else 0
//          )
//      ) += (
//      "system_windows_old" ->
//        (
//          if ((system contains("Windows NT")) && (!(system contains("Windows NT 10")))) 1
//          else 0
//          )
//      ) += (
//      "system_mac_new" ->
//        (
//          if (system contains("Mac OS X 10_13")) 1
//          else 0
//          )
//      ) += (
//      "system_mac_old" ->
//        (
//          if ((system contains("Mac OS X")) && (!(system contains("Mac OS X 10_13")))) 1
//          else 0
//          )
//      ) += (
//      "system_ios_new" ->
//        (
//          if (system contains("iPhone OS 11")) 1
//          else 0
//          )
//      ) += (
//      "system_ios_old" ->
//        (
//          if ((system contains("iPhone OS")) && (!(system contains("iPhone OS 11")))) 1
//          else 0
//          )
//      ) += (
//      "system_android_new" ->
//        (
//          if (system contains("Android 8")) 1
//          else 0
//          )
//      ) += (
//      "system_android_old" ->
//        (
//          if ((system contains("Android")) && (!(system contains("Android 8")))) 1
//          else 0
//          )
//      )
//  }
//
//  def mcTransformer(input: Map[String, Any])={
//    val mc_channel = Source.fromFile("modelfiles/mc_channel.csv").getLines().drop(1).map(_.split('|')).map(x => (x(1), x(0)))
//    val marketing_channel_ss_nm = mc_channel.find(_._1 == input("mc").toString()).map(_._2).getOrElse("Direct Load")
//    input += ("marketing_channel_ss_nm" -> marketing_channel_ss_nm)
//  }

//  def renameAndAddQuote(input: Map[String, Any])={
//    val customer_type = input("customer_type")
//    val sub_sub_category = input("sub_sub_category")
//    input.updated(
//      "h", input("h").toString.toInt
//    ).updated(
//      "smb", input("smb").toString.toInt
//    )
//  }

//  def customerTransformer(input: Map[String, Any])={
//    val customerType = input("customer_type").toString
//    input += (
//      "consumer_flg" ->
//        (if (customerType.toLowerCase == "consumer") 1
//        else 0)
//      ) += (
//      "business_flg" ->
//        (if (customerType.toLowerCase == "business") 1
//        else 0)
//      ) += (
//      "ho_flg" ->
//        (if (customerType.toLowerCase == "home office") 1
//        else 0)
//      )
//  }

//  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }
////    += ("customer_type" -> s"""\"$customer_type\"""") += ("sub_sub_category" -> s"""\"$sub_sub_category\"""")
//
////  def formatDate(epoch: Long, df: SimpleDateFormat) = {
////    try Some(df.format(epoch))
////    catch {
////      case _: Exception => println("No date can be found"); None;
////    }
////  }
//

  def categoricalFromVocabList(vocabList: Array[String]): (String) => Int = {
    val func = (sth: String) => {
      val default: Int = 0
      val start: Int = 1
      if (vocabList.contains(sth)) vocabList.indexOf(sth) + start
      else default
    }
    func
  }

  def buckBucket(bucketSize: Int): (String, String) => Int = {
    val func = (col1: String, col2: String) =>
      Math.abs((col1 + "_" + col2).hashCode()) % bucketSize + 0
    func
  }

  def assemblyFeature(
                       requestMap: Map[String, Any],
                       ATCSKUVocabs: Array[String],
                       columnInfo: ColumnFeatureInfo,
                       bucketSize: Int
                     ) = {
    val atcArray = requestMap("ATC_SKU").asInstanceOf[Seq[String]]
    val atcMap = atcArray.zipWithIndex.map(x => (x._2.toString, x._1)).toMap
    println(atcMap)

    val joined = atcArray.map(
      x => {
        val ATC_SKU = categoricalFromVocabList(ATCSKUVocabs)(x)
        val customer_type_nm = categoricalFromVocabList(Array("CONSUMER", "BUSINESS", "HOME OFFICE"))(requestMap("customer_type_nm").toString)
        val GENDER_CD = categoricalFromVocabList(Array("M", "F", "U"))(requestMap("GENDER_CD").toString)
        val loyalty_ct = buckBucket(bucketSize)(requestMap("loyalty_ind").toString, requestMap("customer_type_nm").toString)
        val loyalty_ind = requestMap("loyalty_ind").toString.toInt
        val od_card_user_ind = requestMap("od_card_user_ind").toString.toInt
        val hvb_flg = requestMap("hvb_flg").toString.toInt
        val agent_smb_flg = requestMap("agent_smb_flg").toString.toInt
        val interval_avg_day_cnt = requestMap("interval_avg_day_cnt").toString.toDouble
        val SKU_NUM = requestMap("itemId").toString.toInt
        val STAR_RATING_AVG = requestMap("STAR_RATING_AVG").toString.toDouble
        val reviews_cnt = requestMap("reviews_cnt").toString.toInt
        val sales_flg = requestMap("sales_flg").toString.toInt
        val userId = requestMap("userId").toString.toInt

        val joined = Map(
          "atcSKU" -> ATC_SKU,
          "customer_type_nm" -> customer_type_nm,
          "GENDER_CD" -> GENDER_CD,
          "loyalty-ct" -> loyalty_ct,
          "loyalty_ind" -> loyalty_ind,
          "od_card_user_ind" -> od_card_user_ind,
          "hvb_flg" -> hvb_flg,
          "agent_smb_flg" -> agent_smb_flg,
          "interval_avg_day_cnt" -> interval_avg_day_cnt,
          "itemId" -> SKU_NUM,
          "STAR_RATING_AVG" -> STAR_RATING_AVG,
          "reviews_cnt" -> reviews_cnt,
          "sales_flg" -> sales_flg,
          "userId" -> userId
        )

        joined
      }
    )
    (joined, atcMap)
  }

  // setup wide tensor
  def getWideTensor(joined: Map[String, Any], columnInfo: ColumnFeatureInfo): Tensor[Float] = {
    val wideColumns = columnInfo.wideBaseCols ++ columnInfo.wideCrossCols
    val wideDims = columnInfo.wideBaseDims ++ columnInfo.wideCrossDims
    val wideLength = wideColumns.length
    var acc = 0
    val indices: Array[Int] = (0 until wideLength).map(i => {
      val index = joined(wideColumns(i)).toString.toInt
      if (i == 0) index
      else {
        acc = acc + wideDims(i - 1)
        acc + index
      }
    }).toArray
    val values = indices.map(_ + 1.0f)
    val shape = Array(wideDims.sum)

    Tensor.sparse(Array(indices), values, shape)
  }

  // setup deep tensor
  def getDeepTensor(joined: Map[String, Any], columnInfo: ColumnFeatureInfo): Tensor[Float] = {
    val deepColumns1 = columnInfo.indicatorCols
    val deepColumns2 = columnInfo.embedCols ++ columnInfo.continuousCols
    val deepLength = columnInfo.indicatorDims.sum + deepColumns2.length
    val deepTensor = Tensor[Float](deepLength).fill(0)

    // setup indicators
    var acc = 0
    deepColumns1.indices.map {
      i =>
        val index = joined(columnInfo.indicatorCols(i)).toString.toInt
        val accIndex = if (i == 0) index
        else {
          acc = acc + columnInfo.indicatorDims(i - 1)
          acc + index
        }
        deepTensor.setValue(accIndex + 1, 1)
    }

    // setup embedding and continuous
    deepColumns2.indices.map {
      i =>
        deepTensor.setValue(i + 1 + columnInfo.indicatorDims.sum,
          joined(deepColumns2(i)).toString.toFloat)
    }
    deepTensor
  }

  def map2Sample(joined: Map[String, Any], columnInfo: ColumnFeatureInfo, modelType: String): Sample[Float] = {

    val wideTensor: Tensor[Float] = getWideTensor(joined, columnInfo)
    val deepTensor: Tensor[Float] = getDeepTensor(joined, columnInfo)
    val l = 1.0

    val label = Tensor[Float](T(l))
    label.resize(1, 1)

    modelType match {
      case "wide_n_deep" =>
        TensorSample[Float](Array(wideTensor, deepTensor), Array(label))
      case "wide" =>
        TensorSample[Float](Array(wideTensor), Array(label))
      case "deep" =>
        TensorSample[Float](Array(deepTensor), Array(label))
      case _ =>
        throw new IllegalArgumentException("unknown type")
    }
  }

  def createUserItemFeatureMap(joined: Array[Map[String, Any]], columnInfo: ColumnFeatureInfo, modelType: String) = {
    val sample = joined.map(x => {
      val uid = x("userId").toString.toInt
      val iid = x("itemId").toString.toInt
      UserItemFeature(uid, iid, map2Sample(x, columnInfo, modelType))
    })
    sample
  }

}
