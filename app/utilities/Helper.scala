package utilities

import com.fasterxml.jackson.databind.ObjectMapper
import com.intel.analytics.bigdl.dataset.{Sample, TensorSample}
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.T
import com.intel.analytics.zoo.models.recommendation.{ColumnFeatureInfo, UserItemFeature}
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.serialization.FrameReader
import models.LoadModel

import scala.collection.immutable.{List, Map}

trait Helper extends LoadModel{

  def categoricalFromVocabList(vocabList: Array[String]): String => Int = {
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
                       requestMap2: Map[String, Any],
                       dmaArray: Array[String],
                       modelArray: Array[String],
                       hrArray: Array[String],
                       columnInfo: ColumnFeatureInfo,
                       bucketSize: Int
                     ) = {

    val candidates = params.candidatesArray.get
    candidates.foreach(
      println(_)
    )
    val candidatesMap = candidates.zipWithIndex.map(x => (x._2.toString, x._1)).toMap

    val joined = candidates.map(
      x => {
        val dma = categoricalFromVocabList(dmaArray)(requestMap2("dma").toString)
        val model = categoricalFromVocabList(modelArray)(requestMap2("model").toString)
        val hr = categoricalFromVocabList(hrArray)(requestMap2("hr").toString)
        val weekend_ind = requestMap2("weekend_ind").toString.toInt
        val location_ind = requestMap2("location_ind").toString.toInt
        val push_ind = requestMap2("push_ind").toString.toInt
        val email_ind = requestMap2("email_ind").toString.toInt
        val itemId = leapTransform(x, "coupon", "itemId", params.itemIndexerModel.get, mapper)
        val hr_weekend = buckBucket(bucketSize)(requestMap2("hr").toString, requestMap2("weekend_ind").toString)
        val userId = leapTransform(requestMap2("aid").toString, "aid", "userId", params.userIndexerModel.get, mapper)
        val joined = Map(
          "userId" -> userId.toInt,
          "itemId" -> itemId.toInt,
          "label" -> 1,
          "hr-weekend" -> hr_weekend,
          "dma" -> dma,
          "model" -> model,
          "hr" -> hr,
          "weekend_ind" -> weekend_ind,
          "location_ind" -> location_ind,
          "push_ind" -> push_ind,
          "email_ind" -> email_ind
        )
        joined
      }
    )
    (joined, candidatesMap)
  }

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

  def getDeepTensor(joined: Map[String, Any], columnInfo: ColumnFeatureInfo): Tensor[Float] = {
    val deepColumns1 = columnInfo.indicatorCols
    val deepColumns2 = columnInfo.embedCols ++ columnInfo.continuousCols
    val deepLength = columnInfo.indicatorDims.sum + deepColumns2.length
    val deepTensor = Tensor[Float](deepLength).fill(0)

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

  def leapTransform(
                   requestString: String,
                   inputCol: String,
                   outputCol: String,
                   transformer: Transformer,
                   mapper: ObjectMapper
                   ) = {
    val schemaLeap = Map(
      "schema" -> Map(
        "fields" -> List(
          Map("type" -> "string", "name" -> inputCol)
        )
      ),
      "rows" -> List(List(requestString))
    )
    val requestLF = mapper.writeValueAsString(schemaLeap)
    val bytes = requestLF.getBytes("UTF-8")
    val predict = FrameReader("ml.combust.mleap.json").fromBytes(bytes).get
    val frame2 = transformer.transform(predict).get
    val result1 = for (lf <- frame2.select(outputCol)) yield lf.dataset.head(0)
    val result2 = result1.get.asInstanceOf[Double] + 1
    result2
  }

}
