package utilities

import com.intel.analytics.bigdl.dataset.{Sample, TensorSample}
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.T
import com.intel.analytics.zoo.models.recommendation.{ColumnFeatureInfo, UserItemFeature}

trait Helper {

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

}
