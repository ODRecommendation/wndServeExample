package models

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Scheduler}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.zoo.models.recommendation.ColumnFeatureInfo

import scala.concurrent.duration.Duration

trait LoadModel {

  System.setProperty("bigdl.localMode", "true")
  println("BigDL localMode is: " + System.getProperty("bigdl.localMode"))
  Engine.init

  var params = ModelParams(
    "./modelFiles/WDModel",
    "./modelFiles/userIndexer.zip",
    "./modelFiles/itemIndexer.zip",
    "./modelFiles/ATCSKU.csv",
    scala.util.Properties.envOrElse("configEnvironmewnt", "dev")
  )

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

  val actorSystem = ActorSystem()
  val scheduler: Scheduler = actorSystem.scheduler
  private val task = new Runnable {
    def run(): Unit = {
      try {
//        ModelParams.downloadModel(params)
        ModelParams.refresh(params)
      }
      catch {
        case _: Exception => println("Model update has failed")
      }
    }
  }

  implicit val executor = actorSystem.dispatcher

  scheduler.schedule(
    initialDelay = Duration(5, TimeUnit.SECONDS),
    interval = Duration(5, TimeUnit.SECONDS),
    runnable = task)

  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

}
