package models

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.intel.analytics.zoo.models.recommendation.ColumnFeatureInfo

trait LoadModel {

  var params = ModelParams(
    "/Users/rbilw002/Documents/GitHub/wndServeExample/target/universal/stage/modelFiles/offerModelOld",
    "/modelFiles/userIndexer.zip",
    "/modelFiles/itemIndexer.zip",
    "/modelFiles/couponAll",
    "/modelFiles/dmaArray",
    "/modelFiles/modelArray",
    "/modelFiles/hrArray",
    scala.util.Properties.envOrElse("configEnvironmewnt", "dev")
  )

  val dmaDim = params.dmaArray.get.length
  val modelDim = params.modelArray.get.length

  val localColumnInfo = ColumnFeatureInfo(
    wideBaseCols = Array("weekend_ind", "location_ind", "push_ind", "email_ind", "dma", "model"),
    wideBaseDims = Array(2, 2, 2, 2, dmaDim, modelDim),
    wideCrossCols = Array("hr-weekend"),
    wideCrossDims = Array(100),
    indicatorCols = Array("dma", "model", "hr"),
    indicatorDims = Array(dmaDim, modelDim, 24),
    embedCols = Array("userId", "itemId"),
    embedInDims = Array(392417, 47),
    embedOutDims = Array(50, 10))
  println("localColumnInfo is constructed")

//  val actorSystem = ActorSystem()
//  val scheduler: Scheduler = actorSystem.scheduler
//  private val task = new Runnable {
//    def run(): Unit = {
//      try {
////        ModelParams.downloadModel(params)
//        ModelParams.refresh(params)
//      }
//      catch {
//        case _: Exception => println("Model update has failed")
//      }
//    }
//  }
//
//  implicit val executor = actorSystem.dispatcher

//  scheduler.schedule(
//    initialDelay = Duration(5, TimeUnit.SECONDS),
//    interval = Duration(5, TimeUnit.SECONDS),
//    runnable = task)

  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

}
