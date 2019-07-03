package models

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.optim.LocalPredictor
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.zoo.models.recommendation.WideAndDeep
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.Transformer
import resource.managed

import scala.io.Source


case class ModelParams(
                   wndModelPath: String,
                   userIndexerModelPath: String,
                   itemIndexerModelPath: String,
                   candidatesArrayPath: String,
                   dmaArrayPath: String,
                   modelArrayPath: String,
                   hrArrayPath: String,
                   env: String,
                   bucketName: String,
                   var wndModel: Option[LocalPredictor[Float]],
                   var wndModelVersion: Option[Long],
                   var userIndexerModel: Option[Transformer],
                   var userIndexerModelVersion: Option[Long],
                   var itemIndexerModel: Option[Transformer],
                   var itemIndexerModelVersion: Option[Long],
                   var candidatesArray: Option[Array[String]],
                   var candidatesArrayVersion: Option[Long],
                   var dmaArray: Option[Array[String]],
                   var dmaArrayVersion: Option[Long],
                   var modelArray: Option[Array[String]],
                   var modelArrayVersion: Option[Long],
                   var hrArray: Option[Array[String]],
                   var hrArrayVersion: Option[Long]
                 )

object ModelParams {

//  private val s3client = AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain()).build()
  private val lock: ReadWriteLock = new ReentrantReadWriteLock()
  private val currentDir = Paths.get(".").toAbsolutePath.toString

  System.setProperty("bigdl.localMode", "true")
  println("BigDL localMode is: " + System.getProperty("bigdl.localMode"))
  Engine.init

  def apply(
             wndModelPath: String,
             userIndexerModelPath: String,
             itemIndexerModelPath: String,
             candidatesArrayPath: String,
             dmaArrayPath: String,
             modelArrayPath: String,
             hrArrayPath: String,
             env: String
           ): ModelParams = ModelParams(
    wndModelPath,
    userIndexerModelPath,
    itemIndexerModelPath,
    candidatesArrayPath,
    dmaArrayPath: String,
    modelArrayPath: String,
    hrArrayPath: String,
    env,
    if (env == "prod" || env == "canary") "ecomdatascience-p" else "ecomdatascience-np",
    loadRecommender(wndModelPath),
    loadVersion(wndModelPath),
    loadMleap(userIndexerModelPath),
    loadVersion(userIndexerModelPath),
    loadMleap(itemIndexerModelPath),
    loadVersion(itemIndexerModelPath),
    loadArrayFile(candidatesArrayPath),
    loadVersion(candidatesArrayPath),
    loadArrayFile(dmaArrayPath),
    loadVersion(dmaArrayPath),
    loadArrayFile(modelArrayPath),
    loadVersion(modelArrayPath),
    loadArrayFile(hrArrayPath),
    loadVersion(hrArrayPath)
  )

//  def downloadModel(params: ModelParams): Any = {
//    lock.writeLock().lock()
//    try {
//      val file = s3client.getObject(new GetObjectRequest(params.bucketName, "lu/subLatest.zip")).getObjectContent
//      println("Downloading the file")
//      val outputStream = new FileOutputStream(s"${params.currentDir}${params.subModelPath}")
//      IOUtils.copy(file, outputStream)
//      println("Download has completed")
//    }
//    catch {
//      case e: Exception => println(s"Cannot download model at ${params.env}-${params.bucketName}-${params.subModelPath}"); None
//    }
//    finally { lock.writeLock().unlock() }
//  }

  def loadRecommender(path: String) = {
    lock.readLock().lock()
    try {
      Some(LocalPredictor(WideAndDeep.loadModel[Float](path)))
    }
    catch {
      case e: Exception => println(s"Cannot load bigDL model at $currentDir$path"); None
    }
    finally { lock.readLock().unlock() }
  }

  def loadMleap(modelPath: String): Option[Transformer] = {
    if (new File(currentDir + "/" + modelPath).exists()) {
      lock.readLock().lock()
      try Some(
        (for (bundleFile <- managed(BundleFile(s"jar:file:$currentDir/$modelPath"))) yield {
          bundleFile.loadMleapBundle().get
        }).opt.get.root
      )
      catch {
        case _: Exception => println(s"Cannot load Mleap model at $currentDir$modelPath"); None;
      }
      finally {
        lock.readLock().unlock()
      }
    }
    else {
      println(s"Cannot update Mleap model at $currentDir$modelPath")
      None
    }
  }

  def loadVersion(path: String): Option[Long] = {
    lock.readLock().lock()
    try Some(new File(currentDir + "/" + path).lastModified())
    catch {
      case _: Exception => println(s"Cannot load model version at $currentDir$path"); None
    }
    finally { lock.readLock().unlock() }
  }

  def loadArrayFile(path: String): Option[Array[String]] = {
    lock.readLock().lock()
    try Some(Source.fromFile(currentDir + "/" + path).getLines().map(_.toString.trim).toArray.distinct)
    catch {
      case _: Exception => println(s"Cannot load array at $currentDir$path"); None
    }
    finally { lock.readLock().unlock() }
  }

//  def refresh(params: ModelParams): ModelParams = {
//    params.wndModel = loadBigDL(params.wndModelPath)
//    params.wndModelVersion = loadVersion(params.wndModelPath)
//    params.userIndexerModel = loadMleap(params.userIndexerModelPath)
//    params.userIndexerModelVersion = loadVersion(params.userIndexerModelPath)
//    params.itemIndexerModel = loadMleap(params.itemIndexerModelPath)
//    params.itemIndexerModelVersion = loadVersion(params.itemIndexerModelPath)
//    params.atcArray = loadArrayFile(params.atcArrayPath)
//    params.atcArrayVersion = loadVersion(params.atcArrayPath)
//    params
//  }

}