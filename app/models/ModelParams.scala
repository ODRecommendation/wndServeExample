package models

import java.io.File
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.intel.analytics.zoo.models.common.ZooModel
import com.intel.analytics.zoo.models.recommendation.Recommender
import com.intel.analytics.bigdl.numeric.NumericFloat

import scala.io.Source


case class ModelParams(
                   wndModelPath: String,
                   atcArrayPath: String,
                   env: String,
                   bucketName: String,
                   var wndModel: Option[Recommender[Float]],
                   var wndModelVersion: Option[Long],
                   var atcArray: Option[Array[String]],
                   var atcArrayVersion: Option[Long]
                 )

object ModelParams {

  private val s3client = AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain()).build()
  private val lock: ReadWriteLock = new ReentrantReadWriteLock()

  System.setProperty("bigdl.localMode", "true")
  println("BigDL localMode is: " + System.getProperty("bigdl.localMode"))

  def apply(
             wndModelPath: String,
             atcArrayPath: String,
             env: String
           ): ModelParams = ModelParams(
    wndModelPath,
    atcArrayPath,
    env,
    if (env == "prod" || env == "canary") "ecomdatascience-p" else "ecomdatascience-np",
    loadModelFile(wndModelPath),
    loadVersion(wndModelPath),
    loadArrayFile(atcArrayPath),
    loadVersion(atcArrayPath)
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

  def loadModelFile(path: String) = {
    lock.readLock().lock()
    if (new File(path).exists()) {
      try {
        Some(ZooModel.loadModel[Float](path).asInstanceOf[Recommender[Float]])
      }
          catch {
            case e: Exception => println(s"Cannot load model at $path"); None
          }
      finally { lock.readLock().unlock() }
    }
    else {
      println(s"Cannot load model at $path")
      None
    }
  }

  def loadVersion(path: String): Option[Long] = {
    lock.readLock().lock()
    try Some(new File(path).lastModified())
    catch {
      case e: Exception => println(s"Cannot load model version at $path"); None
    }
    finally { lock.readLock().unlock() }
  }

  def loadArrayFile(path: String) = {
    lock.readLock().lock()
    try Some(Source.fromFile(path).getLines().drop(1)
      .flatMap(_.split(",")).toArray)
    catch {
      case e: Exception => println(s"Cannot load array at $path"); None
    }
    finally { lock.readLock().unlock() }
  }

  def refresh(params: ModelParams): ModelParams = {
    params.wndModel = loadModelFile(params.wndModelPath)
    params.wndModelVersion = loadVersion(params.wndModelPath)
    params.atcArray = loadArrayFile(params.atcArrayPath)
    params.atcArrayVersion = loadVersion(params.atcArrayPath)
    params
  }

//  def loadModelFile(path: String, currentDir: String): Option[Transformer] = {
//    lock.readLock().lock()
//    try {
//      Some(
//        (
//          for (bundleFile <- managed(BundleFile(s"jar:file:$currentDir$path"))) yield {
//            bundleFile.loadMleapBundle().get
//          }
//          ).opt.get.root
//      )
//    }
//    catch {
//      case e: Exception => println(s"Cannot load model at $currentDir$path"); None
//    }
//    finally { lock.readLock().unlock() }
//  } b b

}