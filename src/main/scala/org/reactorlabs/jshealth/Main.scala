package org.reactorlabs.jshealth

import java.io.FileInputStream
import java.util.{Date, Properties}

import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * @author shabbirahussain
  */
object Main extends Serializable {
  val logger:Logger = Logger.getLogger("project.default.logger")

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ReactorLabs Git Miner")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val sqlContext: SQLContext = spark.sqlContext

  val prop: Properties = new Properties()
  try {
    val path = SparkFiles.get("config-defaults.properties")
    val stream = new FileInputStream(path)
    prop.load(stream)

    stream.close()
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

  val fileStorePath:String = prop.getProperty("raw.data.path")
  val analysisStoreLoc:String = prop.getProperty("sticky.cache.path")

  sc.setLocalProperty("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
  sc.setLocalProperty("spark.checkpoint.compress", "true")
  sc.setLocalProperty("spark.shuffle.spill.compress", "true")
  sc.setLocalProperty("spark.rdd.compress", "true")
  sc.setLogLevel("ERROR")


  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global
}
