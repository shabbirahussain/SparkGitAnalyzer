package org.reactorlabs.jshealth

import java.util.{Date, Properties}

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
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
  sc.setLogLevel("WARN")
  val sqlContext: SQLContext = spark.sqlContext

  sc.setLocalProperty("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
  sc.setLocalProperty("spark.checkpoint.compress", "true")
  sc.setLocalProperty("spark.shuffle.spill.compress", "true")
  sc.setLocalProperty("spark.rdd.compress", "true")

  val fileStorePath = "/home/hshabbir/Data/mysql-2018-04-01"
  val analysisStoreLoc = "/tmp/hadoop-hshabbir/AnalysisStorage"

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global
}
