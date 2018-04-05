package org.reactorlabs.jshealth

import java.util.{Date, Properties}
import java.sql.DriverManager
import java.sql.Connection

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable
import scala.io.Source

/**
  * @author shabbirahussain
  */
object Main extends Serializable {
  val logger:Logger = Logger.getLogger("project.default.logger")

  val prop: Properties = new Properties()
  try {
    val loader = this.getClass.getClassLoader
    val stream = loader.getResourceAsStream("config-defaults.properties")
    prop.load(stream)
    stream.close()
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

  val spark: SparkSession = SparkSession
    .builder()
    .master(prop.getProperty("spark.master"))
    .appName("ReactorLabs Git Miner")
    .config("spark.cores.max",       prop.getProperty("spark.cores.max"))
    .config("spark.executor.cores",  prop.getProperty("spark.executor.cores"))
    .config("spark.workers.cores",   prop.getProperty("spark.executor.cores"))
    .config("spark.executor.memory", prop.getProperty("spark.executor.memory"))
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
  sc.setCheckpointDir("target/temp/spark/")

  val sqlContext: SQLContext = spark.sqlContext


  def main(args: Array[String])
  : Unit = {
    println("Main")
    var start = 0l
    println("started at:" + new Date())
    start = System.currentTimeMillis()


    println("\nended at:" + new Date() + "\ttook:"+ (System.currentTimeMillis() - start))
  }
}
