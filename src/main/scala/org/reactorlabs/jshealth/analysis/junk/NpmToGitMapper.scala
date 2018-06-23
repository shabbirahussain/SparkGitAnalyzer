package org.reactorlabs.jshealth.analysis.junk

import java.io.FileNotFoundException
import java.net.{ConnectException, MalformedURLException, URL}
import java.util.regex.Pattern

import javax.net.ssl.HttpsURLConnection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.reactorlabs.jshealth.Main.{sc, spark}

import scala.io.Source

/**
  * @author shabbir.ahussain
  */
class NpmToGitMapper extends Serializable {
  val sqlContext: SQLContext = spark.sqlContext
  import sqlContext.implicits._

  val MAX_RETIRES = 3
  val regEx = Pattern.compile("""git:\/\/github\.com\/([^\/]+)\/(.+?)(\.git|")""")

  var retryAttempts = 0

  /** Downloads info from npm website.
    *
    * @param packageName is the package name to get info
    * @return string of response.
    */
  private def getGitInfoFromNPMRegistry(packageName: String): Option[String] = {
    val npmUrl = "https://registry.npmjs.org/"
    try {
      val url = new URL(npmUrl + packageName)
      val con = url.openConnection.asInstanceOf[HttpsURLConnection]
      con.setConnectTimeout(2000)

      val input = Source.fromInputStream(con.getInputStream).mkString
      retryAttempts = 0
      return Some(input)
    } catch {
      case e: MalformedURLException => e.printStackTrace()
      case e: FileNotFoundException => {
        if (retryAttempts < MAX_RETIRES) {
          retryAttempts += 1
          return getGitInfoFromNPMRegistry(packageName.split("\\.")(0))
        }
      }
      case e: ConnectException => {
        if (retryAttempts < MAX_RETIRES) {
          retryAttempts += 1
          return getGitInfoFromNPMRegistry(packageName)
        }
      }
      case e: Exception =>
    }
    None
  }

  /**
    * Extract info from the HTTPResponse as text.
    */
  private def extractInfoFromResponse(response: String): Option[(String, String)]= {
    try{
      val m = regEx.matcher(response)
      m.find()
      return Some((m.group(1), m.group(2)))
    } catch {case _:Exception=>}
    None
  }

  /** Extracts repos to be processed from original set.
    *
    * @param orig is the dataframe of the repos deemed original.
    * @return A dataframe of node_module names.
    */
  private def getEssentialNodeRepos(orig: DataFrame): DataFrame = {
    var temp = orig.select($"O_GIT_PATH")
    var res = temp.filter($"O_GIT_PATH".isNull).select($"O_GIT_PATH".as("REPO"))
    var cntNode: Long = 0
    do {
      temp = temp.filter($"O_GIT_PATH".contains("node_modules/")).
        withColumn("O_GIT_PATH", $"O_GIT_PATH".substr(instr($"O_GIT_PATH", "node_modules/") + 13, lit(10000))).
        distinct.checkpoint(true)
      res = res.union(temp.select(split($"O_GIT_PATH", "/")(0).as("REPO"))).checkpoint(true)
      cntNode = temp.count
      println(cntNode)
    } while (cntNode > 0)
    res.distinct
  }


  /** Extracts the list of original repos from the npm registry.
    *
    * @param orig is the dataframe of the repos deemed original.
    * @return A rdd of node_module github REPO_OWNER and REPOSITORY.
    */
  def getEssentialNodeGitRepos(orig: DataFrame): RDD[(String, String)] = {
    getEssentialNodeRepos(orig).rdd.
      map[String](x=> x.get(0).toString).
      map[Option[String]](x=> getGitInfoFromNPMRegistry(x)).
      filter(_.isDefined).map(_.get).
      map[Option[(String, String)]](x=> extractInfoFromResponse(x)).
      filter(_.isDefined).map(_.get)
  }

  def getEssentialNodeGitRepos(file: String): RDD[(String, String)] = {
    sc.textFile(file).
      map[Option[String]](x=> getGitInfoFromNPMRegistry(x)).
      filter(_.isDefined).map(_.get).
      map[Option[(String, String)]](x=> extractInfoFromResponse(x)).
      filter(_.isDefined).map(_.get)
  }
}

object NpmToGitMapper{
  val obj = new NpmToGitMapper

  def main(args: Array[String]): Unit = {
    //    println(obj.extractInfoFromResponse(obj.getGitInfoFromNPMRegistry("astw").get).get)
    //    val repos = obj.getEssentialNodeGitRepos(orig).map(x=> x._1 + "/" + x._2)

    val repos = obj.
      getEssentialNodeGitRepos("s3://shabbirhussain/node_modules").
//      getEssentialNodeGitRepos("/Users/shabbirhussain/Google Drive/NEU/Notes/CS 8678 Project/Déjà vu Episode II - Attack of The Clones/Data/node_modules.txt").
      map(x=> {println(x); x._1 + "/" + x._2})
//    dataStore.loadProjectsQueue(repos)
  }
}