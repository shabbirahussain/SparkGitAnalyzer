package org.reactorlabs.jshealth.util

import java.io.File

import org.reactorlabs.jshealth.analysis.Analysis.sqlContext
import org.reactorlabs.jshealth.Main.sc
import org.reactorlabs.jshealth.util
import java.nio.file.Paths

import breeze.util.BloomFilter
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object DataFrameUtils extends Serializable {
  implicit class DFWithExtraOperations(df: DataFrame) {
    /** Creates a named checkpoint and returns reference to it. Ignores the saving if already exists.
      *
      * @param objName is the checkpoint object name.
      * @return DataFrame reference from the checkpoint
      */
    def checkpoint(objName: String): DataFrame = {
      def getMaskedColumnNames(columns: Seq[String]): Map[String, String] = columns.zipWithIndex.
        map(x=> (x._1, x._1.replaceAll("[^a-zA-z0-9]", "_") + "#" + x._2)).toMap

      val storePath = "%s/_sticky/%s".format(Paths.get(sc.getCheckpointDir.get).getParent.toString, objName)
      val colMapping = getMaskedColumnNames(df.columns)

      val fs = FileSystem.get(sc.hadoopConfiguration)
      val hadoopPath = new Path(storePath)
      if (fs.exists(hadoopPath) && fs.getContentSummary(hadoopPath).getFileCount == 0)
        fs.delete(hadoopPath, true)
      println(storePath)

      df.
        withColumnsRenamed(colMapping).
        write.
        option("codec", classOf[BZip2Codec].getName).
        mode(SaveMode.Ignore).
        save(storePath)

      sqlContext.
        read.load(storePath).
        withColumnsRenamed(colMapping.map(_.swap))
    }

    /** Performs column renaming in bulk.
      *
      * @param colMapping is the input to source to destination column mapping.
      * @return DataFrame with specified columns renamed.
      */
    def withColumnsRenamed(colMapping: Map[String, String]): DataFrame = {
      var newDf = df
      colMapping.foreach(x=> newDf = newDf.withColumnRenamed(x._1, x._2))
      newDf
    }

    /** Generates a seq of bloom filters for the columns specified.
      *
      * @param cols              is the seq of columns to generate filters for.
      * @param falsePositiveRate is the maximum number of false positives.
      * @return An indexed sequence of bloom filters in the order of the columns specified.
      */
    def makeBloomFilter(cols: Seq[Column], falsePositiveRate: Double = 0.001)
    : Seq[BloomFilter[String]] = {
      val temp = df.select(cols: _*).distinct.persist(StorageLevel.MEMORY_ONLY)
      val stats = temp.select(temp.columns.map(c => approx_count_distinct(c, rsd = 0.1).as(c)): _*).collect()(0)
      val rng = cols.indices

      val bloomFilters = temp.rdd.mapPartitions(y => {
        val bf = rng.map(i => BloomFilter.optimallySized[String](stats.getLong(i), falsePositiveRate))
        y.foreach(row => rng.foreach(i => bf(i) += row.getAs[String](i)))
        Iterator(bf)
      }).reduce(_.zip(_).map(u => u._1 | u._2))

      temp.unpersist(blocking = false)
      bloomFilters
    }
  }
}
