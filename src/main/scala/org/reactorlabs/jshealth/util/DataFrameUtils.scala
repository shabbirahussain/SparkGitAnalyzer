package org.reactorlabs.jshealth.util

import org.reactorlabs.jshealth.analysis.Analysis.sqlContext
import org.reactorlabs.jshealth.Main.sc

import java.nio.file.Paths
import breeze.util.BloomFilter
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
      val storeLoc = "%s/_sticky/%s".format(Paths.get(sc.getCheckpointDir.get).getParent.toString, objName)
      val colMapping = df.columns.
        zipWithIndex.
        map(x=> (x._1, x._1.replaceAll("[^a-zA-z0-9]", "_") + "#" + x._2)).
        toMap

      var newDf = df
      colMapping.foreach(x=> newDf = newDf.withColumnRenamed(x._1, x._2))
      newDf.
        write.
        option("codec", classOf[BZip2Codec].getName).
        mode(SaveMode.Ignore).
        save(storeLoc)

      newDf = sqlContext.read.load(storeLoc)
      colMapping.foreach(x=> newDf = newDf.withColumnRenamed(x._2, x._1))
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
