package org.reactorlabs.jshealth.util

import breeze.util.BloomFilter
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.approx_count_distinct
import org.apache.spark.storage.StorageLevel

object DataFrameUtils extends Serializable {
  /** Generates a seq of bloom filters for the columns specified.
    *
    * @param dataFrame is the input dataframe to process.
    * @param cols is the seq of columns to generate filters for.
    * @param falsePositiveRate is the maximum number of false positives.
    * @return An indexed sequence of bloom filters in the order of the columns specified.
    */
  def makeBloomFilter(dataFrame: DataFrame, cols: Seq[Column], falsePositiveRate: Double = 0.001)
  : Seq[BloomFilter[String]] = {
    val temp  = dataFrame.select(cols:_*).distinct.persist(StorageLevel.MEMORY_ONLY)
    val stats = temp.select(temp.columns.map(c=> approx_count_distinct(c, rsd= 0.1).as(c)): _*).collect()(0)
    val rng   = cols.indices

    val bloomFilters = temp.rdd.mapPartitions(y=> {
      val bf = rng.map(i=> BloomFilter.optimallySized[String](stats.getLong(i), falsePositiveRate))
      y.foreach(row=> rng.foreach(i=> bf(i) += row.getAs[String](i)))
      Iterator(bf)
    }).reduce(_.zip(_).map(u => u._1 | u._2))

    temp.unpersist(blocking = false)
    bloomFilters
  }
}
