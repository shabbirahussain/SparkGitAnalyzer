package org.reactorlabs.jshealth.models

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Schemas extends Enumeration{
  type FileType = Value
  val FILE_METADATA, COMMIT_MESSAGES, CONTENTS, INDEX = Value

  type Metadata = (
    StructType    // Columns datatypes
      , Seq[String] // Uniqueness based on
      , Seq[String] // Column Names
    )
  val asMap: Map[Schemas.Value, Metadata] = Map(
    FILE_METADATA  -> (StructType(Array(
      StructField("REPO_OWNER",   StringType, nullable = false),
      StructField("REPOSITORY",   StringType, nullable = false),
      StructField("GIT_PATH",     StringType, nullable = false),
      StructField("HASH_CODE",    StringType, nullable = true),
      StructField("COMMIT_TIME",  LongType,   nullable = false),
      StructField("COMMIT_ID",    StringType, nullable = false)))
      , Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH", "HASH_CODE", "COMMIT_ID")
      , Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH", "HASH_CODE", "COMMIT_TIME", "COMMIT_ID")),
    COMMIT_MESSAGES -> (StructType(Array(
      StructField("COMMIT_ID",    StringType, nullable = false),
      StructField("AUTHOR",       StringType, nullable = false),
      StructField("LONG_MESSAGE", StringType, nullable = false)))
      , Seq("COMMIT_ID")
      , Seq("COMMIT_ID", "AUTHOR", "LONG_MESSAGE")),
    INDEX   -> (StructType(Array(
      StructField("HASH_CODE", StringType, nullable = false)))
      , Seq("HASH_CODE")
      , Seq("HASH_CODE")),
    CONTENTS  -> (StructType(Array(
      StructField("HASH_CODE", StringType, nullable = false),
      StructField("CONTENTS",  StringType, nullable = false)))
      , Seq("HASH_CODE")
      , Seq("HASH_CODE", "CONTENTS"))
  )
}