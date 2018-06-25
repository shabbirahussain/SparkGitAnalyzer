import java.nio.file.Paths

import breeze.util.BloomFilter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.reactorlabs.jshealth.models._
import org.reactorlabs.jshealth.util.DataFrameUtils._
import org.reactorlabs.jshealth.util._

import scala.concurrent.Future
import org.reactorlabs.jshealth.Main._
import sqlContext.implicits._
import org.reactorlabs.jshealth.analysis.junk.Analysis._

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Queries are below
///////////////////////////////////////////////////////////////////////////////////////////////////////

// Copy as Import
val getParentFileName = udf[String, String]((s: String) =>
  Paths.get(s).getParent match {
    case null => ""
    case e@_ => e.toString
  })
val getFileName = udf[String, String]((s: String) => Paths.get(s).getFileName.toString)

// Make bloom filter to single out only records which belong to an original repo from which someone copied something.
val allFolderHashBF = headCopy.makeBloomFilter(Seq($"O_REPO_OWNER", $"O_REPOSITORY"), 0.0001)

@transient val wRepoFolderTimeAsc = Window.
  partitionBy("REPO_OWNER", "REPOSITORY", "FOLDER").
  orderBy($"COMMIT_TIME")
@transient val wRowNumTimeAsc = Window.
  partitionBy($"sum(crc32(HASH_CODE))", $"sum(crc32(FILE_NAME))", $"count(crc32(FILE_NAME))").
  orderBy($"COMMIT_TIME")

// Get all the paths fingerprint present in the all repo upto a max(original commit point) used by the copier.
val rankedOrigFolderHash = allData.
  // Filter extra data with bloom filters
  filter(x=> allFolderHashBF(0).contains(x.getString(0)) && allFolderHashBF(1).contains(x.getString(1))).
  // Extract first level folder to aggregate repository.
  withColumn("FOLDER", getParentFileName($"GIT_PATH")).
  withColumn("crc32(FILE_NAME)", crc32(getFileName($"GIT_PATH"))).
  // Create checksums of required columns.
  withColumn("crc32(HASH_CODE)", crc32($"HASH_CODE")).
  withColumn("crc32(PREV_HASH_CODE)", lag("crc32(HASH_CODE)", 1).
    over(Window.
      partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").
      orderBy($"COMMIT_TIME"))
  ).
  // Create flags to identify commit type for a path.
  withColumn("IS_ADDITION", $"crc32(PREV_HASH_CODE)".isNull).
  withColumn("IS_DELETION", $"crc32(HASH_CODE)".isNull).
  // Null value replace checksums of hash. We will treat them as zero in further calculations.
  withColumn("crc32(HASH_CODE)"     , when($"IS_DELETION", 0).otherwise($"crc32(HASH_CODE)")).
  withColumn("crc32(PREV_HASH_CODE)", when($"IS_ADDITION", 0).otherwise($"crc32(PREV_HASH_CODE)")).
  // Overcome double counting for the same path. This way we are only considering new additions.
  withColumn("crc32(FILE_NAME)",
    // Weired case where we get 2+ consecutive nulls for a file. How can file be deleted twice without getting added? (revert maybe)
    // Ex: $"REPO_OWNER" === "2gis" && $"REPOSITORY" === "mapsapi" && $"GIT_PATH".startsWith("src/DGLayer/test")
    when( $"IS_ADDITION" && $"IS_DELETION", 0).  // <-- this one is weired case patch
    when($"IS_ADDITION",  $"crc32(FILE_NAME)").
    when($"IS_DELETION", -$"crc32(FILE_NAME)").
    otherwise(0)).
  withColumn("crc32(HASH_CODE)", $"crc32(HASH_CODE)" - $"crc32(PREV_HASH_CODE)").
  // Fingerprint all additions in a commit for every folder.
  groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER").
  agg(sum($"crc32(HASH_CODE)")
    , sum($"crc32(FILE_NAME)")
    , sum(signum($"crc32(FILE_NAME)").cast(IntegerType)).as("sum(signum(crc32(FILE_NAME)))")
  ).
  // Do cumulative sum of fingerprints.
  select(
    $"REPO_OWNER", $"REPOSITORY", $"COMMIT_TIME", $"FOLDER",
    sum($"sum(crc32(HASH_CODE))").over(wRepoFolderTimeAsc).as("sum(crc32(HASH_CODE))"),
    sum($"sum(crc32(FILE_NAME))").over(wRepoFolderTimeAsc).as("sum(crc32(FILE_NAME))"),
    sum($"sum(signum(crc32(FILE_NAME)))").over(wRepoFolderTimeAsc).as("count(crc32(FILE_NAME))")
  ).
  // Remove all states where folders become empty.
  filter($"count(crc32(FILE_NAME))" > 0).
  // Pick the min state of folder (case when reverted state is identical to some earlier state).
  groupBy("REPO_OWNER", "REPOSITORY", "FOLDER", "sum(crc32(HASH_CODE))", "sum(crc32(FILE_NAME))", "count(crc32(FILE_NAME))").
  agg(min($"COMMIT_TIME").as("COMMIT_TIME")).
  select(
    $"REPO_OWNER" .as("O_REPO_OWNER"),
    $"REPOSITORY" .as("O_REPOSITORY"),
    $"COMMIT_TIME".as("O_COMMIT_TIME"),
    $"FOLDER".as("O_FOLDER"),
    $"sum(crc32(HASH_CODE))",
    $"sum(crc32(FILE_NAME))",
    $"count(crc32(FILE_NAME))",
    row_number().over(wRowNumTimeAsc).as("ROW_NUM")
  ).
  checkpoint("rankedOrigFolderHash").
  cache()


var copyAsImportExamples = headCopy. // Pick head paths only.
  select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"COMMIT_TIME", crc32($"HASH_CODE")).
  distinct.
  // Extract last level folder to aggregate repository.
  withColumn("FOLDER", getParentFileName($"GIT_PATH")).
  withColumn("crc32(FILE_NAME)", crc32(getFileName($"GIT_PATH"))).
  groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER").
  agg(sum("crc32(HASH_CODE)")
    , sum("crc32(FILE_NAME)")
    , count("crc32(FILE_NAME)")).
  checkpoint("copyFolderHash").
  select($"REPO_OWNER"
    , $"REPOSITORY"
    , $"COMMIT_TIME"
    , $"FOLDER"
    , $"sum(crc32(HASH_CODE))"
    , $"sum(crc32(FILE_NAME))"
    , $"count(crc32(FILE_NAME))"
    , lit(null).as("O_REPO_OWNER")
    , lit(null).as("O_REPOSITORY")
    , lit(null).as("O_FOLDER")
    , lit(null).as("O_COMMIT_TIME")
  ).cache

// Do multi-pass join to find if all the paths from original at the time of copy exists in the copied folder.
for(rng <- Seq((1, 1), (2, 10), (11, 100))){
  copyAsImportExamples =
    copyAsImportExamples.as("A").
      join(rankedOrigFolderHash.
        filter($"ROW_NUM".between(rng._1, rng._2)).
        as("B")
        , joinType = "LEFT_OUTER"
        , joinExprs =
            $"A.O_COMMIT_TIME".isNull &&
            $"A.sum(crc32(HASH_CODE))"   === $"B.sum(crc32(HASH_CODE))"   &&
            $"A.sum(crc32(FILE_NAME))"   === $"B.sum(crc32(FILE_NAME))"   &&
            $"A.count(crc32(FILE_NAME))" === $"B.count(crc32(FILE_NAME))" &&
            $"A.COMMIT_TIME" > $"B.O_COMMIT_TIME" &&
            ($"A.REPO_OWNER" =!= $"B.O_REPO_OWNER" || $"A.REPOSITORY" =!= $"B.O_REPOSITORY")
      ).
      select($"A.REPO_OWNER"
        , $"A.REPOSITORY"
        , $"A.COMMIT_TIME"
        , $"A.FOLDER"
        , $"A.sum(crc32(HASH_CODE))"
        , $"A.sum(crc32(FILE_NAME))"
        , $"A.count(crc32(FILE_NAME))"
        , coalesce($"A.O_REPO_OWNER" , $"B.O_REPO_OWNER" ).as("O_REPO_OWNER")
        , coalesce($"A.O_REPOSITORY" , $"B.O_REPOSITORY" ).as("O_REPOSITORY")
        , coalesce($"A.O_FOLDER"     , $"B.O_FOLDER"     ).as("O_FOLDER")
        , coalesce($"A.O_COMMIT_TIME", $"B.O_COMMIT_TIME").as("O_COMMIT_TIME")
      ).
      dropDuplicates("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER", "count(crc32(FILE_NAME))").
      cache

  println("After joining between %s count of manual = %d".
    format(rng, copyAsImportExamples.filter($"O_COMMIT_TIME".isNull).count))
}
copyAsImportExamples = copyAsImportExamples.
//  drop("sum(crc32(HASH_CODE))", "sum(crc32(FILE_NAME))").
  checkpoint("copyAsImportExamples")


val copyAsImportCount = copyAsImportExamples.
  select(
    $"REPO_OWNER",
    $"REPOSITORY",
    $"FOLDER",
    $"count(crc32(FILE_NAME))",
    $"O_REPO_OWNER".isNotNull.as("IS_COPY_AS_IMPORT")
  ).distinct.
  withColumn("PACKAGER_MANAGER_NAME",
    when($"FOLDER".contains("www/"), "WWW").
      when($"FOLDER".contains("node_modules/"), "NPM").
      when($"FOLDER".contains("bower_components/"), "BOWER").
      when($"FOLDER".contains(".bower-cache/"), "BOWER")
      otherwise("OTHERS")
  ).
  groupBy("IS_COPY_AS_IMPORT", "PACKAGER_MANAGER_NAME").
  agg(sum("count(crc32(FILE_NAME))")).collect


/*
val temp = allData.filter($"REPO_OWNER" === "2gis" && $"REPOSITORY" === "mapsapi").cache
val temp1 = temp.filter($"GIT_PATH".startsWith("src/DGLayer/test")).orderBy($"COMMIT_TIME".asc).checkpoint("temp1").cache
temp1.
  withColumn("crc32(FILE_NAME)", crc32(getFileName($"GIT_PATH"))).
  withColumn("crc32(HASH_CODE)", crc32($"HASH_CODE")).
  withColumn("crc32(PREV_HASH_CODE)", lag("crc32(HASH_CODE)", 1).
    over(Window.
      partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").
      orderBy($"COMMIT_TIME"))
  ).
  withColumn("IS_ADDITION", $"crc32(PREV_HASH_CODE)".isNull).
  withColumn("IS_DELETION", $"crc32(HASH_CODE)".isNull).
  // Null value replace checksums of hash. We will treat them as zero in further calculations.
  withColumn("crc32(HASH_CODE)"     , when($"IS_DELETION", 0).otherwise($"crc32(HASH_CODE)")).
  withColumn("crc32(PREV_HASH_CODE)", when($"IS_ADDITION", 0).otherwise($"crc32(PREV_HASH_CODE)")).
  // Overcome double counting for the same path. This way we are only considering new additions.
  withColumn("crc32(FILE_NAME)",
    when( $"IS_ADDITION" && $"IS_DELETION", 0).
    when($"IS_ADDITION",  $"crc32(FILE_NAME)").
    when($"IS_DELETION", -$"crc32(FILE_NAME)").
    otherwise(0)).
  withColumn("crc32(HASH_CODE)1", $"crc32(HASH_CODE)" - $"crc32(PREV_HASH_CODE)").
  select("HASH_CODE"
    , "COMMIT_TIME"
    , "IS_ADDITION"
    , "IS_DELETION"
    , "crc32(HASH_CODE)"
    , "crc32(PREV_HASH_CODE)"
    , "crc32(FILE_NAME)"
    , "crc32(HASH_CODE)1"
  ).
  orderBy("COMMIT_TIME").
  show


temp1.
  withColumn("FOLDER", getParentFileName($"GIT_PATH")).
  withColumn("crc32(FILE_NAME)", crc32(getFileName($"GIT_PATH"))).
  withColumn("crc32(HASH_CODE)", crc32($"HASH_CODE")).
  withColumn("crc32(PREV_HASH_CODE)", lag("crc32(HASH_CODE)", 1).
    over(Window.
      partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").
      orderBy($"COMMIT_TIME"))
  ).
  withColumn("IS_ADDITION", $"crc32(PREV_HASH_CODE)".isNull).
  withColumn("IS_DELETION", $"crc32(HASH_CODE)".isNull).
  // Null value replace checksums of hash. We will treat them as zero in further calculations.
  withColumn("crc32(HASH_CODE)"     , when($"IS_DELETION", 0).otherwise($"crc32(HASH_CODE)")).
  withColumn("crc32(PREV_HASH_CODE)", when($"IS_ADDITION", 0).otherwise($"crc32(PREV_HASH_CODE)")).
  // Overcome double counting for the same path. This way we are only considering new additions.
  withColumn("crc32(FILE_NAME)",
    when( $"IS_ADDITION" && $"IS_DELETION", 0).
    when($"IS_ADDITION",  $"crc32(FILE_NAME)").
    when($"IS_DELETION", -$"crc32(FILE_NAME)").
    otherwise(0)).
  withColumn("crc32(HASH_CODE)", $"crc32(HASH_CODE)" - $"crc32(PREV_HASH_CODE)").
  // Fingerprint all additions in a commit for every folder.
  groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER").
  agg(sum($"crc32(HASH_CODE)"), sum($"crc32(FILE_NAME)"), sum(signum($"crc32(FILE_NAME)"))).
  // Do cumulative sum of fingerprints.
  select(
    $"REPO_OWNER", $"REPOSITORY", $"COMMIT_TIME", $"FOLDER",
    sum($"sum(crc32(HASH_CODE))").over(wRepoFolderTimeAsc).as("sum(crc32(HASH_CODE))"),
    sum($"sum(crc32(FILE_NAME))").over(wRepoFolderTimeAsc).as("sum(crc32(FILE_NAME))"),
    sum($"sum(signum(crc32(FILE_NAME)))").over(wRepoFolderTimeAsc).as("count(crc32(FILE_NAME))")
  ).
  orderBy("COMMIT_TIME").
  show

allFolderHash.filter($"REPO_OWNER" === "2gis" && $"REPOSITORY" === "mapsapi" && $"FOLDER" === "src/DGLayer/test")
*/
///////////////////////////////////////////////////////////////////////////////////////////////////////
// Extra validations
///////////////////////////////////////////////////////////////////////////////////////////////////////
val folders = copyAsImportExamples.
  select("REPO_OWNER", "REPOSITORY", "FOLDER", "count(crc32(FILE_NAME))").distinct.
  withColumn("PACKAGER_MANAGER_NAME",
    when($"FOLDER".contains("node_modules/"), "NPM").
      when($"FOLDER".contains("bower_components/"), "BOWER").
      when($"FOLDER".contains(".bower-cache/"), "BOWER").
      when($"FOLDER".contains("bower/"), "BOWER").
      when($"FOLDER".contains("www/"), "WWW")
      otherwise("OTHERS")
  ).
  filter($"PACKAGER_MANAGER_NAME"==="OTHERS").
  select($"FOLDER").
  rdd.flatMap(x=> x.getAs[String]("FOLDER").split("/")).
  toDF("FOLDER").
  groupBy("FOLDER").count.filter($"COUNT">1000).
  //    persist().
  orderBy($"COUNT".desc).
  show


val unidentifiedNodeCopies = headCopy. // Pick head paths only.
  filter($"GIT_PATH".contains("node_modules/")).
  select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.
  join(copyAsImportExamples.select("REPO_OWNER", "REPOSITORY").distinct.withColumn("JNK", lit(true)),
    usingColumns = Seq("REPO_OWNER", "REPOSITORY"),
    joinType = "LEFT_OUTER").
  filter($"JNK".isNull).
  persist(StorageLevel.DISK_ONLY)
val unidentifiedNodeCopiesCnt  =  unidentifiedNodeCopies.count