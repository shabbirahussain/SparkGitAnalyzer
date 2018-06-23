
import java.nio.file.Paths
import breeze.util.BloomFilter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.reactorlabs.jshealth.models._
import org.reactorlabs.jshealth.util.DataFrameUtils._
import org.reactorlabs.jshealth.util._
import scala.concurrent.Future
import org.reactorlabs.jshealth.Main._
import sqlContext.implicits._

sc.setCheckpointDir("%s/checkpoints/".format(analysisStoreLoc))

@transient val wHash = Window.partitionBy("HASH_CODE")
@transient val wHashTimeAsc = wHash.orderBy($"COMMIT_TIME")
@transient val wPathTimeDsc = Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").
  orderBy($"COMMIT_TIME".desc)

val allData = read(fileStorePath, Schemas.FILE_METADATA).
  repartition(1024).
  withColumn("HEAD_COMMIT_TIME", first("COMMIT_TIME").over(wPathTimeDsc)).
  withColumn("HEAD_HASH_CODE"  , first("HASH_CODE"  ).over(wPathTimeDsc)).
  select($"REPO_OWNER",
    $"REPOSITORY",
    $"GIT_PATH",
    $"HASH_CODE",
    $"COMMIT_TIME",
    $"HEAD_COMMIT_TIME",
    $"HEAD_HASH_CODE",
    first("REPO_OWNER" ).over(wHashTimeAsc).as("O_REPO_OWNER"),
    first("REPOSITORY" ).over(wHashTimeAsc).as("O_REPOSITORY"),
    first("GIT_PATH"   ).over(wHashTimeAsc).as("O_GIT_PATH"),
    first("COMMIT_TIME").over(wHashTimeAsc).as("O_COMMIT_TIME"),
    first("HEAD_HASH_CODE"  ).over(wHashTimeAsc).as("O_HEAD_HASH_CODE"),
    first("HEAD_COMMIT_TIME").over(wHashTimeAsc).as("O_HEAD_COMMIT_TIME"),
    when(count("COMMIT_TIME").over(wHash) === 1, lit(true)).as("IS_UNIQUE")
  ).
  repartition(1024).
  checkpoint("allData").
  persist(StorageLevel.MEMORY_ONLY_SER)

val orig = allData.
  filter($"HASH_CODE".isNotNull && $"COMMIT_TIME" === $"O_COMMIT_TIME").
  select($"O_REPO_OWNER",
    $"O_REPOSITORY",
    $"O_GIT_PATH",
    $"O_COMMIT_TIME",
    $"HASH_CODE",
    $"O_HEAD_COMMIT_TIME",
    $"O_HEAD_HASH_CODE",
    $"IS_UNIQUE"
  ).distinct() // In case 2 orig files are committed at different paths in the same commit.

// List all the copied content (hash equal).
val copy = allData.drop("IS_UNIQUE").
  filter($"HASH_CODE".isNotNull  && $"COMMIT_TIME" =!= $"O_COMMIT_TIME").
  filter(
    $"REPO_OWNER" =!= $"O_REPO_OWNER" ||
      $"REPOSITORY" =!= $"O_REPOSITORY" ||
      $"GIT_PATH"   =!= $"O_GIT_PATH"
  ). // Prevent file revert getting detected as copy
  filter(!($"O_HEAD_HASH_CODE".isNull && $"O_HEAD_COMMIT_TIME" === $"COMMIT_TIME")) // Ignore immediate moves
val headCopy = copy.filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME")

allData.describe()
orig.describe()
copy.describe()