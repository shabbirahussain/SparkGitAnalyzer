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
import org.reactorlabs.jshealth.analysis.junk.Analysis._

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Queries are below
///////////////////////////////////////////////////////////////////////////////////////////////////////

val allPathsCount = allData.
  filter($"HASH_CODE".isNotNull).                       // Paths which aren't deleted
  filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").       // Head only
  select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count

val origPathsCount = orig.
  filter($"O_COMMIT_TIME" === $"O_HEAD_COMMIT_TIME"). // Head only
  select($"O_REPO_OWNER", $"O_REPOSITORY", $"O_GIT_PATH", $"IS_UNIQUE").distinct.
  groupBy("IS_UNIQUE").count.collect

val copyPathsCount = copy.
  filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").       // Head only
  select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
