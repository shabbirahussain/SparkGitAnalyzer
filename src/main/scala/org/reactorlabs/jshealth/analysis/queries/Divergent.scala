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

val divergentCopyCount = copy.
  filter($"COMMIT_TIME" =!= $"HEAD_COMMIT_TIME").
  select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.
  join(orig.
    filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").   // Head only
    filter($"HASH_CODE".isNotNull).
    select(
      $"O_REPO_OWNER".as("REPO_OWNER"),
      $"O_REPOSITORY".as("REPOSITORY"),
      $"O_GIT_PATH"  .as("GIT_PATH"),
      $"IS_UNIQUE"
    )
    , usingColumns= Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
  groupBy("IS_UNIQUE").count.collect

