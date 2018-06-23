package org.reactorlabs.jshealth.analysis.junk

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

object Analysis  {
  // Following stub definitions are loaded from startup script located in resources.
  val allData: DataFrame = _
  val orig: DataFrame = _
  val copy: DataFrame = _
  val headCopy: DataFrame = _





















  ///////////////////////////////////////////////////////////////////////////////////////////////////////


  // Divergent Analysis = 17795


//
//
//    copyAsImportExamples.filter(!$"FOLDER".contains("node_modules/")).
//      rdd.map(x=> x.mkString(",")).
//      coalesce(1).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/copyAsImportExamples")
//
//    copyAsImportExamples.filter(!$"IS_NODE_MODULE" && $"count(FILE_NAME)" > 1).show
//
//
//
//    headCopy.filter($"REPO_OWNER" === "paulmillr" && $"REPOSITORY" === "es6-shim").show
//    allCopyPathsXRepo.filter($"REPO_OWNER" === "paulmillr" && $"REPOSITORY" === "es6-shim" && $"FOLDER" === "node_modules/chai/").show
//    allFolderHash.filter($"REPO_OWNER" === "chaijs" && $"REPOSITORY" === "chai" && $"FOLDER" === "").show
//
//    headCopy.filter($"REPO_OWNER" === "paulmillr" && $"REPOSITORY" === "es6-shim" && $"GIT_PATH".startsWith("node_modules/chai/")).show
//    allFolderHash.filter($"REPO_OWNER" === "chaijs" && $"REPOSITORY" === "chai" && $"FOLDER" === "" && $"COMMIT_TIME" === "1453982071").show
//
//
//    allData.filter($"REPO_OWNER" === "chaijs" && $"REPOSITORY" === "chai" && $"COMMIT_TIME" <= "1453982071" && !$"GIT_PATH".contains("/")).
//      orderBy($"GIT_PATH", $"COMMIT_TIME").
//      show(1000)
//
//    copyAsImportCount



    // Obsolete code analysis
    val activeRepoObsoleteCopyCount = {
      // List of all the copied content which is at the head of that path.
      val undeletedCopiesAtHead = copy.
        filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME"). // Head only
        filter($"HASH_CODE".isNotNull)              // Paths which aren't deleted

      val repoLastCommitTime = allData.
        groupBy("REPO_OWNER", "REPOSITORY").max("COMMIT_TIME").
        withColumnRenamed("max(COMMIT_TIME)", "REPO_LAST_COMMIT_TIME")

      // Repos which have a later commit than bug. (Only active js development.)
      // TODO: will moving a file count towards obsolete code?
      // TODO: Merge following two commands
      val activeRepoObsoleteCopyCount = undeletedCopiesAtHead.
        filter($"O_HEAD_HASH_CODE" =!= $"HASH_CODE"). // Specifies original was fixed after
        // filter($"O_FIX_HASH_CODE".isNotNull).      // Specifies original was fixed after and the fix wasn't a delete
        join(repoLastCommitTime, Seq("REPO_OWNER", "REPOSITORY")).
        withColumn("IS_ACTIVE", $"REPO_LAST_COMMIT_TIME" > $"O_HEAD_COMMIT_TIME").
        select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "IS_ACTIVE").distinct.
        groupBy("IS_ACTIVE").count.collect

      activeRepoObsoleteCopyCount
    }

    /*s


          val test = copy.
            filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").   // Pick head paths only.
            filter($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY").
            filter(length($"GIT_PATH") > length($"O_GIT_PATH")).
            filter($"GIT_PATH".contains($"O_GIT_PATH")).
            join(copyAsImportExamples.select("REPO_OWNER", "REPOSITORY").withColumn("JNK", lit(true)).distinct,
              usingColumns = Seq("REPO_OWNER", "REPOSITORY"),
              joinType = "LEFT_OUTER").
            filter($"JNK".isNull).persist(StorageLevel.MEMORY_AND_DISK_SER)

          test.filter($"GIT_PATH".contains("node_modules/")).count
          test.filter($"GIT_PATH".contains("node_modules/") && $"O_GIT_PATH".contains("node_modules/")).count

          test.filter(!$"GIT_PATH".contains("node_modules/")).show


          copy.filter($"REPO_OWNER" === "mraleph" && $"REPOSITORY" === "mraleph.github.com" && $"GIT_PATH".contains("irhydra/2.bak/packages/core_elements/src/web-animations-next/node_modules/mocha/")).
            filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").                                       // Pick head paths only.
            filter($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY").   // They should have a different repo to compare
            filter($"GIT_PATH".contains($"O_GIT_PATH")).
            filter(length($"GIT_PATH") > length($"O_GIT_PATH")).
            withColumn("GIT_PATH_PREFIX", $"GIT_PATH".substr(lit(0), length($"GIT_PATH") - length($"O_GIT_PATH"))).
            groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY").
            agg(max($"O_COMMIT_TIME")).show


        truePathCopy.filter($"REPO_OWNER" === "mraleph" && $"REPOSITORY" === "mraleph.github.com" && $"GIT_PATH_PREFIX".contains("irhydra/2.bak/packages/core_elements/src/web-animations-next/node_modules/mocha/")).show
                truePathCopy.filter($"REPO_OWNER" === "oscmejia" && $"REPOSITORY" === "tutorial-nodejs-cli").show


        allJSFilesAtTimeOfCopyFromSrc.filter($"REPO_OWNER" === "mochajs" && $"REPOSITORY" === "mocha").show

        allCopyPathsXRepo.filter($"REPO_OWNER" === "mraleph" && $"REPOSITORY" === "mraleph.github.com").show




          allData.filter($"REPO_OWNER" === "mochajs" && $"REPOSITORY" === "mocha" && !$"GIT_PATH".contains("/")).show


        allData.filter($"REPO_OWNER" === "mochajs" && $"REPOSITORY" === "mocha" && !$"GIT_PATH".contains("/")).
          rdd.coalesce(1, true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")

                allCopyPathsXRepo.filter($"REPO_OWNER" === "oscmejia" && $"REPOSITORY" === "tutorial-nodejs-cli").show


                allData.filter($"REPO_OWNER" === "oscmejia" && $"REPOSITORY" === "tutorial-nodejs-cli" && $"GIT_PATH".contains("lib/commander.js")).show


                allData.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" === "commander.js" && ).show
                allData.filter($"HASH_CODE" === "cdd206a9d678c529fe6ec3f44483e8a90368c8ec").agg(min("COMMIT_TIME")).collect

                allJSFilesAtTimeOfCopyFromSrc.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" === "commander.js").show


                allData.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" ===  "commander.js" && $"COMMIT_TIME" <= "1344026849").
                  orderBy($"GIT_PATH", $"COMMIT_TIME").
                  select("GIT_PATH", "HASH_CODE", "COMMIT_TIME").
                  rdd.coalesce(1, true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")


                allJSFilesAtTimeOfCopyFromSrc.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" === "commander.js").show



            */


  // Chain copy res = 0
//  {
//    val connect = copy.
//      withColumn("C", concat($"REPO_OWNER", lit("/"), $"REPOSITORY")).
//      withColumn("O", concat($"O_REPO_OWNER", lit("/"), $"O_REPOSITORY")).
//      select("O", "C").checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    val cc = connect.as("A").join(connect.as("B"), joinExprs = $"A.O" === $"B.C")
//
//
//
//
//  }


  // Phase of copy
//    val testTime = df.filter(lower($"GIT_PATH").contains("test/")).
//      groupBy($"REPO_OWNER", $"REPOSITORY").
//      agg(min("COMMIT_TIME")).withColumnRenamed("min(COMMIT_TIME)", "T_COMMIT_TIME").
//      checkpoint(true)
//
////    val copyPhase = df.groupBy($"REPO_OWNER", $"REPOSITORY").
////      agg(min("COMMIT_TIME"), max("COMMIT_TIME"), count("COMMIT_TIME"), countDistinct("GIT_PATH")).
////      join(copy.select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME"), usingColumns = Seq("REPO_OWNER", "REPOSITORY")).
////      withColumn("COPY_PHASE", ($"COMMIT_TIME" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)")).
////      join(testTime, usingColumns = Seq("REPO_OWNER", "REPOSITORY"), joinType = "LEFT_OUTER").
////      withColumn("T_COMMIT_TIME1", when($"T_COMMIT_TIME".isNull, $"max(COMMIT_TIME)").otherwise($"T_COMMIT_TIME")).
////      withColumn("TEST_PHASE", ($"T_COMMIT_TIME1" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)")).
////      groupBy("count(COMMIT_TIME)", "count(DISTINCT GIT_PATH)").agg(mean("COPY_PHASE"), mean("TEST_PHASE")).
////      checkpoint(true)
//
//    //val copy2 = copy.select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME")
//    val copy3 = copy.groupBy("REPO_OWNER", "REPOSITORY").min("COMMIT_TIME").withColumnRenamed("min(COMMIT_TIME)", "COMMIT_TIME")
//    val copyPhase = allData.groupBy($"REPO_OWNER", $"REPOSITORY").
//      agg(min("COMMIT_TIME"), max("COMMIT_TIME"), countDistinct("COMMIT_TIME"), countDistinct("GIT_PATH")).
//      join(copy3, usingColumns = Seq("REPO_OWNER", "REPOSITORY")).
//      withColumn("COPY_PHASE", ($"COMMIT_TIME" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)" + 1)).
//      groupBy("count(DISTINCT COMMIT_TIME)", "count(DISTINCT GIT_PATH)").agg(sum("COPY_PHASE"), count("COPY_PHASE")).
//      checkpoint(true)
//
//    copyPhase.rdd.map(_.mkString(",")).
//      coalesce(1, shuffle = true).
//      saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/copyPhase")


}
