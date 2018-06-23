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
  val allData: DataFrame = _

  // List all min timestamp commit per hash.
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


  // Copy as Import
  val getParentFileName = udf[String, String]((s: String) => Paths.get(s).getParent match {
      case null => ""
      case e@_ => e.toString
    })
  val getFileName = udf[String, String]((s: String) => Paths.get(s).getFileName.toString)

  val headCopy = copy.filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME")
  // Make bloom filter to single out only records which belong to an original repo from which someone copied something.
  val bf = headCopy.makeBloomFilter(Seq($"O_REPO_OWNER", $"O_REPOSITORY"), 0.0001)
  @transient val wRepoFolderTimeAsc = Window.partitionBy("REPO_OWNER", "REPOSITORY", "FOLDER").orderBy($"COMMIT_TIME")
  // Get all the paths fingerprint present in the all repo upto a max(original commit point) used by the copier.
  val allFolderHash = allData.
    // Filter extra data with bloom filters
    filter(x=> bf(0).contains(x.getString(0)) && bf(1).contains(x.getString(1))).
    // Extract first level folder to aggregate repository.
    withColumn("FOLDER", getParentFileName($"GIT_PATH")).
    withColumn("crc32(FILE_NAME)", crc32(getFileName($"GIT_PATH"))).
    // Create checksums of required columns.
    withColumn("crc32(HASH_CODE)", crc32($"HASH_CODE")).
    withColumn("crc32(PREV_HASH_CODE)", lag("crc32(HASH_CODE)", 1).
      over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy($"COMMIT_TIME"))).
    // Create flags to identify commit type for a path.
    withColumn("IS_ADDITION", $"crc32(PREV_HASH_CODE)".isNull).
    withColumn("IS_DELETION", $"crc32(HASH_CODE)".isNull).
    // Null value replace checksums of hash. We will treat them as zero in further calculations.
    withColumn("crc32(HASH_CODE)"     , when($"IS_DELETION", 0).otherwise($"crc32(HASH_CODE)")).
    withColumn("crc32(PREV_HASH_CODE)", when($"IS_ADDITION", 0).otherwise($"crc32(PREV_HASH_CODE)")).
    // Overcome double counting for the same path. This way we are only considering new additions.
    withColumn("crc32(FILE_NAME)",
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
    filter($"count(crc32(FILE_NAME))" > 0).
    groupBy("REPO_OWNER", "REPOSITORY", "FOLDER", "sum(crc32(HASH_CODE))", "sum(crc32(FILE_NAME))", "count(crc32(FILE_NAME))").
    agg(min($"COMMIT_TIME").as("COMMIT_TIME")). // Pick the min state of folder is reverted
    checkpoint("allFolderHash")

  val copyFolderHash = headCopy. // Pick head paths only.
    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"COMMIT_TIME", crc32($"HASH_CODE")).
    distinct.
    // Extract last level folder to aggregate repository.
    withColumn("FOLDER", getParentFileName($"GIT_PATH")).
    withColumn("crc32(FILE_NAME)", crc32(getFileName($"GIT_PATH"))).
    groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER").
    agg(sum("crc32(HASH_CODE)"), sum("crc32(FILE_NAME)"), count("crc32(FILE_NAME)")).
    checkpoint("copyFolderHash")

  // Do join to find if all the paths from original at the time of copy exists in the copied folder.
  val bloomFilters = (0 until 2).map(_=> BloomFilter.optimallySized[String](copyFolderHash.count(), falsePositiveRate = 0.0001))
  val isFirstOfItsName = udf((e: String) => bloomFilters.
      filter(!_.contains(e)).
      take(1).
      map(_+=e).
      nonEmpty
  )


  val copyAsImportExamples1 = copyFolderHash.as("A").limit(1000).
    repartition($"REPO_OWNER", $"REPOSITORY", $"FOLDER", $"COMMIT_TIME").
    join(allFolderHash.as("B").
      select(
        $"REPO_OWNER" .as("O_REPO_OWNER"),
        $"REPOSITORY" .as("O_REPOSITORY"),
        $"COMMIT_TIME".as("O_COMMIT_TIME"),
        $"FOLDER".as("O_FOLDER"),
        $"sum(crc32(HASH_CODE))",
        $"sum(crc32(FILE_NAME))",
        $"count(crc32(FILE_NAME))"
      )
      , joinType = "LEFT_OUTER"
      , joinExprs = when(
        $"A.sum(crc32(HASH_CODE))" === $"B.sum(crc32(HASH_CODE))" &&
          $"A.sum(crc32(FILE_NAME))"   === $"B.sum(crc32(FILE_NAME))" &&
          $"A.count(crc32(FILE_NAME))" === $"B.count(crc32(FILE_NAME))" &&
          $"COMMIT_TIME" > $"O_COMMIT_TIME" &&
          ($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY")
        , isFirstOfItsName(concat($"REPO_OWNER", $"REPOSITORY", $"FOLDER", $"COMMIT_TIME")))
    ).
    select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER", "O_REPO_OWNER", "O_REPOSITORY", "O_FOLDER", "A.count(crc32(FILE_NAME))", "O_COMMIT_TIME").
    checkpoint("copyAsImportExamples_tmp").
    groupBy("REPO_OWNER", "REPOSITORY", "FOLDER", "count(crc32(FILE_NAME))").
    agg(first($"O_COMMIT_TIME"), first($"O_REPO_OWNER"), first("O_REPOSITORY"), first($"O_FOLDER")).
    checkpoint("copyAsImportExamples1")

  val df = copyAsImportExamples1

  df.
    checkpoint("PermName").
    select()

  val copyAsImportExamples =
    copyFolderHash.as("A").
      repartition($"REPO_OWNER", $"REPOSITORY", $"FOLDER", $"COMMIT_TIME").
      join(allFolderHash.as("B").
        select(
          $"REPO_OWNER" .as("O_REPO_OWNER"),
          $"REPOSITORY" .as("O_REPOSITORY"),
          $"COMMIT_TIME".as("O_COMMIT_TIME"),
          $"FOLDER".as("O_FOLDER"),
          $"sum(crc32(HASH_CODE))",
          $"sum(crc32(FILE_NAME))",
          $"count(crc32(FILE_NAME))"
        )
        , joinType = "LEFT_OUTER"
        , joinExprs = when(
          $"A.sum(crc32(HASH_CODE))" === $"B.sum(crc32(HASH_CODE))" &&
            $"A.sum(crc32(FILE_NAME))"   === $"B.sum(crc32(FILE_NAME))" &&
            $"A.count(crc32(FILE_NAME))" === $"B.count(crc32(FILE_NAME))" &&
            $"COMMIT_TIME" > $"O_COMMIT_TIME" &&
            ($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY")
          , isFirstOfItsName(concat($"REPO_OWNER", $"REPOSITORY", $"FOLDER", $"COMMIT_TIME")))
      ).
      select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER", "O_REPO_OWNER", "O_REPOSITORY", "O_FOLDER", "A.count(crc32(FILE_NAME))", "O_COMMIT_TIME").
      groupBy("REPO_OWNER", "REPOSITORY", "FOLDER", "count(crc32(FILE_NAME))").
      agg(first($"O_COMMIT_TIME"), first($"O_REPO_OWNER"), first("O_REPOSITORY"), first($"O_FOLDER")).
      checkpoint("copyAsImportExamples")

  val copyAsImportCount = copyAsImportExamples.
    select(
      $"REPO_OWNER",
      $"REPOSITORY",
      $"FOLDER",
      $"count(crc32(FILE_NAME))",
      $"first(O_REPO_OWNER, false)".isNotNull.as("IS_COPY_AS_IMPORT")
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





  copyAsImportExamples.filter($"first(O_REPO_OWNER, false)".isNull).show


  val temp = manualCopyExamples.
    withColumn("FOLDER", getParentFileName($"GIT_PATH")).
    withColumn("FILE_NAME", getFileName($"GIT_PATH")).
    withColumn("IS_JQUERY", sum(when(lower($"GIT_PATH").contains("jquery"), 1).otherwise(0)).over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "FOLDER"))).
    persist
  temp.filter($"IS_JQUERY" === 0).
    select("REPO_OWNER", "REPOSITORY", "FOLDER").distinct.count
    show(20, false)


  allData.
    filter($"REPO_OWNER" === "AvianFlu" && $"REPOSITORY" === "ntwitter" && $"GIT_PATH".startsWith("lib/twitter-node/")).
//    select($"GIT_PATH").distinct.
    show(20, false)


  allData.
    filter($"REPO_OWNER" === "unscene" && $"REPOSITORY" === "node-oauth" && $"GIT_PATH".startsWith("lib/twitter-node/")).
//    select($"GIT_PATH").distinct.
    show(20, false)
  ""



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


  copyAsImportExamples.filter($"FOLDER".contains("www/")).show


  val unidentifiedNodeCopies = headCopy. // Pick head paths only.
    filter($"GIT_PATH".contains("node_modules/")).
    select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.
    join(copyAsImportExamples.select("REPO_OWNER", "REPOSITORY").distinct.withColumn("JNK", lit(true)),
      usingColumns = Seq("REPO_OWNER", "REPOSITORY"),
      joinType = "LEFT_OUTER").
    filter($"JNK".isNull).
    persist(StorageLevel.DISK_ONLY)
  val unidentifiedNodeCopiesCnt  =  unidentifiedNodeCopies.count
















  ///////////////////////////////////////////////////////////////////////////////////////////////////////
  // Count unique files in the head.
  val (allPathsCount, origPathsCount, copyPathsCount) = {
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

    (allPathsCount, origPathsCount, copyPathsCount)
  }

  // Divergent Analysis = 17795
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
