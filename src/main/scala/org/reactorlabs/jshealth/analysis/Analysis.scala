package org.reactorlabs.jshealth.analysis

import org.reactorlabs.jshealth.Main.{prop, sc, spark}
import org.reactorlabs.jshealth.util.DataFrameUtils

object Analysis  {
  import org.apache.hadoop.io.compress.BZip2Codec
  import org.apache.spark.sql.{SQLContext, SparkSession, Column, DataFrame}
  import breeze.util.BloomFilter
  import org.apache.spark.sql
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, Decimal}
  import org.apache.spark.storage.StorageLevel

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  import scala.collection.JavaConversions._

  val sqlContext: SQLContext = spark.sqlContext
  import sqlContext.implicits._
  sc.setLogLevel("ERROR")
  sc.setCheckpointDir("/Users/shabbirhussain/Data/project/temp/")
  sc.setLocalProperty("spark.cleaner.referenceTracking.cleanCheckpoints", "true")

  sc.setLocalProperty("spark.checkpoint.compress", "true")
//  sc.setLocalProperty("spark.rdd.compress", "true")

  val customSchema = StructType(Array(
    StructField("REPO_OWNER",   StringType, nullable = false),
    StructField("REPOSITORY",   StringType, nullable = false),
    StructField("GIT_PATH",     StringType, nullable = false),
    StructField("HASH_CODE",    StringType, nullable = true),
    StructField("COMMIT_TIME",  LongType,   nullable = false)))

//  sc.textFile("/Users/shabbirhussain/Data/project/FILE_HASH_HISTORY/raw/*/").
//    distinct.
//    map(_.replaceAll(",null,", "")).
//    coalesce(1, false).
//    saveAsTextFile("/Users/shabbirhussain/Data/project/FILE_HASH_HISTORY/1522449220894/", classOf[BZip2Codec])



  val rawData = sqlContext.read.format("csv").
    option("delimiter",",").option("quote","\"").schema(customSchema).
//    load("s3://shabbirhussain/FILE_HASH_HISTORY/*/*/").
    load("/Users/shabbirhussain/Google Drive/NEU/Notes/CS 8678 Project/Déjà vu Episode II - Attack of The Clones/Data/FILE_HASH_HISTORY/First10K/*/").
    distinct

  val allData = rawData.
    filter($"REPO_OWNER".isNotNull && $"REPOSITORY".isNotNull && $"GIT_PATH".isNotNull &&$"COMMIT_TIME".isNotNull).
    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME".cast(sql.types.LongType)).
    withColumn("min(COMMIT_TIME)", min("COMMIT_TIME").over(Window.partitionBy("HASH_CODE"))).
    repartition($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").
    withColumn("HEAD_COMMIT_TIME", first("COMMIT_TIME").over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy($"COMMIT_TIME".desc))).
    withColumn("HEAD_HASH_CODE"  , first("HASH_CODE"  ).over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy($"COMMIT_TIME".desc))).
    persist(StorageLevel.MEMORY_AND_DISK_SER)

  // List all original content based on first commit.
  val orig = allData.
    filter($"COMMIT_TIME" === $"min(COMMIT_TIME)" && $"HASH_CODE".isNotNull).
    select(
      $"REPO_OWNER" .as("O_REPO_OWNER"),
      $"REPOSITORY" .as("O_REPOSITORY"),
      $"GIT_PATH"   .as("O_GIT_PATH"),
      $"COMMIT_TIME".as("O_COMMIT_TIME"),
      $"HASH_CODE",
      $"HEAD_COMMIT_TIME".as("O_HEAD_COMMIT_TIME"),
      $"HEAD_HASH_CODE"  .as("O_HEAD_HASH_CODE")
    )

  // List all the copied content (hash equal).
  val copy = allData.
    filter($"COMMIT_TIME" =!= $"min(COMMIT_TIME)" && $"HASH_CODE".isNotNull).drop($"min(COMMIT_TIME)").
    join(orig, usingColumn = "HASH_CODE").
    filter( // Prevent file revert getting detected as copy
      $"REPO_OWNER" =!= $"O_REPO_OWNER" ||
      $"REPOSITORY" =!= $"O_REPOSITORY" ||
      $"GIT_PATH"   =!= $"O_GIT_PATH"
    ).
    filter(!($"O_HEAD_HASH_CODE".isNull && $"O_HEAD_COMMIT_TIME" === $"COMMIT_TIME")). // Ignore immediate moves
    checkpoint(false).
    persist(StorageLevel.MEMORY_AND_DISK_SER)//checkpoint(true)

  // Count unique files in the head.
  val (allUniqPathsCount, origUniqPathsCount, copyUniqPathsCount) = {

    /* Special case when two files are together checkedin both unique. So both of them become original. Now one file is deleted and commited again.
     * This makes this path as copy coz file was deleted and now this path with higher timerstamp gets connected to other original.

    // allData.filter($"GIT_PATH" === "client/node_modules/azure/node_modules/xmlbuilder/lib/XMLBuilder.js").orderBy("COMMIT_TIME").show
    // orig.filter($"GIT_PATH" === "client/node_modules/azure/node_modules/xmlbuilder/lib/XMLBuilder.js").orderBy("COMMIT_TIME").show
    // copy.filter($"GIT_PATH" === "client/node_modules/azure/node_modules/xmlbuilder/lib/XMLBuilder.js").orderBy("COMMIT_TIME").take(1)
    */
	// all = 16324958, orig = 652394, copy = 15503999
    val allUniqPathsCount = allData.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").       // Head only
      filter($"HASH_CODE".isNotNull).                       // Paths which aren't deleted
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
    val origUniqPathsCount = orig.
      filter($"O_COMMIT_TIME" === $"O_HEAD_COMMIT_TIME").   // Head only
      select("O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH").distinct.count
    val copyUniqPathsCount = copy.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").       // Head only
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count

    (allUniqPathsCount, origUniqPathsCount, copyUniqPathsCount)
  }

  // Divergent Analysis = 59139
  val divergentCopyCount = copy.
    select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.
    intersect(orig.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").   // Head only
      filter($"HASH_CODE".isNotNull).
      select(
        $"O_REPO_OWNER".as("REPO_OWNER"),
        $"O_REPOSITORY".as("REPOSITORY"),
        $"O_GIT_PATH"  .as("GIT_PATH")
      )).count


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

  // Copy as Import
  val copyAsImportCount = {
    val extractLevel1Folder = udf[String, String]((s: String) => {
      val temp = s.split("/"); if (temp.length == 1) "" else temp(0);
    })

    val headCopy = copy.filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME") // Pick head paths only.

    // Copy path resembles original path
    val truePathCopy = headCopy.
      filter($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY"). // They should have a different repo to compare
      filter($"GIT_PATH".contains($"O_GIT_PATH")).
      withColumn("GIT_PATH_PREFIX", $"GIT_PATH".substr(lit(0), length($"GIT_PATH") - length($"O_GIT_PATH"))).
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY").
      agg(max($"O_COMMIT_TIME")).
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Cross join all paths fingerprint within a copied folder with all the orig repos form that contributed to this folder.
    val allCopyPathsXRepo = headCopy.
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "COMMIT_TIME", "HASH_CODE").
      withColumn("crc32(HASH_CODE)", crc32($"HASH_CODE")).drop("HASH_CODE").
      distinct. // Using distinct as multiple originals can be present as 2 copies committed simultaneously
      join(truePathCopy, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "COMMIT_TIME")).
      filter($"GIT_PATH".startsWith($"GIT_PATH_PREFIX")).
      withColumn("O_GIT_PATH", $"GIT_PATH".substr(length($"GIT_PATH_PREFIX") + 1, lit(100000))).drop("GIT_PATH").
      withColumn("crc32(O_GIT_PATH)", crc32($"O_GIT_PATH")).
      // Extract first level folder to aggregate repository.
      withColumn("O_FOLDER", extractLevel1Folder($"O_GIT_PATH")).
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY", "max(O_COMMIT_TIME)", "O_FOLDER").
      agg(sum("crc32(HASH_CODE)"), sum("crc32(O_GIT_PATH)"), count("O_GIT_PATH")).
      checkpoint(true)

    // Make bloom filter to single out only records which belong to an original repo from which someone copied something.
    val bf = DataFrameUtils.makeBloomFilter(truePathCopy, Seq($"O_REPO_OWNER", $"O_REPOSITORY"), 0.0001)


    /* Turns out datatype conversion is worse than a join
    val allCopyPathsXRepo = headCopy.
      withColumn("crc32(HASH_CODE)" , crc32($"HASH_CODE")).
      // Avoid self joining by localized group self join.
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME").
      agg(collect_list(struct($"GIT_PATH", $"O_REPO_OWNER", $"O_REPOSITORY", $"O_GIT_PATH", $"O_COMMIT_TIME", $"crc32(HASH_CODE)"))).
      rdd.flatMap(y=> {
        val repoOwner  = y.getAs[String]("REPO_OWNER")
        val repository = y.getAs[String]("REPOSITORY")
        val commitTime = y.getAs[Long]("COMMIT_TIME")

        val lst = y.getSeq[Row](3)
        val candidateOrig = lst.
          filter(r=> (r.getAs[String]("O_REPO_OWNER") != repoOwner) || (r.getAs[String]("O_REPOSITORY") != repository)).
          filter(r=> r.getAs[String]("GIT_PATH").endsWith(r.getAs[String]("O_GIT_PATH"))).
          map(r=> {
            val gitPath   = r.getAs[String]("GIT_PATH")
            val oGitPath  = r.getAs[String]("O_GIT_PATH")
            val gitPathPrefix = gitPath.substring(0, gitPath.length - oGitPath.length)
            (gitPathPrefix, r.getAs[String]("O_REPO_OWNER"), r.getAs[String]("O_REPOSITORY"), r.getAs[Long]("O_COMMIT_TIME"))
          }).
          groupBy(x=> (x._1, x._2, x._3)).mapValues(_.maxBy(_._4)).values

        lst.flatMap(r=> {
          val gitPath = r.getAs[String]("GIT_PATH")
          val crc32HashCode = r.getAs[Long]("crc32(HASH_CODE)")
          candidateOrig.
            filter(o=> gitPath.startsWith(o._1)).
            map(o=> {
            (repoOwner, repository, gitPath, commitTime, o._1, o._2, o._3, o._4, crc32HashCode)
          })
        })
      }).
      toDF("REPO_OWNER", "REPOSITORY", "GIT_PATH", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY", "max(O_COMMIT_TIME)", "crc32(HASH_CODE)").
      withColumn("O_GIT_PATH", $"GIT_PATH".substr(length($"GIT_PATH_PREFIX") + 1, lit(100000))).
      withColumn("crc32(O_GIT_PATH)", crc32($"O_GIT_PATH")).
      // Extract first level folder to aggregate repository.
      withColumn("O_FOLDER", extractLevel1Folder($"O_GIT_PATH")).
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY", "max(O_COMMIT_TIME)", "O_FOLDER").
      agg(sum("crc32(HASH_CODE)"), sum("crc32(O_GIT_PATH)"), count("O_GIT_PATH")).
      persist(StorageLevel.MEMORY_AND_DISK_SER)


    // Make bloom filter to single out only records which belong to an original repo from which someone copied something.
    val bf = makeBloomFilter(allCopyPathsXRepo, Seq($"O_REPO_OWNER", $"O_REPOSITORY"), 0.0001)
    */


    // Get all the paths fingerprint present in the all repo upto a max(original commit point) used by the copier.
    @transient val w1 = Window.partitionBy("REPO_OWNER", "REPOSITORY", "FOLDER").orderBy($"COMMIT_TIME")
    val allJSFilesAtTimeOfCopyFromSrc = allData.
      // Filter extra data with bloom filters
      filter(x=> bf(0).contains(x.getString(0)) && bf(1).contains(x.getString(1))).
      // Extract first level folder to aggregate repository.
      withColumn("FOLDER", extractLevel1Folder($"GIT_PATH")).
      // Create checksums of required columns.
      withColumn("crc32(GIT_PATH)" , crc32($"GIT_PATH")).
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
      withColumn("crc32(GIT_PATH)",
        when($"IS_ADDITION",  $"crc32(GIT_PATH)").
        when($"IS_DELETION", -$"crc32(GIT_PATH)").
        otherwise(0)).
      withColumn("crc32(HASH_CODE)", $"crc32(HASH_CODE)" - $"crc32(PREV_HASH_CODE)").
      // Fingerprint all additions in a commit for every first level folder.
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER").
      agg(sum($"crc32(GIT_PATH)"), sum($"crc32(HASH_CODE)"), sum(signum($"crc32(GIT_PATH)"))).
      // Do cumulative sum of fingerprints.
      select(
        $"REPO_OWNER", $"REPOSITORY", $"COMMIT_TIME", $"FOLDER",
        sum($"sum(crc32(GIT_PATH))")        .over(w1).as("sum(crc32(GIT_PATH))"),
        sum($"sum(crc32(HASH_CODE))")       .over(w1).as("sum(crc32(HASH_CODE))"),
        sum($"sum(signum(crc32(GIT_PATH)))").over(w1).as("sum(count(GIT_PATH))")
      )


    // Do join to find if all the paths from original at the time of copy exists in the copied folder.
    val copyAsImportExamples = allCopyPathsXRepo.
      join(allJSFilesAtTimeOfCopyFromSrc.
        select(
          $"REPO_OWNER".as("O_REPO_OWNER"),
          $"REPOSITORY".as("O_REPOSITORY"),
          $"COMMIT_TIME".as("O_COMMIT_TIME"),
          $"FOLDER".as("O_FOLDER"),
          $"sum(crc32(HASH_CODE))",
          $"sum(crc32(GIT_PATH))" .as("sum(crc32(O_GIT_PATH))"),
          $"sum(count(GIT_PATH))" .as("count(O_GIT_PATH)")
        ),
        usingColumns = Seq("O_REPO_OWNER", "O_REPOSITORY", "O_FOLDER", "sum(crc32(O_GIT_PATH))", "sum(crc32(HASH_CODE))", "count(O_GIT_PATH)")).
      filter($"max(O_COMMIT_TIME)" <= $"O_COMMIT_TIME").
      select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY", "count(O_GIT_PATH)").distinct.
      checkpoint(true)


	  // res = 3380348
    // TODO: What should we consider that path if it belongs to copy as import and then changed to something else?
    val copyAsImportCount = copyAsImportExamples.agg(sum("count(O_GIT_PATH)")).collect


    val unidentifiedNodeCopiesCount = copy.filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME"). // Pick head paths only.
      filter(length($"GIT_PATH") > length($"O_GIT_PATH")).
      filter($"GIT_PATH".contains("node_modules/")).
      filter($"GIT_PATH".contains($"O_GIT_PATH")).
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.
      join(copyAsImportExamples.select("REPO_OWNER", "REPOSITORY").distinct.withColumn("JNK", lit(true)),
        usingColumns = Seq("REPO_OWNER", "REPOSITORY"),
        joinType = "LEFT_OUTER").
      filter($"JNK".isNull).count
      
    copy.filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME"). // Pick head paths only.
      filter($"GIT_PATH".contains("node_modules/")).
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count

    copyAsImportCount
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
