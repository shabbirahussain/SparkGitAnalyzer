package org.reactorlabs.jshealth

import java.io.File

import org.apache.spark.sql.DataFrame
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.models.Schemas

/**
  * @author shabbirahussain
  */
package object util {
  def deleteRecursively(file: File)
  : Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete){
      //throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

  def recursiveListFiles(f: File)
  : Array[File] = {
    val these = f.listFiles
      .filter(!_.isHidden)

    these
      .filter(_.isFile)
      .++(these
        .filter(_.isDirectory)
        .flatMap(recursiveListFiles)
      )
  }

  def read(path: String, split: Schemas.Value): DataFrame = {
    val fullPath = "%s/data/*/%s/".format(path, split)
    println(fullPath)
    val meta = Schemas.asMap(key = split)
    sqlContext.read
      .schema(meta._1)
      .load(fullPath)
      .dropDuplicates(meta._2)
  }
}
