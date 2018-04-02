package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.Paths

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
}
