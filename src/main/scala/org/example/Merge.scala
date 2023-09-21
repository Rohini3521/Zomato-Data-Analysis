package org.example

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object Merge {
  def mergeFiles(srcPath: String
                 , tgtPath: String
                 , deleteSourcePostMerge: Boolean
                 , deleteDestinationIfExist: Boolean,
                 spark: SparkSession): Unit = {
    val log = Logger.getLogger("org")

    val fsConf = spark.sparkContext.hadoopConfiguration

/*
    log.info("Merging multiple files to single file.")
    log.info("Source location: " + srcPath)
    log.info("Target location: " + tgtPath)
*/

    val fs = FileSystem.get(fsConf)
    val inputPath = new Path(srcPath)
    val targetPath = new Path(tgtPath)

    // Check if file already present
    val isFileAlreadyPresent = fs.exists(targetPath)
    log.info("File already present at target location: " + isFileAlreadyPresent.toString)

    // If file is already present, delete the file or raise exception as per flag value.
    if (isFileAlreadyPresent) {
      if (deleteDestinationIfExist) {
        log.info("Deleting already existing file at target location.")
        fs.delete(targetPath, false)
        log.info("File deleted successfully at target location.")
      }
      else {
        throw new RuntimeException("Destination file already present: " + targetPath)
      }
    }
    // Merge files present in source folder to single file at target location.
    FileUtil.copyMerge(fs, inputPath, fs, targetPath, deleteSourcePostMerge, fsConf, "")
    log.info("Files merged successfully to " + targetPath)
  }


}
