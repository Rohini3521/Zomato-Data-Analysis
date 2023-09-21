package org.example


import org.example.Merge.mergeFiles
import org.apache.spark.sql.{DataFrame, SparkSession}


object Save {
  def saveFile(df: DataFrame, tgtPath: String, insertMode: String,spark:SparkSession): Unit = {

    // data in minIo current folder for druid load dim_data_load
    val tempPath = tgtPath + "_temp"
    //log.info("Saving data to HDFS at: " + tempPath)

    // Save file.
    df.write
      .mode(insertMode)
      .json(tempPath)

    // Merging multiple part files to single file.
    mergeFiles(tempPath, tgtPath, deleteSourcePostMerge = true,deleteDestinationIfExist = true,spark)
  }


}
