package org.example

import org.apache.log4j.{Level, Logger}
import org.example.Input.getInputData
import org.example.Process.Processing
import org.example.Session.getSparkSession

object MainMethod {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = getSparkSession

    val input_path = "D:/zomatorestaurants.csv"

    val raw_df = getInputData(input_path,spark)
    Processing(raw_df,spark)




  }

}
