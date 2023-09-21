package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.Schema.getSchema

object Input {
  def getInputData(path:String,spark:SparkSession) : DataFrame = {
    val df = spark.read
      .option("header", "true")
      .format("csv")
      .schema(getSchema)
      .load(path)
      .withColumnRenamed("approx_cost(for two people)","approx_cost_for_two_people")
    df.show()
    df

  }

}
