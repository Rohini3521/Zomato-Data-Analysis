package org.example
import org.example.Save.saveFile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Process {
  def Processing(raw_df: DataFrame, spark: SparkSession): Unit = {
    val ls_expected_columns = List("id", "address", "name", "online_order", "book_table"
      , "rate", "votes", "rest_type", "dish_liked", "cuisines", "approx_cost_for_two_people", "city")
    val mapExpectedColumns = ls_expected_columns.map(x => col(x))
    var required_cols = raw_df.select(mapExpectedColumns: _*)


    val select_df = raw_df.select(mapExpectedColumns: _*)
    select_df.repartition(2, col("city"))
      .write
      .mode("overwrite")
      .bucketBy(2, "city")
      .sortBy("city")
      .option("path", "D:/table" + "_restaurants")
      .saveAsTable("tbl_bucket_restaurants")

    val query_df = spark
      .sql(
        """select name,address,approx_cost_for_two_people from tbl_bucket_restaurants
          |where address like '%Banashankari%' AND rate like '%4.%'
          |""".stripMargin)

    val tgtPath = "D:/zomOut.json"

    saveFile(query_df, tgtPath, "overwrite",spark)


  }

}
