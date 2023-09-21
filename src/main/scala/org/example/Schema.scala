package org.example

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Schema {
  def getSchema: StructType = {
    val schema = StructType(Array(
      StructField("id",IntegerType,nullable=true),
      StructField("address",StringType,nullable=true),
      StructField("name",StringType,nullable=true),
      StructField("online_order",StringType,nullable=true),
      StructField("book_table",StringType,nullable=true),
      StructField("rate",StringType,nullable=true),
      StructField("votes",IntegerType,nullable=true),
      StructField("rest_type",StringType,nullable=true),
      StructField("dish_liked",StringType,nullable=true),
      StructField("cuisines",StringType,nullable=true),
      StructField("approx_cost(for two people)",IntegerType,nullable=true),
      StructField("city",StringType,nullable=true)

    ))
    schema

  }

}
