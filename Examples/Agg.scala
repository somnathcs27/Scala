package com.som

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, lit}

object Example_Agg {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Diff_Datatype_array").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val columnNames = List("A", "B", "C")

    def get_Data(cnt: String): DataFrame = {
      val data = Seq(("A1".concat(cnt), "B1".concat(cnt), "C1".concat(cnt))
        , ("A2".concat(cnt), "B2".concat(cnt), "C2".concat(cnt))
      )
      spark.createDataFrame(data).toDF(columnNames: _*)
    }
    def get_col_count(inputDF: DataFrame, col_name: String, suffix: String): String = {
      inputDF.select(col_name.concat(suffix)).collect()(0)(0).toString
    }

    def get_distinct_count(tbl_name: String, col_list: List[String]): DataFrame = {
      import spark.implicits._

      var df1 = get_Data("1")
      var df2 = get_Data("2")
//      var df3 = get_Data("3")
//      df2 = df2.union(df3)
      println("df1")
      df1.show()
      println("df2")
      df2.show()
      val outputDF = spark.createDataFrame(col_list.map(Tuple1(_))).toDF("columnName")
      val countDistinctFunc = columnNames.map(colName => countDistinct(colName).as(s"${colName}_count"))
      val df1_count = df1.agg(countDistinctFunc.head, countDistinctFunc.tail:_*)
      val df2_count = df2.agg(countDistinctFunc.head, countDistinctFunc.tail:_*)
      val final_outputDF = outputDF.withColumn("Count_in_DF1", lit(get_col_count(df1_count, outputDF.select($"columnName").map(_.getString(0)).collect.head, "_count")))
        .withColumn("Count_in_DF2", lit(get_col_count(df2_count, outputDF.select($"columnName").map(_.getString(0)).collect.head, "_count")))

      println("final_outputDF")
      final_outputDF
    }

    get_distinct_count("table_name", columnNames).show()

  }
}
