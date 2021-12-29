package com.lisz.bigdata2.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Lesson02_sql_api01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    // Dataframe = 数据+ 元数据 = 一张表！

    val rdd = sc.textFile("data/person.txt")
    val rddRow = rdd.map(_.split("\\s+")).map(arr => Row.apply(arr(0), arr(1).toInt))

    val fields = Array(
      StructField.apply("name", DataTypes.StringType, true),
      StructField.apply("age", DataTypes.IntegerType, true))
    val schema: StructType = StructType.apply(fields)

    val dataFrame = session.createDataFrame(rddRow, schema)
    dataFrame.show
    dataFrame.printSchema
    dataFrame.createTempView("aaa")
    session.sql("select * from aaa").show
    // 👇没有 Hive metastore支持，则下面这句无法执行成功
    //session.sql("create table abc (name string, age int)")
  }
}
