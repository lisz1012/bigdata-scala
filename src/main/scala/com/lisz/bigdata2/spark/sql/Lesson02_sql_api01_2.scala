package com.lisz.bigdata2.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Lesson02_sql_api01_2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    // Dataframe = 数据+ 元数据 = 一张表！

    val rdd = sc.textFile("data/person.txt")
    // 第一个版本：动态封装 （已经淘汰了）
    val userSchema = Array(
      "name string",
      "age int"
      //"sex int"
    )

    // 1. row RDD
    def toDataType(vv:(String, Int))={
      userSchema(vv._2).split(" ")(1) match {
        case "string" => vv._1.toString
        case "int" => vv._1.toInt
      }
    }

    def getDataType(vv:String)={
      vv match {
        case "string" => DataTypes.StringType
        case "int" => DataTypes.IntegerType
      }
    }

    val rowRDD = rdd.map(_.split(" ")) // 每一条是一个数组：[zhangsan，18， 0]
      .map(_.zipWithIndex) // [(zhangsan, 0), (18, 1), (0, 2)]
      .map(x => x.map(toDataType(_))) // 各列的真是数据的类型要对得上
      .map(x => Row.fromSeq(x)) // Row代表很多的列，每个列要标识出准确的类型

    // 2. struct type
    val fields = userSchema.map(_.split(" ")).map(x => {
      StructField.apply(x(0), getDataType(x(1)), true)
    })
    val schema = StructType.apply(fields)
    val dataFrame = session.createDataFrame(rowRDD, schema)
    dataFrame.show
    dataFrame.printSchema
  }
}
