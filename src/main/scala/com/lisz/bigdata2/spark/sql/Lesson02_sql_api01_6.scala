package com.lisz.bigdata2.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object Lesson02_sql_api01_6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._
    val dataFrame = List(
      "hello world",
      "hello world",
      "hello lisz",
      "hello world",
      "hello world",
      "hello spark",
      "hello world",
      "hello spark"
    ).toDF("line")

    dataFrame.createTempView("aaa")
    val df = session.sql("select * from aaa")
    df.show
    df.printSchema

    println("-----------------------")
    // explode每元素（单词）放到一行. 单词统计倒序排列结果
    session.sql("select word, count(*) ct from (select explode(split(line, ' ')) as word from aaa) as tt group by word order by ct desc").show
    println("-----------------------")
    // 面向API的时候，DF低昂挡雨table
    /*
    以上两种方式，哪个更快？
    为什么时第二种? 语法解析
    */
    val res = dataFrame.selectExpr("explode(split(line, ' ')) as word").groupBy("word").count().orderBy("count")
    res.show
    res.write.mode(SaveMode.Append).parquet("data/aaa")
    println("-----------------------")

    val frame = session.read.parquet("data/aaa")
    frame.show()
    frame.printSchema()

    /*
    基于文件
    session.read.parquet()
    session.read.textFile()
    session.read.json()
    session.read.csv()
      Transform to DF
    res.write.parquet()
    res.write.orc()
    res.write.text()
    */

    // 直接从DB里拉过来
  }
}
