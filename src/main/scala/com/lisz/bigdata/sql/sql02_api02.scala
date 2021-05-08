package com.lisz.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object sql02_api02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._
    val dataDF = List(
      "hello world",
      "hello lisz",
      "hello spark",
      "hello lisz",
      "hello world",
      "hello spark",
      "hello world",
      "hello world"
    ).toDF("line")

    dataDF.createTempView("aaa")
    val frame = session.sql("select * from aaa")
    frame.show()
    frame.printSchema()

    println("------------------------")

    session.sql("select explode(split(line, ' ')) as word from aaa").show()
    println("------------------------")
    session.sql("select word, count(*) as ct from (select explode(split(line, ' ')) as word from aaa) group by word sort by ct desc").show()

    println("------------------------")
    // 面向API的时候DF相当于from table
    val subTab = dataDF.selectExpr("explode(split(line, ' ')) as word")
    val dataset = subTab.groupBy("word")
    val res = dataset.count().sort("count")
    res.show()
    /*
    以上两种方式，哪个更快？
    为什么时第二种? 语法解析
         */
    println("------------------------")

    res.write.mode(SaveMode.Append).parquet("./data/out/wc")

    println("------------------------")

    val frame1 = session.read.parquet("./data/out/wc")
    frame1.show()
    frame1.printSchema()
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
