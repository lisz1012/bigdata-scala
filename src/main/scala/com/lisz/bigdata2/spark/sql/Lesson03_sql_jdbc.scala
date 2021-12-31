package com.lisz.bigdata2.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Lesson03_sql_jdbc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("INFO")

    import session.implicits._

    val prop = new Properties()
    prop.put("url", "jdbc:mysql://192.168.1.25/spark")
    prop.put("user", "root")
    prop.put("password", "P@ssw0rd")
    prop.put("driver", "com.mysql.cj.jdbc.Driver")
    // 无论什么数据源，拿到的都是DS或者DF
    val jdbcDF = session.read.jdbc(prop.get("url").toString, "student", prop)
    jdbcDF.createTempView("aaa")
    session.sql("select name from aaa").show()
    // 把从JDBC读到的数据再写回MySQL，指定一个新的表名："aaa"
    jdbcDF.write.jdbc(prop.get("url").toString, "aaa", prop)
  }
}
