package com.lisz.bigdata2.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Lesson03_sql_jdbc2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().appName("test").master("local").config("spark.sql.shuffle.partitions", "1").getOrCreate() // 默认shuffle的分区数是200，不够就用200，最小的并行度，一般都会去调这个值。跟有没有JDBC无关
    val sc = session.sparkContext
    sc.setLogLevel("INFO")

    val prop = new Properties()
    prop.put("url", "jdbc:mysql://192.168.1.25/spark")
    prop.put("user", "root")
    prop.put("password", "P@ssw0rd")
    prop.put("driver", "com.mysql.cj.jdbc.Driver")

    val usersDF = session.read.jdbc(prop.get("url").toString, "users", prop)
    val scoreDF = session.read.jdbc(prop.get("url").toString, "score", prop)

    usersDF.createTempView("users")
    scoreDF.createTempView("score")

    // 左表id和右表id为key，value就是各自id对应的数据，然后相同的id依照相同的规则去到同一个计算节点
    val resDF = session.sql("select users.id, users.name, users.age, score.score from users join score on users.id = score.id")
    resDF.show
    resDF.printSchema()

//    println("Partitions: " + resDF.rdd.partitions.length)
//    // 没有这一句，则会有好多（200）分区
//    val resDF01 = resDF.coalesce(1)
//    println("Partitions: " + resDF01.rdd.partitions.length)

    resDF.write.jdbc(prop.get("url").toString, "stu_score_2", prop) // 写入MySQL中的新表：stu_score_2

  }
}
