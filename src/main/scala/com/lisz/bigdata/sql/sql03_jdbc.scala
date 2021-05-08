package com.lisz.bigdata.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object sql03_jdbc {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local")
      .config("spark.sql.shuffle.partitions", "1") // 默认会有200并行度的参数，并行度有人来决定
      .getOrCreate()
    val sc = session.sparkContext
    // sc.setLogLevel("ERROR")

    val properties = new Properties()
    properties.put("url", "jdbc:mysql://192.168.1.6/spark")
    properties.put("user", "root")
    properties.put("password", "^abc123$")
    properties.put("driver", "com.mysql.cj.jdbc.Driver")

    // 没有hive的情况下 无论什么数据源拿到的都是DF或DS
//    val jdbcDF = session.read.jdbc(properties.get("url").toString, "person", properties)
//    jdbcDF.createTempView("person")
//    session.sql("select * from person").show()
//
//    // 一般都是先落成文件，一文件的形式传输，save/load，主要是可靠稳定
//    jdbcDF.write.jdbc(properties.get("url").toString, "xxxx", properties)

    val usersDF = session.read.jdbc(properties.get("url") toString, "users", properties)
    val scoreDF = session.read.jdbc(properties.get("url") toString, "score", properties)
    usersDF.createTempView("users")
    scoreDF.createTempView("score")
    val resDF = session.sql("select users.id, users.name, users.age, score.score from users join score on users.id = score.id")
    resDF.show()
    resDF.printSchema()

    println(resDF.rdd.getNumPartitions)
    //val resDF01 = resDF.coalesce(1)
    //println(resDF01.rdd.getNumPartitions)
    resDF.write.jdbc(properties.get("url").toString, "aaaa", properties )
  }

}
