package com.lisz.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sql01_basic {
  def main(args: Array[String]): Unit = {
    // sql 字符串 -> dataset 堆RDD的一个包装（优化器） -> 只有RDD才能触发DAGScheduler
    // Spark SQL里备有SparkContext，所以用SparkSession
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession
      .builder()
      .config(conf)
      //.enableHiveSupport() // 开启这个选项的时候spark sql on 才支持DDL，没开启，spark只有catalog
      .getOrCreate()

    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    val databases = session.catalog.listDatabases()
    databases.show(false)
    val tables = session.catalog.listTables()
    tables.show()
    val functions = session.catalog.listFunctions()
    functions.show(false)

    println("----------------------")

    // 以session为主的操作 DataFrame 实际上就是一个DataSet[Row]类型
    val df = session.read.json("./data/json")
    df.show()
    df.printSchema()

    df.createTempView("aaa") //通过session向catalog注册表名，df有数据
//    val frame = session.sql("select name from aaa")
//    frame.show()
//
//    println("---------------------")
//
//    val tables2 = session.catalog.listTables()
//    tables2.show(false)

    import scala.io.StdIn._
    while (true) {
      val sql = readLine("Input your SQL: ")
      session.sql(sql).show()
    }
  }

}
