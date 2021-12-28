package com.lisz.bigdata2.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Lesson01_sql_basic {
  def main(args: Array[String]): Unit = {
    // sql字符串 -> dataset -> 对RDD的包装（优化器）-> 只有RDD才能触发DAGScheduler
    // 变化：
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf) //.appName("test").master("local")
      //.enableHiveSupport() // 只有开启这个选项的时候spark sql on hive才支持DDL，没开启，spark只有catalog，而没有metastorage.catalog是对接的位置
      .getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    val databases = session.catalog.listDatabases()
    databases.show
    val tables = session.catalog.listTables()
    tables.show
    val functions = session.catalog.listFunctions
    functions.show(999, true)

    println("-------------------------")

    // DataFrame: DataSet[Row]
    val df: DataFrame = session.read.json("data/json")
    df.show()         // 相当于 select * from json;
    df.printSchema()  // 相当于 desc json;
    df.createTempView("aaa")  // 这一过程是df通过session向catalog中注册表名，为后面的SQL使用

    val frame = session.sql("select name from aaa")
    frame.show

    println("-------------------------")

    session.catalog.listTables.show

    import scala.io.StdIn._
    while (true){
//      val sql = readLine("input your sql: ")
      val sql = readLine("spark-sql>")  // 想上来就能执行SQL，必须先在上面准备好元数据
      session.sql(sql).show
    }
  }
}
