package com.lisz.bigdata2.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Lesson04_sql_standalone_hive {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().appName("test").master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.warehouse.dir", "/Users/shuzheng/IdeaProjects/bigdata-scala/warehouse")
      .enableHiveSupport.getOrCreate() // 这次开启Hive支持 自己会启动Hive的metastore
    val sc = session.sparkContext
//    sc.setLogLevel("ERROR")
    import session.sql
    /**
     * 一定要有数据库的概念！！
     * 上来就默认 use default
     * mysql  是个软件
     * 一个mysql可以创建很多的库 database 隔离的
     * so，公司只装一个mysql，不同的项目组，自己用自己的库  database
     *
     * spark/hive 一样的, 都有库的概念
     */
    //    session.sql("create table xxx (name string, age int)")
//    session.sql("insert into xxx values ('zhangsan', 18), ('lisi', 22)")


//    sql("create database lisz3")
//    sql("create table xxxx (name string, age int)")
//    sql("create table xxxx (name string, age int)")
//    sql("insert into xxxx values ('zhangsan', 18), ('lisi', 22)")

    session.catalog.listTables().show()
//    sql("select * from xxxx").show
//    sql("create database lisz1")
//    sql("create table t01(name string)") // 作用在current库
    sql("insert into t01 values ('zhangsan'), ('lisi')")
    sql("select * from t01").show
    session.catalog.listTables().show() // 作用在current库
    println("------------------------------------")
    sql("create database lisz1")
    sql("use lisz1")
    sql("create table t01(name string)")
//    sql("drop table t01")
    sql("select * from t01").show
    session.catalog.listTables().show() // 作用在lisz库
  }
}
