package com.lisz.bigdata.sql

import org.apache.spark.sql.SparkSession
// 大数据里面最值钱的是hive及其各种查询
object sql04_standalone_hive {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("asdas")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.warehouse.dir", "/Users/shuzheng/Documents/spark-warehouse")
      .enableHiveSupport() // 开启hive支持, 自己会启动hive的metastore
      .getOrCreate()
    val sc = session.sparkContext
    //sc.setLogLevel("ERROR")


    /**
     * 一定要记住有数据库的概念~！！！！！！！！！！！！！！！！！！！！
     * use default
     * mysql  是个软件
     * 一个mysql可以创建很多的库 database 隔离的
     * so，公司只装一个mysql，不同的项目组，自己用自己的库  database
     *
     * spark/hive 一样的, 都有库的概念
     *
     *
     */

    //session.sql("create database lisz")
    //session.sql("create table lisz.aaaa (name string, age int)")
    //session.sql("create table aaaa (name string, age int)")
    //session.sql("insert into lisz.aaaa values ('zhangsan', 18), ('lisi', 22)")
    //session.sql("insert into aaaa values ('zhangsan', 18), ('lisi', 22)")
    //session.sql("create table xxxx (name string, age int)")
    //session.sql("insert into xxxx values ('zhangsan', 18), ('lisi', 22)")
    session.catalog.listTables().show()
    //session.sql("select * from aaaa").show()

    //session.sql("create database lisz")
    session.sql("use lisz ")
    session.catalog.listTables().show()
    session.sql("create table table02(name string)")

    session.catalog.listTables().show()
  }

}
