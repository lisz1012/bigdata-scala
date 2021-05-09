package com.lisz.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sql_05_on_hive {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local")
      .config("hive.metastore.uris", "thrift://hadoop-03:9083") // 跟hiveserver2的配置是一样的
      .enableHiveSupport()
      .getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._

    val df01 = List(
      "zhangsan",
      "lisi"
    ).toDF("name")
    df01.createTempView("ooxx")

    //session.sql("create table xxoo (id int)")
    session.sql("insert into xxoo values (3), (6), (7)")
    df01.write.saveAsTable("oxox")

    session.catalog.listTables().show()
    session.sql("show tables").show()
    val dataFrame = session.sql("select * from psn")
    dataFrame.show()
    dataFrame.printSchema()
  }

}
