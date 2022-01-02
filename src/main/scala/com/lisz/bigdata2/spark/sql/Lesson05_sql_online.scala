package com.lisz.bigdata2.spark.sql

import org.apache.spark.sql.SparkSession

object Lesson05_sql_online {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test on hive").master("local")
      .config("hive.metastore.uris", "thrift://hadoop-01:9083") // 传说中的：SPARK ON HIVE！！！！！metastore在哪儿要告诉Spark
      .enableHiveSupport()
      .getOrCreate()

    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._
    val df01 = List(
      "zhangsan",
      "lisi"
    ).toDF("name")
    df01.createTempView("aaa")  // 将创建临时表，只在当前application的内存里能看见

    //session.sql("create table bbb (id int)") // 能写入hive的MySQL，其中会多一张新的表bbb的元数据
    session.sql("insert into bbb values (3), (6), (7)") // 数据能放进去
    df01.write.saveAsTable("aaa") // 能存成一张表

    session.catalog.listTables.show  // 数据可以背list出来，从metastore中
    // 如果没有hive，表最开始一定是Dataset/DataFrame过来的
//    session.sql("use default")
    session.sql("show tables").show  // 也可以显示，metastore里面的表
  }
}
