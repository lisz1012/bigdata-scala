package com.lisz.bigdata2.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.beans.BeanProperty

class Person extends Serializable {
  @BeanProperty
  var name: String = ""
  @BeanProperty
  var age: Int = 0
}

object Lesson02_sql_api01_4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    // Dataframe = 数据+ 元数据 = 一张表！
    // 第三个版本：动态封装 （也已经淘汰了）
    // 1. MR spark pipeline iter 一次内存飞过一条数据 -> 这一条记录完成读取/计算/序列化，所以new在里面和外面就没有区别了
    // 2。 分布式计算，计算逻辑由Driver序列化，发送给jvm的executor中执行，类比MR中的IntWritable、LongWritable
    val person = new Person()
    val rdd = sc.textFile("data/person.txt")
    // 第三个版本：动态封装 （也已经淘汰了）
    val rddBean = rdd.map(_.split(" ")).map(x => {

      person.name = x(0)
      person.age = x(1).toInt
      person
    })

    val df = session.createDataFrame(rddBean, classOf[Person])
    df.show
    df.printSchema
  }
}
