package com.lisz.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}

import scala.beans.BeanProperty

class Person extends Serializable {
  @BeanProperty
  var name :String = ""
  @BeanProperty
  var age :Int = 0
}

object sql02_api01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    // 数据 + 元数据 == Dataframe == 一张表
    // 1 数据： RDD[Row]
    // 2 元数据：StructType

    // 1.数据
//    val rdd = sc.textFile("./data/person.txt")
//    第一种方式
//    val rddRow = rdd.map(line => {
//      val strs = line.split("\\s+")
//      (strs(0), strs(1).toInt)
//    }).map(x => Row.apply(Array(x._1, x._2)))
//    val rddRow = rdd.map(_.split("\\s+")).map(arr => Row.apply(arr(0), arr(1).toInt))
//
//    // 元数据, 跟rowRdd和数据顺序一致
//    val fields = Array(
//      StructField.apply("name", DataTypes.StringType, true),
//      StructField.apply("age", DataTypes.IntegerType, true)
//    )
//    val schema = StructType.apply(fields)
//    val dataFrame = session.createDataFrame(rddRow, schema)
//
//    dataFrame.show()
//    dataFrame.printSchema()
//    dataFrame.createTempView("person")
//    session.sql("select * from person").show()

//    val userSchema = Array(
//      "name string",
//      "age int",
//      "gender int"
//    )
//
//    def toDataType(vv:(String, Int)) ={
//      userSchema(vv._2).split("\\s+")(1) match {
//        case "string" => vv._1.toString
//        case "int" => vv._1.toInt
//      }
//    }
//
//    val rowRdd = rdd.map(_.split("\\s+"))
//      // [(zhangsan, 0), (18, 1), (0, 2)]
//      .map(x => x.zipWithIndex)
//      .map(x => x.map(toDataType(_)))
//      .map(x => Row.fromSeq(x)) // Row 代表很多列，每个列要标识出准确类型
//
//    def getDataType(v: String) = {
//      v match {
//        case "string" => DataTypes.StringType
//        case "int" => DataTypes.IntegerType
//      }
//    }
//
//    val fields = userSchema.map(_.split("\\s+")).map(x=>StructField.apply(x(0), getDataType(x(1))))
//    val schema = StructType.apply(fields)
//
//    val schema2 = StructType.fromDDL("name string, age int, sex int")
//    val df = session.createDataFrame(rowRdd, schema2)
//    df.show()
//    df.printSchema()





//    val p = new Person()
//    // MR、Spark都是pipeline，iter，一次内存飞过一条数据，这一条记录完成了读取、计算、序列化
//    // 分布式计算，计算逻辑有Driver 序列化，发送给其他JVM的Executor中执行，所以要让Person继承序列化接口
//    val rddBean = rdd.map(_.split("\\s+")).map(arr => {
//      p.setName(arr(0))
//      p.setAge(arr(1).toInt)
//      p
//    })
//    val df = session.createDataFrame(rddBean, classOf[Person])
//    df.show()
//    df.printSchema()






    // 最后这种方法最常用！！但所有这些在工作中都不怎么用，但是有助于理解Spark SQL
    // Dataset是RDD的扩展，RDD面只向collection，Dataset除了面向collection还能面向SQL等领域语言，DataFrame是他的一个子集
    //Spark 的Dataset  既可以按collection，类似于rdd的方法操作，也可以按SQL领域语言定义的方式操作数据
    // Dataset比RDD强的地方是有编码器，对数据有识别、序列化反序列化等优化，序列化之后的数据能少占用内存，且可以触发钨丝计划，使用堆外内存


    /**
     * 纯文本文件，不带自描述，string  不被待见的
     * 必须转结构化。。再参与计算
     * 转换的过程可以由spark完成
     * hive数仓（重要：集群大小、磁盘空间和block块大小都依托于数据特征）
     * 接数：源数据   不删除，不破坏
     * ETL  中间态
     * 所有的计算发生再中间态
     * 中间态：--》一切以后续计算成本为考量：json、parquet..
     * 文件格式类型？
     * 分区/分桶 为了后续计算，要分析后续计算特征。分区表可以让计算起步的时候加载的数据量变少；分桶可以让计算过程中
     * shuffle移动的数据量变少
     */

    //import session.implicits._
    val ds = session.read.textFile("./data/person.txt")
    val person = ds.map(line => {
      val strs = line.split("\\s+")
      (strs(0), strs(1).toInt)
    })(Encoders.tuple(Encoders.STRING, Encoders.scalaInt))
    val dataFrame = person.toDF("name", "age")
    dataFrame.show()
    dataFrame.printSchema()
  }


}
