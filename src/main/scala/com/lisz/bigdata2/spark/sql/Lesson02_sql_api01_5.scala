package com.lisz.bigdata2.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SparkSession}

import scala.beans.BeanProperty

object Lesson02_sql_api01_5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")
    // Dataset是RDD的一个扩展，RDD只是面向collection，而Dataset既可以面向collection也可以面向SQL这种领域的关键字映射。
    // 所以有了Dataset/Dataframe之后SQL这种字符串到逻辑执行才可以被打通。SQL里的select等一定能够转成某种RDD包装的方式。
    // Dataset/Dataframe承上启下。
    // Spark的Dataset既可以按照collection，类似于RDD的方法操作，也可以按照SQL领域语言定义的方式操作数据

    // 最后这种方法最常用！！但所有这些在工作中都不怎么用，但是有助于理解Spark SQL
    // Dataset是RDD的扩展，RDD面只向collection，Dataset除了面向collection还能面向SQL等领域语言，DataFrame是他的一个子集
    //Spark 的Dataset  既可以按collection，类似于rdd的方法操作，也可以按SQL领域语言定义的方式操作数据
    // Dataset比RDD强的地方是有编码器，对数据有识别、序列化反序列化等优化，序列化之后的数据能少占用内存，且可以触发钨丝计划，使用堆外内存

    /**
     * 纯文本文件，不带自描述，string  不被待见的
     * 必须转结构化。。再参与计算
     * 转换的过程可以由spark完成
     * hive数仓（重要：集群大小、磁盘空间和block块大小都依托于数据特征的）
     * 接数：源数据   不删除，不破坏。接过数据来并转换成中间态这是个ETL的过程
     * ETL  中间态，此时还不能用，还要进行一些加工才能变成结果态
     * 所有的计算发生再中间态
     * 中间态：--> 一切以后续计算成本为考量：json、parquet（会压缩）.. 后续计算哪种最快选择哪种
     * 文件格式类型？
     * 中间态要分区/分桶 为了方便后续计算，要分析后续计算特征。分区（按照时间分区）表可以让计算起步的时候加载的数据量变少；分桶可以让计算过程中
     * shuffle移动的数据量变少
     */
    // import session.implicits._
    val ds01 = session.read.textFile("data/person.txt")
    val person = ds01.map(line => {
      val strs = line.split(" ")
      (strs(0), strs(1).toInt)
    })(Encoders.tuple[String, Int](Encoders.STRING, Encoders.scalaInt)) // 需要一个编码器，内存里得是字节数组，充分利用钨丝计划，调用堆里/外的字节数组. 或者import session.implicits._ 隐式转换
    val cperson = person.toDF("name", "age") // 转化为Dataframe 附加自描述
    cperson.show
    cperson.printSchema
  }
}
