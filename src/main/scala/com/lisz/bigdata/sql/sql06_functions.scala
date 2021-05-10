package com.lisz.bigdata.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

// 聚合前提是由组的概念，必须group by？不一定，sum也可以
class MyAggFun extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    // ooxx(score)
    StructType.apply(Array(StructField.apply("score", IntegerType, false)))
  }

  override def bufferSchema: StructType = {
    // avg sum / count = avg
    StructType.apply(Array(
      StructField.apply("sum", IntegerType, false),
      StructField.apply("count", IntegerType, false)
    ))
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 组内一条记录调用一次
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    buffer(1) = buffer.getInt(1) + 1
  }

  // 有可能多次溢写，则有多次buffer
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Double = {
    buffer.getInt(0) / buffer.getInt(1) * 1.0
  }
}


object sql06_functions {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("functions").master("local").getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._
    val dataDF = List(
      ("A", 1, 90),
      ("B", 1, 90),
      ("A", 2, 50),
      ("C", 1, 80),
      ("B", 2, 60)
    ).toDF("name", "class", "score")
    dataDF.createTempView("users")

    // 分组排序的统计
//    session.sql("select name, sum(score) sum_score from users group by name order by sum_score desc").show()
//    session.sql("select * from users order by name desc, score").show
//    session.sql("select name, avg(score) avg from users group by name order by avg desc").show()
//    dataDF.groupBy("name").avg("score").show()

    // 自定义函数，用的不多，自带的就够用
//    session.udf.register("ooxx", (x:Int) => {x*10})
//    session.sql("select *, ooxx(score) ox from users sort by ox desc").show()

//    session.udf.register("ooxx", new MyAggFun)
//    session.sql("select name, ooxx(score) avg_score from users group by name order by avg_score desc").show()

    // case when
//    session.sql("select *, " +
//      "case " +
//      "  when score <= 100 and score >= 90 then 'Excellent' " +
//      "  when score < 90 and score >= 80 then 'Good' " +
//      "  when score < 80 and score >= 60 then 'Average' " +
//      "  else 'Fail' " +
//      "end as ox " +
//      " from users").show()


    // 优良差分组
//    session.sql("select " +
//      " case " +
//      "   when score <= 100 and score >= 90 then 'Excellent' " +
//      "   when score < 90 and score >= 80 then 'Good' " +
//      "   when score < 80 and score >= 60 then 'Average' " +
//      "   else 'Fail'" +
//      " end as grd, " +
//      " count(*)  " +
//      " from users " +
//      " group by grd "
//    ).show()

    // 行列转换: 由一行多列变为多行
//    session.sql("select  name, " +
//      " explode(split(concat(case when class = 1 then 'AA' else 'BB' end, ' ', score), ' ')) " +
//      " as ox " +
//      " from users ").show()

    // OLAP 开窗函数 依赖 fun() over(partition order). group by 是纯聚合函数：一组最后就一条. over做的是分区排序/统计 ，rank作用在每条记录上
//    session.sql("select *,   " +
//      " rank() over(partition by class order by score desc) as rank, " +  //rank(score) 也可以
//      " row_number() over(partition by class order by score desc) as number " +
//      "  from users  ").show()
//
//    session.sql("select *,    " +
//      " count(score) over(partition by class) as num  " +
//      " from users  ").show()
//
//
//
//
//    println("----------------------")
//    //session.sql("select class, count(score) as num from users group by class").show()
//    val res = session.sql("select name, sum(score) sum_score from users group by name order by sum_score desc")
//    res.show()
//    println("----------------------")
//    res.explain(true)

    val res = session.sql("select ta.name, ta.class, tb.score  + 20 + 80  " +
      "          " +
      " from   " +
      "  (select name, class from users) as ta  " +
      " join  " +
      " (select name, score from users where score > 10) as tb  " +
      " on ta.name = tb.name    " +
      " where   tb.score > 60   ")
//    val res = session.sql("select ta.name from  " +
//        " (select * from  " +
//        " (select * from users  where score > 60  ) as tt)  as ta  ")
    res.show()
    println("-----------------------------")
    res.explain(true)
  }

}
