package com.lisz.bigdata2.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

class MyAggFun extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    StructType.apply(Array(StructField.apply("score", IntegerType, false)))
  }

  override def bufferSchema: StructType = {
    StructType.apply(Array(
      StructField.apply("sum", IntegerType, false),
      StructField.apply("count", IntegerType, false)))
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //组内一条记录调用一次
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    buffer(1) = buffer.getInt(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Double = {
    buffer.getInt(0) / buffer.getInt(1)
  }
}

object Lesson06_sql_functions {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("functions").master("local").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    import session.implicits._
    val dataDF = List(
      ("A", 1, 90),
      ("B", 1, 90),
      ("A", 2, 50),
      ("C", 1, 80),
      ("B", 2, 60)
    ).toDF("name", "class", "score")

    // 原生SQL排序统计
    dataDF.createTempView("users")
//    session.sql("select name, sum(score) sum from users group by name order by sum desc").show // order by 作用在结果表。给出了分组，按照分组算总分
    //session.sql("select * from users order by name, score desc").show() // 没有给出分组，那就是一个二次排序，先按照name后按照score排序

//    session.udf.register("fun01", (x:Int)=>{x * 10})
//    session.sql("select *, fun01(score) as fun01 from users").show

//    session.udf.register("ooxx", new MyAggFun)
//    session.sql("select ooxx(score) from users group by name").show()

    // 根据每一行都某一列的输入值，结果会有变化，这种use case可以用case when
//    session.sql("select *, " +
//      "case " +
//      "   when score <= 100 and score >=90 then '优'" +
//      "   when score <= 89 and score >=80 then '良'" +
//      "   when score <= 79 and score >=60 then '及格' " +
//      "   else '差' " +
//      "end as grade " +
//      "from users").show

//    session.sql("select case " +
//      " when score <= 100 and score >= 90 then '优' " +
//      " when score <= 89 and score >= 80 then '良' " +
//      " when score <= 79 and score >= 60 then '及格' " +
//      " else '差'" +
//      " end as grade, count(*) from users group by grade"
//    ).show()

    // 行列转换
    // session.sql("select name, explode(split(concat( case when class = 1 then '语文' when class = 2 then '数学' end, ' ', score), ' ')) from users").show()

    // OLAP 开窗函数fun() over(partition order)
    // group by 是纯聚合函数
    // session.sql("select *,   rank()  over (partition by class order by score desc) as rank, row_number() over(partition by class order by score desc) as number from users").show

    // 选出了每个人的各科排名和总人数
    session.sql("select *, rank() over(partition by class order by score desc) as rank, count(score) over (partition by class) as num from users").show
    /**
     * +----+-----+-----+----+---+
     * |name|class|score|rank|num|
     * +----+-----+-----+----+---+
     * |   A|    1|   90|   1|  3|
     * |   B|    1|   90|   1|  3|
     * |   C|    1|   80|   3|  3|
     * |   B|    2|   60|   1|  2|
     * |   A|    2|   50|   2|  2|
     * +----+-----+-----+----+---+
     */
//    println("--------------------")
//    val res = session.sql("select name, sum(score) from users group by name order by name desc")
//    println("--------------------")
//    res.explain(true)

    val res = session.sql("select ta.name, ta.class, tb.score + 20 + 80 from (select name, class from users) as ta " +
      "join (select name, score from users where score > 10) as tb " +
      "on ta.name = tb.name" +
      " where tb.score > 60") // 单表关联、自关联，一张表里面既有发款记录也有对方的收款记录
    res.show
    res.explain(true)

//    val res = session.sql("select ta.name from (select * from (select * from users where score > 60) as tt) as ta")
//    res.show
//    res.explain(true)

    //    session.sql("select class, count(score) as num  from users group by class").show
  }
}
