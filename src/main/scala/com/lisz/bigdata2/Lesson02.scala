package com.lisz.bigdata2

import java.util.Date

object Lesson02 {

  def m(): Unit = {
    println("hello")
  }

  def main(args: Array[String]): Unit = {
    println("--------basic--------")
    def fun01(): Unit = {
      println("hello world")
    }
    fun01()
    var x = 3;
    println("--------function--------")
    var y = fun01()
    println(y)
    y

    def fun02(): String = {
      "asasda"
    }
    println(fun02())

    def fun03(a : Int): Unit = {
      println(a)
    }

    println("-------递归函数--------")
    def fun04(num : Int): Int = {
      if (num == 1) {
        num
      } else {
        num * fun04(num - 1)
      }
    }
    println(fun04(3))

    println("-------默认值函数--------")
    def fun05(a:Int=8, b:String="abc"): Unit = {
      println(s"$a\t$b")
    }
    fun05()
    fun05(10, "ccc")
    fun05(22)
    fun05(b="xxx")

    println("-------匿名函数--------")
    val f:(Int, Int)=>Int = (a, b) => { // (a:Int, b:Int)也行
      a + b
    }
    println(f(2, 3))

    println("-------嵌套函数--------")
    def fun06(a:String): Unit = {
      def fun05(): Unit = {
        println(a)
      }
      fun05()
    }
    fun06("aaaaa")

    println("-------偏应用函数--------")
    def fun07(date:Date, tp:String, msg:String): Unit = {
      println(s"$date\t$tp\t$msg")
    }
    fun07(new Date(), "info", "ok")
    val info = fun07(_:Date, "info", _:String) // 不用每次调用都传入 "info"了
    info(new Date(), "ok")
    val error = fun07(_:Date, "error", _:String)
    error(new Date(), "not ok")

    println("-------可变参数函数--------")
    def fun08(a:Int*): Unit = {
      for (elem <- a) {
        println(elem)
      }
      //a.foreach((x:Int)=>{println(x)})
//      a.foreach(println(_))
      a.foreach(println)
    }
    fun08(2)
    fun08(1, 2, 3, 4, 5, 6)

    println("-------高阶函数--------")
    // 函数为参数
    def compute(a:Int, b:Int, f: (Int, Int)=>Int): Int ={
      f(a, b)
    }
    println(compute(2, 3, (_+_)))
    // 函数作为返回值
    def factory (i:String): (Int, Int) => Int = {
      def add (a:Int, b:Int): Int = {
        a + b
      }
      if (i.equals("+")) {
        add
      } else if (i.equals("*")){
        (x:Int, y:Int)=>(x*y)
      } else if (i.equals("-")) {
        (x:Int, y:Int)=>(x-y)
      } else {
        (x:Int, y:Int)=>(x/y)
      }
    }
    println(compute(5, 6, factory("+")))

    println("-------柯里化--------") // 梳理化😂
    def fun09(a:Int)(b:Int)(c:String): Unit = {
      println(s"$a\t$b\t$c")
    }
    fun09(3)(8)("dhskjfhksj")

    def fun10(a:Int*)(b:String*): Unit = {
      a.foreach(println)
      b.foreach(println)
    }
    fun10(1,2,3,4,5,6)("asdhas", "jaghsdjsad")

    println("--------*.方法--------")
    //val func = m
    // 方法不想被执行：方法名+空格+_
    val func = m _
    // Java中+是关键字，scala中+是方法或函数，scala愈发重视没有基本类型的，数字3会被看成Int 3:Int
    // 语法 -> 编译器 -> 字节码 <- JVM规则。编译器衔接了人和机器

  }


}
