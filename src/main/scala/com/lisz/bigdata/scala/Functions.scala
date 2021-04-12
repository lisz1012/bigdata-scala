package com.lisz.bigdata.scala

import java.util.Date

object Functions {
  def function(): Unit = {
    println("Hello")
  }

  def main(args: Array[String]): Unit = {
    println("----- basic -----")
    def fun01(): Unit ={
      println("Hello world")
    }
    fun01()

    def fun02(): Int = {
      3
    }
    println(fun02())

    println("----- 传参 -----")
    val a = 5
    def fun03(a:Int): Unit = {
      println(a)
    }
    fun03(a)

    println("----- 递归函数 -----")
    def fun04(n:Int): Int = {
      if (n == 0) {
        return 1   // There must be a "return" here
      }
      n * fun04(n - 1)
    }
    println(fun04(4))

    // 默认值函数
    println("----- 默认值函数 -----")
    def fun05(a:Int = 3, b:String = "abc"): Unit ={
      println(s"$a\t$b")
    }
    fun05()
    fun05(5)
    fun05(b = "def")
    fun05(10, "hello")

    println("----- 匿名函数 -----")
    var c = (a:Int, b:Int) => {
      a + b
    }
    val w = c(5, 6)
    println(w)
    println(c(10, 20))

    var d: (Int, Int) => Int = (a:Int, b:Int) => {
      a * b
    }
    println(d(2, 3))

    println("----- 偏应用函数 -----")
    def fun07(date:Date, tp:String, msg:String): Unit = {
      println(s"$date\t[$tp}\t$msg")
    }
    var info = fun07(_:Date, "INFO", _:String)
    var error = fun07(_:Date, "ERROR", _:String)
    info(new Date(), "OK")
    error(new Date(), "Error")

    println("----- 高阶函数 -----")
    def calculate(a:Int, b:Int, f:(Int, Int) => Int): Unit = {
      println(f(a,b))
    }
    calculate(2, 3, (a:Int, b:Int) => {
      a + b
    })
    calculate(2, 3, (a:Int, b:Int) => {
      a * b
    })
    calculate(3, 8, _*_)

    def factory(i:String): (Int, Int) => Int = {
      if (i.equals("+")) {
        _+_
      } else if(i.equals("*")) {
        _*_
      } else if(i.equals("-")) {
        _-_
      } else {
        _/_
      }
    }
    calculate(12, 3, factory("*"))

    println("----- 柯里化 -----")
    def fun08(a:Int*)(b:Long*)(c:String*): Unit = {
        a.foreach(println)
        b.foreach(println)
        c.foreach(println)
    }
    fun08(1,2,3)(10L, 20L, 30L)("Hello", "my", "scala")

    println("----- *.方法 -----")
    var func = function
    var func1 = function _
    func1()
  }
}
