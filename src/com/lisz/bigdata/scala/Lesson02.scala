package com.lisz.bigdata.scala

import java.util.Date

object Lesson02 {
  def function(): Unit ={
    println("Hello object!")
  }

  def main(args: Array[String]): Unit = {
    println("--------basic---------")

    def fun01() {
      println("Hello world")
    }

    fun01()
    var x = 3
    var y = fun01()
    println(y)
    var z = fun01
    println(z)

    def fun02(): Int = {
      return 3
    }

    y = fun02()
    println(y)

    def fun03(a: Int): Unit = {
      println(a)
    }

    fun03(33)

    println("--------递归函数---------")

    def fun04(num: Int): Int = {
      if (num == 1) {
        num
      }
      num * fun04(num - 1)
    }

    val i: Int = fun04(4)
    println(i)


    println("--------默认值函数---------")

    def fun05(a: Int = 8, b: String = "abc"): Unit = {
      println(s"$a\t$b")
    }

    fun05()
    fun05(9, "def")
    fun05(22)
    fun05(b = "xxx")


    println("--------匿名值函数---------")
    var c = (a: Int, b: Int) => {
      a + b
    }
    val w = c(5, 6)
    println(c(3, 4))
    println(w)

    var d: (Int, Int) => Int = (a: Int, b: Int) => {
      a * b
    }

    println("--------嵌套值函数---------")

    def fun06(a: String): Unit = {
      def fun05(): Unit = {
        println(a)
      }

      fun05()
    }

    fun06("abc")


    println("--------偏应用函数---------")

    def fun07(date: Date, tp: String, msg: String): Unit = {
      println(s"$date\t[$tp]\t$msg")
    }

    fun07(new Date(), "INFO", "ok")
    var info = fun07(_: Date, "INFO", _: String)
    var error = fun07(_: Date, "ERROR", _: String)
    info(new Date(), "OK")
    error(new Date(), "Got an error")


    println("--------可变参数函数---------") // 传入类型要一致
    def fun08(a: Int*): Unit = {
      for (i <- a) {
        println(i)
      }
      println("==============")
      a.foreach((x: Int) => {
        println(x)
      })
      println("==============") // 参数在函数体中只使用一次的时候，可以把它的声明去掉，然后把实现中的它换成"_"
      a.foreach(println(_))
      println("==============") // println作为参数传了进去
      a.foreach(println)
    }
    fun08(2)
    fun08(1, 2, 3, 4, 5, 6)

    println("--------高阶函数---------") // 函数作为参数和返回值
    //函数作为参数
    def calculate(a:Int, b:Int, f:(Int, Int) => Int): Unit = {
      println(f(a, b))
    }
    calculate(3, 8, (a:Int, b:Int) => {
      a + b
    })
    calculate(3, 8, (a:Int, b:Int) => {
      a * b
    })
    calculate(3, 8, _ * _)// 函数体里的出现顺序与声明里相同, 第一个_代表第一个参数，；第二个_代表第二个参数
    // 函数作为返回值。Java中，+叫"关键字"；scala中，+叫"函数"或"方法"。Scala中没有基本类型，所以写一个数字3，语法是把3看待成Int对象
    def factory(i:String): (Int, Int) => Int = {
      def plus(x:Int, y:Int): Int = {
        x + y
      }
      if(i.equals("+")) {
        plus
      } else if (i.equals("*")){
        _*_
      } else if(i.equals("-")) {
        _-_
      } else {
        _/_
      }
    }
    calculate(3, 8, factory("-"))

    println("--------柯里化---------") // 函数作为参数和返回值
    def fun09(a:Int)(b:Int)(c:String): Unit = {
      println(s"$a\t$b\t$c")
    }
    fun09(3)(8)("abc")
    def fun10(a:Int*)(b:String*): Unit = {
      a.foreach(println)
      b.foreach(println)
    }
    fun10(3, 8, 10)("abc", "def")

    println("--------*.方法---------") // 函数作为参数和返回值
    val func = function     // 方法无参数就可以省略括号
    val func1 = function _  // 方法不想执行，复制给一个引用，方法名 + 空格 + 下划线
    func1()
  }
}
