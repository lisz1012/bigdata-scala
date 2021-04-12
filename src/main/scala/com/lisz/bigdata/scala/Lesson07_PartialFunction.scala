package com.lisz.bigdata.scala

object Lesson07_PartialFunction {
  def main(args: Array[String]): Unit = {
    def xxx: PartialFunction[Any, String] = {
      case "hello" => "val is hello"
      case x:Int => s"$x...is int"
      case _ => "none"
    }

    println(xxx(55))
    println(xxx("hello"))
    println(xxx("hi"))
  }

}
