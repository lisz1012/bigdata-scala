package com.lisz.bigdata2.scala

object Lesson07_PartialFunction {
  def xxx:PartialFunction[Any, String] = {
    case "hello" => "val is hello"
    case x:Int => s"$x...is int"
    case _ => "none"
  }

  def main(args: Array[String]): Unit = {
    println(xxx(55))
    println(xxx("hello"))
    println("hi")
  }
}
