package com.lisz

object TestScala {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
  }
}

object aaa {
  private val x : aaa = new aaa("male")
  private val name = "zhangsan"
  println(s"bbb ... up...")
  def main(args: Array[String]): Unit = {
    println("Hello from aaa")
    println(s"name: ${x.name}")
    x.printMsg()
  }
  println(s"bbb ... down...")
}

class aaa (sex:String) {
  var a = 3
  var name = "lisi"
  //private val lisi = new bbb("lisi")
//  def this(name:String) {
//    this()
//    this.name = name
//  }
  println(s"bbb ... up $a ...")
  def printMsg(): Unit = {
    //println(s"hello ${aaa.name} from msg")
//    println(s"hello $name from msg")
    println(s"hello ${aaa.name} from msg")
  }
  println(s"bbb ... up ${a+6} ...")
}
