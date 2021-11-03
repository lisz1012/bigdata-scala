package com.lisz.bigdata2.scala

trait God {
  def say(): Unit = {
    println("god...say")
  }
}

trait Monster {
  // 会报错：class Person inherits conflicting members
//  def say(): Unit ={
//    println("monster..say")
//  }

  def cry(): Unit = {
    println("Monster...cry")
  }
  def haiRen: Unit
}

class Person (name:String) extends God with Monster {
  def hello: Unit ={
    println(s"$name say hello!")
  }

  override def haiRen: Unit = {
    println("害人")
  }
}

object Lesson04_trait {
  def main(args: Array[String]): Unit = {
    val p = new Person("zhangsan")
    p.hello
    p.cry
    p.say
    p.haiRen
  }
}
