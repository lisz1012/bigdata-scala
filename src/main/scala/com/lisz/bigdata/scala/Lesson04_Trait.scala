package com.lisz.bigdata.scala

object Lesson04_Trait {

  trait God {
    def say(): Unit ={
      println("God... say")
    }
  }

  trait Daemon {
    def cry(): Unit ={
      println("Daemon...say")
    }
    def hurt():Unit
  }

  // Multi inherit
  class Person(name:String) extends God with Daemon {
    def hello(): Unit ={
      println(s"$name says hello!")
    }

    override def hurt(): Unit = {
      println("Person implements daemon's hurt, hairening...")
    }
  }

  def main(args: Array[String]): Unit = {
    val person = new Person("zhang san")
    person.hello()
    person.say()
    person.cry()
    person.hurt()
  }
}
