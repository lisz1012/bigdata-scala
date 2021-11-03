package com.lisz.bigdata2.scala

import java.util

object Lesson08_implicit {
  def main(args: Array[String]): Unit = {
    val list1= new util.ArrayList[Int]()
    list1.add(1)
    list1.add(2)
    list1.add(3)
    val list2= new util.ArrayList[Int]()
    list2.add(4)
    list2.add(5)
    list2.add(6)
//    foreach(list, println)
//    def foreach[T](list:util.LinkedList[T], f:(T)=>Unit): Unit ={
//      val iter = list.iterator()
//      while (iter.hasNext) {
//        f(iter.next())
//      }
//    }
    // 类XXX的构造和foreach方法分别分担了原来的一个参数
//    val xx = new XXX[Int](list)
//    xx.foreach(println)
//    list1.foreach(println)
    // 必须先承认一件事情：List里面没有foreach方法。这些代码最终交给的是scala的编译器
    // 1. scala编译器发现有错误
    // 2。寻找他有没有implicit定义的方法，且方法的参数正好是list的类型
    // 3。编译期：完成人类：
    // val xx = new XXX[Int](list)
    //    xx.foreach(println)
    // 编译器帮人把代码改写了
    // 隐式转换方法
    implicit def aaa[T](list:util.LinkedList[T]): XXX[T] = {
      val iter = list.iterator()
      new XXX(iter)
    }
    implicit def bbb[T](list:util.ArrayList[T]): XXX[T] = {
      val iter = list.iterator()
      new XXX(iter)
    }
    // RDD N个方法 scala， 不去修改源代码的情况下，无限扩展某个类, 增加其方法
//    implicit class XXX[T](list:util.LinkedList[T]) {
//      def foreach(f:(T)=>Unit): Unit ={
//        val iter: util.Iterator[T] = list.iterator()
//        while (iter.hasNext) {
//          f(iter.next())
//        }
//      }
//
//      def hello(): Unit = {
//        println("hello")
//      }
//    }
    list1.foreach(println)
    //list.hello
    println("-------")
    list2.foreach(println)



    // 隐式转换参数
    implicit val ascxaca:String = "lisi"
    implicit val age = 1
    // implicit val ascsdfdsf:String = "wangwu"
    def xxx (implicit name:String, age:Int): Unit = { // 在implicit后面的多个参数都会被参数列表里的 implicit影响，在外面都要隐式定义
      println(name)
      println(age)
    }
    // xxx("zhangsan")
    xxx

    def yyy (age:Int)(implicit name:String): Unit = { // 在implicit后面的多个参数都会被参数列表里的 implicit影响，在外面都要隐式定义
      println(name)
      println(age)
    }
    yyy(66)("zhangsan")
    yyy(66)
  }
}

class XXX[T](list:util.Iterator[T]) {
  def foreach(f:(T)=>Unit): Unit ={
    while (list.hasNext) {
      f(list.next())
    }
  }
}