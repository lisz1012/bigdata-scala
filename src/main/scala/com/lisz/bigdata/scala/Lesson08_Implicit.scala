package com.lisz.bigdata.scala

import java.util

object Lesson08_Implicit {
  def main(args: Array[String]): Unit = {
    val linkedList = new util.LinkedList[Int]()
    linkedList.add(1)
    linkedList.add(2)
    linkedList.add(3)

    val arrayList = new util.ArrayList[Int]()
    arrayList.add(4)
    arrayList.add(5)
    arrayList.add(6)

    //list.foreach(println) //3个东西：list数据集、foreach遍历行为、println 处理函数
    //foreach(list, println)
//    val xxx = new XXX[Int](list)
//    xxx.foreach(println)
    //隐式转换方法
    implicit def assada[T](list: util.LinkedList[T]) = {
      val it = list.iterator()
      new XXX(it)
    }
    implicit def asdasdasd[T](list:util.ArrayList[T]) = {
      val it = list.iterator()
      new XXX(it)
    }
    //隐式转换类 spark RDD N方法， scala 扩展RDD
//    implicit class XXX[T](list:util.LinkedList[T]){
//      def foreach(f:(T) => Unit): Unit = {
//        val it = list.iterator()
//        while (it.hasNext) {
//          f(it.next())
//        }
//      }
//    }
    linkedList.foreach(println) // list原来并没有foreach方法，java里这么写会报错，这些代码最终交给的事scala的编译器，scala也发现了这里不对劲
    // 但是他先不抛出异常，而是先去找implicit 关键字定义的方法，且方法的参数正好是list的类型，如果有，则在编译器，完成人类手写new XXX(list)
    // 的过程：
    //    val xxx = new XXX[Int](list)
    //    xxx.foreach(println)
    // 编译器帮人们把代码给改写了，但是XXX[T]类及其代码必须得写
    arrayList.foreach(println)

    implicit val asdsakhd:String = "li si"
    //implicit val asdsakhd2:String = "wang wu"
    implicit val sadsad:Int = 88
//    def xxxx(implicit name:String, age:Int): Unit ={
//      println(s"$name\t$age")
//    }
    def xxxx(age:Int)(implicit name:String): Unit ={
      println(s"$name\t$age")
    }

    //xxxx("zhang san")
    //xxxx
    xxxx(100)("dashjsd")
    xxxx(120)
  }

}

//class XXX[T](list:util.LinkedList[T]){
//  def foreach(f:(T) => Unit): Unit = {
//    val it = list.iterator()
//    while (it.hasNext) {
//      f(it.next())
//    }
//  }
//}
class XXX[T](iter:util.Iterator[T]){
  def foreach(f:(T) => Unit): Unit = {
    while (iter.hasNext) {
      f(iter.next())
    }
  }
}
