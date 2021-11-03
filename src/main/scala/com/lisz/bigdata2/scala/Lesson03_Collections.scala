package com.lisz.bigdata2.scala

import java.util

import scala.collection.mutable.ListBuffer

object Lesson03_Collections {
  def main(args: Array[String]): Unit = {
    val listJava = new util.LinkedList[String]()
    listJava.add("hello")

    // Scala 还有自己的数据结构
    // 1。 数组
    // val不可变描述的是引用的地址的值
    val arr = Array[Int](1,2,3,4,5)
    println(arr{1}) // 大小括号都可以
    println(arr(1)) // 大小括号都可以
    arr(1) = 99
    println(arr(1))
    arr.foreach(println)

    // 2。列表/链表，默认的事不可变的。里面的对象都不能增删改
    val list01 = List(1,2,3,4,5,4,3,2,1)
    list01.foreach(println)
    println("-------------------------")

    val list02 = new ListBuffer[Int]
    list02+=300
    list02+=400
    list02+=500
    list02+=600
    list02.foreach(println)

    println("-----Set-----")
    val set01 = Set[Int](1, 2, 3, 4, 5, 1)
    set01.foreach(println)
    // 可变的Set
    import scala.collection.mutable.Set
    val set02 = Set(11, 22, 33, 44, 55, 11)
    set02.add(200)
    set02.foreach(println)
    val set03 = scala.collection.immutable.Set(111, 222, 333)
    set03.foreach(println)

    // Tuple
    val t2 = new Tuple2(11, "asdads") // 很像键值对
    val t3 = Tuple3(1, "sdfds", "sdas")
    val t4 = (1, 2, 3, 4)
    val t22 = ((a:Int, b:Int) => a + b + 8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
    println(t2._1)
    println(t4._3)
    println(t22._1(8, 2))
    println(t22._1)
    val iter = t22.productIterator
    while (iter.hasNext) {
      println(iter.next())
    }

    println("--------Map--------")
    val map01 = Map(("a", 33), "b" -> 22, ("c", 1314), "a" -> 3333)
    val keys = map01.keys
    keys.foreach(println)
    for (elem <- keys) {
      println(s"$elem -> ${map01.get(elem).get}")
    }
    // Option: None/Some
    println(map01.get("a"))
    println(map01.get("a").get)
    println(map01.get("z"))
    //println(map01.get("z").get)  // NoSuchElementException
    println(map01.get("z").getOrElse("hello"))

    val map02 = scala.collection.mutable.Map(("a", 11), ("b", 22))
    map02.put("c", 33)

    println("--------艺术---------")
    val list = List(1, 2, 3, 4, 5, 6)
    list.foreach(println)
    val listMap = list.map((x: Int) => x + 10)
    listMap.foreach(println)
    list.foreach(println) // 老版本数据不会被改变
    val listMap2 = list.map(_ * 10)
    listMap2.foreach(println)

    println("--------艺术-升华-------0")
    val listStr = List("hello scala", "hello spark", "good idea")
    val flatMap = listStr.flatMap(_.split("\\s+"))
    flatMap.foreach(println)
    val mapList = flatMap.map((_, 1))
    mapList.foreach(println)

    println("--------艺术-再-升华-------")
    val iterator = listStr.iterator // 迭代器不存数据，只是存一个指针
    // flatMap只是返回了一个新的迭代器，没有发生计算。一个iterator会返回另一个Iterator, 在新迭代器的hasNext和next中被传入的
    // 函数得以被调用。多个Iterator会像这样被连成一个链条。中间只有一个小的cur存储空间被占用到了（见flatMap函数）
    // Java 8中新增的集合的map等方法，来自scala，也可以说来自spark的各个同名算子
    val iterFlatMap = iterator.flatMap(_.split("\\s+"))
    //iterFlatMap.foreach(println)
    val iterMapList = iterFlatMap.map((_, 1))
    //iterMapList.foreach(println)
    while (iterMapList.hasNext) {
      val tuple = iterMapList.next
      println(tuple)
    }
    //listStr.flatMap(_.split("\\s+")).map((_,1)).(_+_)

    // listStr是真正的数据集





  }
}
