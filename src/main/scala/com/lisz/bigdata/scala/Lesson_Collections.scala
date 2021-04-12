package com.lisz.bigdata.scala

import java.util

import scala.collection.mutable.ListBuffer

object Lesson_Collections {
  def main(args: Array[String]): Unit = {
    val listJava = new util.LinkedList[String]()
    listJava.add("aaa")
    listJava.add("bbb")
    listJava.add("ccc")

    // scala's own data structure: array
    val arr1 = Array[Int](1, 2, 3)
    // Get element
    println(arr1.apply(1))
    println(arr1(1))
    // Set value to an element
    arr1.update(1, 100)
    println(arr1.apply(1))
    arr1(1) = 99
    println(arr1(1))

    for (ele <- arr1) {
      println(ele)
    }
    arr1.foreach(println) // Spark's style


    // scala's own data structure: LinkedList
    // scala的Collection中有两个包：immutable和mutable，常用的是默认的是不可变的
    val list1 = List(1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1) // immutable
    for (elem <- list1) {
      println(elem)
    }
    println("------ list ------")
    list1.foreach(println)

    println("------ list2 ------")
    val list2 = new ListBuffer[Int]()
    list2.+=(3)
    list2 += 4
    list2 += 5
    list2 += 5
    list2.foreach(println)
    println("--- after moving 5")
    list2 -= 5 // 一个值=5的元素被删除
    list2.foreach(println)

    println("--- after moving 5")
    list2 -= 5 //一个值=5的元素被删除
    list2.foreach(println)

    println("------ Set ------")
    val set = Set(1, 2, 3, 3)  // immutable
    set.foreach(println)
    for (elem <- set) {
      println(elem)
    }

    import scala.collection.mutable.Set
    val set2 = Set(11, 22, 33, 44, 11)
    set2.foreach(println)
    println("------ After Adding ------")
    set2.add(88)
    set2.foreach(println)

    println("------ Immutable again ------")
    val set3 = scala.collection.immutable.Set(100, 200, 300)
    set3.foreach(println)

    // Tuple
    println("------ Tuple ------")
    val t2 = new Tuple2(11, "adda") // Key - Value
    println(s"${t2._1}\t${t2._2}")
    println(t2)
    val t3 = Tuple3(22, "dfsadfsaf", 's')
    val t4: (Int, Int, Int, Int) = (1, 2, 3, 4)
    println(t4._4)
    val t22 = ((a: Int, b:Int) => a + b + 9, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4)
    val i:Int = t22._1(12, 3)
    println(i)
    println(t22._1)

    val iterator = t22.productIterator
    while (iterator.hasNext){
      print(s"${iterator.next()}  ")
    }
    println()

    println("------ Map ------")
    val map = Map(("a", 33), "b" -> 22, ("c", 345), "a" -> 35)
    print(map.size)
    map.foreach(println)
    val keys:Iterable[String] = map.keys
    println(map.get("a"))
    println(map.get("a").getOrElse("No a"))
    println(map.get("w").getOrElse("No w"))

    val iterator1 = keys.iterator
    while (iterator1.hasNext) {
      val str = iterator1.next()
      println(str)
    }
    keys.foreach(k=>println(s"key: $k\tValue: ${map.get(k).get}"))

    // mutable
    val map2 = scala.collection.mutable.Map(("a", 11), ("b", 22))
    map2.put("c", 33)
    map2.foreach(println)

    println("------ 艺术 ------")
    val list = List(1, 2, 3, 4, 5, 6)
    list.foreach(println)
    val list3 = list.map((x: Int) => x + 10)
    list3.foreach(println) //新集合
    list.foreach(println)  // 不变
    val list4 = list.map(_ * 10)
    list4.foreach(println)

    println("------ 艺术升华 ------")
    val listStr = List("Hello world", "Hello lisz", "good idea")
    //val listStr = Array("Hello world", "Hello lisz", "good idea")
    //val listStr = Set("Hello world", "Hello lisz", "good idea")
    val listStr2 = listStr.flatMap((x: String) => x.split("\\s+"))
    listStr2.foreach(println)
    val listStr3 = listStr2.map((_, 1))
    listStr3.foreach(println)
    // 以上代码的问题是：每一步计算数据都留有对象数据，内存使用扩大了N倍。Iterator解决这个问题

    println("------ 艺术再升华 ------")
    val iterator2 = listStr.iterator  //什么是迭代器？为什么会有迭代器模式？迭代器里不存数据
    val listStr4 = iterator2.flatMap((_.split("\\s+"))).map((_, 1))
    listStr4.foreach(println)

    println("-----------------------")
    val iterator3 = listStr.iterator
    val listStr5 = iterator3.flatMap((_.split("\\s+"))) // 这里只是new迭代器，注册hasNext和next方法
    //listStr5.foreach(println)
    //上面的println已经将listStr5迭代器的指针移动到了末尾
    val listStr6 = listStr5.map((_, 1))
    //listStr6.foreach(println)
    while (listStr6.hasNext){
      val tuple = listStr6.next() // 什么时候这里调用了，才会调用到被注册的方法
      println(tuple)
    }
  }
}
