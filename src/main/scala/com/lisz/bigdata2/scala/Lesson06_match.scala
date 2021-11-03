package com.lisz.bigdata2.scala

// match java中的switch
object Lesson06_match {
  def main(args: Array[String]): Unit = {
    val tup = (1.0, 88, "abc", false, 99)
    val iter = tup.productIterator
    val res = iter.map(x => { // 这个大括号会返回一个Unit，while中打印出()
      x match {
        case 1 => println(s"$x...is 1  ")
        case 88 => println(s"$x... is 88")
        case false => println(s"$x...is false")
        case w: Int if w > 50 => println(s"$w...is > 50")
        case _ => println("我也不知道啥类型")
      }
    })
    while (res.hasNext) {
      println(res.next)
    }
  }

}
