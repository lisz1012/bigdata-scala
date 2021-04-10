package com.lisz.bigdata.scala

object Lesson01 {
  /*
   if, while, for
  */
  def main(args: Array[String]): Unit = {
    var a = 3
    if (a < 0){
      println(s"$a < 0")
    } else if (a > 0){
      println(s"$a > 0")
    } else {
      println(s"$a == 0")
    }

    var b:Int = 0
    while (b < 10){
      println(b)
      b+=1;
    }

    val seqs = 1 to 10
    println(seqs)
    val seqs2 = 1 to (10, 2)
    println(seqs2)
    val seqs3 = 1 until(10, 2)
    println(seqs3)
    val seqs4 = 1 until(10)
    println(seqs4)

    for (i <- seqs) {
      println(i)
    }
    for (i <- 1 until 10) {
      println(s"i = $i")
    }
    for (i <- 1 until (10) if(i % 2 == 0)) {
      println(s"i = $i")
    }
    println("-------------------")
    for (i <- 1 to 9) {
      for (j <- 1 to i) {
        print(s"$i * $j = ${i * j}  ")
      }
      println()
    }
    println("-------------------")
    for (i <- 1 to 9; j <- 1 to i) {
      print(s"$i * $j = ${i * j}\t")
      if (i == j){
        println()
      }
    }
    println("-------------------")
    for (i <- 1 to 9; j <- 1 to 9 if(j<=i)) {
      print(s"$i * $j = ${i * j}\t")
      if (i == j){
        println()
      }
    }

    val seq4 = for (i <- 1 to (10) if(i%2==1)) yield i
    println(seq4)

    val seqs5 = for (i <- 1 to 10 if(i%2==0)) yield {
      i * 2
    }
    for (i <- seqs5) {
      print(i + "  ")
    }
  }
}
