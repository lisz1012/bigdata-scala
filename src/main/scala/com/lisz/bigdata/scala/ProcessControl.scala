package com.lisz.bigdata.scala

object ProcessControl {
  def main(args: Array[String]): Unit = {
    var a:Int = 3
    if(a < 0) {
      println("Negative")
    } else if(a > 0){
      println("Positive")
    } else {
      println(println("Zero"))
    }

    var b = 0;
    while (b < 10) {
      println(b)
      b += 1
    }

    val seq1 = 1 to 10
    println("------- seq1 -------")
    for (i <- seq1){
      println(i)
    }

    val seq2 = 1 to(10, 2)
    println("------- seq2 -------")
    for(i <- seq2){
      println(i)
    }

    val seq3 = 1 until(9, 2)
    println("------- seq3 -------")
    for(i <- seq3){
      println(i)
    }

    // Multiplication table
    for(i <- 1 to 9; j <- 1 to 9 if(j <= i)) {
      print(s"$i * $j = ${i * j}\t")
      if (i == j){
        println()
      }
    }

    // Collect even numbers with "yield" key word
    val seq5 = for (i <- 1 to (10) if (i % 2 == 0)) yield i
    println(seq5)

    val seq6 = for (i <- 1 to 10 if (i % 2 == 1)) yield {
      i * 2
    }
    println(seq6)
  }

}
