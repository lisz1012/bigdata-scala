package com.lisz.bigdata2.scala

object Lesson01 {
  def main(args: Array[String]): Unit = {
    var a = 3
    if (a < 0) {
      println(s"$a<0")
    } else {
      println(s"$a>=0")
    }

    var b = 0
    while (b < 10) {
      println(b)
      b += 1
    }

    println("----------------------------")

    val seqs : Range.Inclusive = 1 to (10, 2) // 1-10, 步进为2
    println(seqs)
    val seq2 = 1 until(10)
    println(seq2)
    for (i <- seq2 if(i%2==0)) {
      println(i)
    }
    println("----------------------------")
//    for (i <- 1 to 9) {
//      for (j <- 1 to i) {
//        print(s"$i*$j=${i*j}  ")
//      }
//      println()
//    }
      var n = 0;
      for (i <- 1 to 9; j <- 1 to 9 if(j <= i)) { // if 守卫
        n += 1
        print(s"$i*$j=${i*j}  ")
        if (i == j) {
          println()
        }
      }
    println(n)

    val seq3 = for (i <- 1 to 10) yield {
      i * 2
    }
    println(s"seq3: $seq3")
    for (i <- seq3) {
      println(i)
    }
  }
}
