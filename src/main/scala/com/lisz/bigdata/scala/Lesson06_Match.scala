package com.lisz.bigdata.scala

// Match == Java Switch
object Lesson06_Match {
  def main(args: Array[String]): Unit = {
    val tup = (1.0, 88, "abc", false, 99)
    val iterator = tup.productIterator
    val res = iterator.map((x) => {
      x match {
        case 1 => println(s"$x...is 1")
        case 88 => println(s"$x is 88")
        case false => println(s"$x...is false")
        case w: Int if w > 50 => println(s"$w is > 50")
        case _ => println("I don't know what the type is")
      }
    })
    while (res.hasNext) {
      println(res.next)
    }

    println(s"args.length = ${args.length}")
    val env = "PROD"
    var db = ""
    env match {
      case "PROD" => db = "prod"
      case "PROD_TW" => db = "prod_tw"
      case _ => throw new IllegalArgumentException(s"""ENV $env is not allowed value""")
    }
    println(s"""DB = $db""")

    for (day <- 1 to 5) {
      println(s"Day of week: $day")
    }
  }
}
