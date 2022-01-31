package com.lisz.bigdata2.spark.streaming

import java.io.PrintStream
import java.net.ServerSocket

object MakeData {
  def main(args: Array[String]): Unit = {
    val listen = new ServerSocket(8889)
    println("Server starts")
    while (true) {
      val client = listen.accept
      new Thread() {
        override def run(): Unit = {
          var num = 0
          if (client.isConnected) {
            val out = client.getOutputStream
            val printStream = new PrintStream(out)
            while (client.isConnected){
              num += 1
              printStream.println(s"hello ${num}")
              printStream.println(s"hi ${num}")
              printStream.println(s"hi ${num}")
              Thread.sleep(1000)
            }
          }
        }
      }.start
    }
  }

}
