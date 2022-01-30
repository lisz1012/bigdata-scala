package com.lisz.bigdata2.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * 接收socket数据
 * 那么receiver要具备连接socket的能力：host port
 * Receiver 只负责连、读、存。计算还是由Driver端来控制的
 */
class CustomReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.DISK_ONLY){
  override def onStart(): Unit = {
    new Thread() {
      override def run(): Unit = {
        aaa()
      }
    }.start
  }

  private def aaa(): Unit = {
    val server = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(server.getInputStream))
    var line = reader.readLine()
    while (!isStopped() && line != null) {
      store(line)
      line = reader.readLine()
    }
  }

  override def onStop(): Unit = ???
}
