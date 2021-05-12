package com.lisz.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/*
接收socket的数据，
Receiver要具备链接Socket的能力： host:port
只管着建立连接，读取数据，存储，计算的事情，Receiver不管
 */
class CustomReceiver(host:String, port:Int)
  extends Receiver[String] (StorageLevel.DISK_ONLY)  {

  override def onStart(): Unit = {
    new Thread{
      override def run(): Unit = (
        connect()
      )
    }.start()
  }

  private def connect(): Unit = {
    val server = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(server.getInputStream))
    var line = reader.readLine()
    while (server.isConnected && line != null) {
      store(line)
      line = reader.readLine()
    }
    reader.close()
  }

  override def onStop(): Unit = ???
}
