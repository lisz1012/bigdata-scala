package com.lisz.bigdata.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/*
1。checkpoint不会立刻触发作业
2。action算子触发runJob之后再触发的checkpoint
3。checkpoint会单独触发runJob（通电：代码逻辑会重复一次）：checkpoint会调用rdd.sparkContext.runJob
4。所以，我们在使用checkpoint的时候要结合persist算子，因为persist是优先任务执行的，然而checkpoint是之后任务或者job的
无论persist、checkpoint都有一个共通点：那些被重复利用的数据
RDD：弹性的分布式的数据集
分布式：partition
弹性的：某一个分区可能是memory、disk、hdfs
mapreduce中，如果一个转换的数据很值钱，可以单独跑一个map的MR程序，map的结果只能存在HDFS（没有spark 这么有弹性）后续可以面向这个结果，
跑几百个不同逻辑的计算，但是memory没法用。
cache得太多，反而会挤占Execution的内存空间，增加磁盘的IO，降低效率
RDD在Driver里面被new出来，但是会被序列化、反序列化到Executor中运行，从Driver被new出来再发送到Executor执行，且RDD不存储数据
那RDD为什么还能persist存储数据？其实persist代表了映射关系，只要有persist，某个RDD的数据就一定能存储到BlockManager里，后者有两个位置可以选：
1。memory 2。disk。什么时候persist的数据会被放到这些载体里面的？Executor执行的时候，RDD向右面流动数据的时候，先截获它可以借火的数据然后给
blockmanager
 */
object Lesson07_rdd_control {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("control").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    sc.setCheckpointDir("./data/ckp")  // ("hdfs://mycluster/ooxx/tmp/")

    // 贴源的
    val data = sc.parallelize(1 to 10)

    // 转换加工的
    val d2rdd = data.map(x => {
      if (x % 2 == 0) {
        ("A", x)
      } else {
        ("B", x)
      }
    })

    // 调优点：只有那些重复使用的RDD才适合调优：缓存结果数据。我们可以调优task中间的RDD
    // d2rdd.cache() // http://192.168.1.102:4040/storage/ 552 B
    //d2rdd.persist(StorageLevel.MEMORY_ONLY_SER) // http://192.168.1.102:4040/storage/ 338.0 B
    d2rdd.persist(StorageLevel.MEMORY_AND_DISK) // 优先Memory，然后Disk，内存空间不够了，才会放到磁盘。persist缓存的是计算结果
    // 权衡：调优： 可靠性和速度。如果结果集来之不易。则需要缓存下来：checkpoint, 把数据集存到更可靠的地方，见上面：sc.setCheckpointDir

    d2rdd.checkpoint() //action算子runJob跑到最后会触发job：// checkpoint会单独出发runJob job2

    // 奇偶分组
    val group = d2rdd.groupByKey()
    group.foreach(println) // job1

    // 奇偶统计
    val kv1 = d2rdd.mapValues(x => 1) // 重复用了d2rdd
    val res1 = kv1.reduceByKey(_ + _)

    res1.foreach(println) // job3

    // 这里有调优，RDD可以调用persist缓存数据，只不过有级别。但是RDD可能不是触发shuffle-write的、task最后那个RDD
    // RDD如果是Task最后的那一个，且shuffle-read之后的结果集被复用的话， 该RDD的执行可以被跳过，其DAG显示为灰色，这里我们无需调优
    val res2 = res1.mapValues(e => (e + " lisz"))
    res2.foreach(println) // job4

    Thread.sleep(Long.MaxValue)
  }
}
