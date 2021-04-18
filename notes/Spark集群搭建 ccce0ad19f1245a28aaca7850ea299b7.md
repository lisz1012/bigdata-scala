# Spark集群搭建

## 基础设施

1. 节点的配置
2. 部署Hadoop：HDFS、Zookeeper
3. ssh免密钥

## 下载与安装

```java
wget [https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.6.tgz](https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.6.tgz)
tar xf ./spark-2.3.4-bin-hadoop2.6.tgz
mv spark-2.3.4-bin-hadoop2.6 /opt/bigdata
```

## 部署细节

`vim /etc/profile`

```java
export SPARK_HOME=/opt/bigdata/spark-2.3.4-bin-hadoop2.6
```

`. /etc/profile`

`cd $SPARK_HOME`

`cp slaves.template slaves` 删掉 文件最后的“localhost“，并追加：

```java
hadoop-02
hadoop-03
hadoop-04
```

`cp spark-env.sh.template spark-env.sh` 

 `vim spark-env.sh`

```java
export HADOOP_CONF_DIR=/opt/bigdata/hadoop-2.10.0/etc/hadoop
export SPARK_MASTER_HOST=hadoop-01
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=4g
```

`cd /opt/bigdata`

```java
scp -r ./spark-2.3.4-bin-hadoop2.6/ hadoop-02:`pwd`
scp -r ./spark-2.3.4-bin-hadoop2.6/ hadoop-03:`pwd`
scp -r ./spark-2.3.4-bin-hadoop2.6/ hadoop-04:`pwd`
```

ssh到hadoop-02～4，执行： `.  /etc/profile`

## 启动

1. 先在  `hadoop-02` `hadoop-03` `hadoop-04` 上启动ZK，因为要做Hadoop集群的HA（非必须）：

    `zhServer.sh start`

2. 在hadoop-01上启动HDFS： `start-dfs.sh`
3. 在hadoop-01上启动spark： `/opt/bigdata/spark-2.3.4-bin-hadoop2.6/sbin/start-all.sh`

    这样会在配置指定了master的节点上启动一个Master进程，在其他的HDFS节点上启动Worker进程，他们都是Spark的资源层角色，还不代表应用程序跑起来了，什么时候真跑程序了，会多一些进程。访问我们指定的Master节点的Web UI端口号，可以看到页面：[http://hadoop-01:8080](http://hadoop-01:8080/)

 4. `/opt/bigdata/spark-2.3.4-bin-hadoop2.6/bin/spark-shell --master spark://hadoop-01:7077`

注：spark-shell.sh不带参数就直接在交互界面执行命令，就进入了client模式，因为Driver在远程就不会取到当前输入的命令

 5.  验证，执行以下命令统计某个HDFS目录下文件的词频：

```java
scala> sc.textFile("hdfs://mycluster/sparktest/data.txt").flatMap(_.split("\\s+")).map((_*,1)).reduceByKey(_*+_).foreach(println)
```

发现并无结果输出，因为Spark是一个分布式计算框架，计算的逻辑最终会发送到集群里面去， `foreac` 这个算子会作用在每一个分区上，虽然当前数据很小，但他并不在本机，而是在某一个DataNode上，所以这个逻辑实际上是在那个DN上执行的，所以没在本地节点打印，这也是为什么交互模式必须是client模式，Driver必须在当前这个进程。所以还要用回收算子： `collect` 所以要想看到结果，得执行：

```java
scala> sc.textFile("hdfs://mycluster/sparktest/data.txt").flatMap(_*.split("\\s+")).map((_*,1)).reduceByKey(_*+_*).collect().foreach(println)
```

执行的应该比较快，因为之前缓存过，通过： [http://hadoop-01:4040/jobs/](http://hadoop-01:4040/jobs/) 查看，发现第二次的确快很多：

![Spark%E9%9B%86%E7%BE%A4%E6%90%AD%E5%BB%BA%20ccce0ad19f1245a28aaca7850ea299b7/Screen_Shot_2021-04-17_at_7.04.11_PM.png](Spark%E9%9B%86%E7%BE%A4%E6%90%AD%E5%BB%BA%20ccce0ad19f1245a28aaca7850ea299b7/Screen_Shot_2021-04-17_at_7.04.11_PM.png)

我们还可以继续无限写命令，所以Spark要先把JVM Executor资源申请好

兼容性表：[https://docs.qubole.com/en/latest/user-guide/engines/spark/spark-supportability.html](https://docs.qubole.com/en/latest/user-guide/engines/spark/spark-supportability.html)