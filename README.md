# 使用 scala操作Spark-SQL

## 本项目依赖的第三方包

 [点击下载]( https://pan.baidu.com/s/1QmNHvbCqrTPmCkSnXWoPgQ)，将下载的
 `lib`目录放到项目跟目录下即可。本人亲测的测试环境是:
 
 ```
 spark:1.6.2
 hbase:1.2.1
 ```

## 使用方法

以`com.teclan.scala.Main`为例，将项目（以及依赖，依赖最好导出，有可能环境不一样会导致无法运行）导出成jar包，
拷贝至 spark 的 `classpath`目录下，`classpath`的配置可以参考环境变量或者spark的默认配置文件
（`$SPARK_HOME/conf/spark-defaults.conf`）中找到。在 `$SPARK_HOME/conf/spark-defaults.conf`
文件中，配置的内容大致如下：

```
spark.executor.extraClassPath    /home/hadoop/lib/*
spark.driver.extraClassPath      /home/hadoop/lib/*
```

将导出的 jar 拷贝至 `classpath`之后，在 `$SPARK_HOME`目录下执行

```
bin/spark-submit --master spark://10.0.88.42:7077 --class com.teclan.scala.Main --executor-memory 512m --total-executor-cores 2 /home/hadoop/spark/teclan/teclan-scala.jar hdfs://10.0.88.42:9000/hbase hdfs://10.0.88.42:9000/hbase_out
```

其中：

```
-master 指定集群master，例如  spark://10.0.88.42:7077

--class 指定类所在地址，例如 com.teclan.scala.Main

--executor-memory 512m 指定每个work运行内存为512m

--total-executor-cores 2  指定总共提供2个核处理给所有work

/home/hadoop/spark/teclan/teclan-scala.jar 提供上传的jar包所在目录

hdfs://10.0.88.42:9000/hbase 提供所需分析的文件所在hdfs中的目录

hdfs://10.0.88.42:9000/hbase_out 可选，提供处理完后的文件要放到hdfs中某目录
```
__特别注意：如果是spark集群，并且你的程序不只一个类的话，就需要将你的jar包按照上述提示，拷贝至每个几点的`classpath`下，__
__否则，会报找不到类的异常__
