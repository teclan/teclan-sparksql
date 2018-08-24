package com.teclan.scala

import java.util.Calendar

import scala.reflect.runtime.universe

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Main {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("spark://10.0.88.42:7077").setAppName("teclan");
    val hBaseConf = HBaseConfiguration.create();

    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181");
    hBaseConf.set("hbase.zookeeper.quorum", "10.0.88.42");

    analyzerSimpleTable(sparkConf, hBaseConf);

    // leftJoin(sparkConf, hBaseConf);

  }

  //  def leftJoin(sparkConf: SparkConf, hBaseConf: Configuration): Unit = {
  //
  //    var stable = "student";
  //    var sfamily = "stu";
  //
  //    var ctable = "cls";
  //    var cfamily = "cls";
  //
  //    hBaseConf.set(TableInputFormat.INPUT_TABLE, stable);
  //
  //    // 创建 spark context
  //    val sc = new SparkContext(sparkConf);
  //    val sqlContext = new SQLContext(sc);
  //
  //    // 从数据源获取数据
  //    var sHbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
  //
  //    import sqlContext.implicits._
  //
  //    var begin = Calendar.getInstance.getTimeInMillis.toInt;
  //    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
  //    val sd = sHbaseRDD.map(r => (
  //      Bytes.toString(r._2.getValue(Bytes.toBytes(sfamily), Bytes.toBytes("id"))),
  //      Bytes.toString(r._2.getValue(Bytes.toBytes(sfamily), Bytes.toBytes("name"))),
  //      Bytes.toString(r._2.getValue(Bytes.toBytes(sfamily), Bytes.toBytes("cls_id"))) //, Bytes.toString(r._2.getValue(Bytes.toBytes(family), Bytes.toBytes("createdAt")))
  //      ))
  //      .toDF("id", "name", "cls_id");
  //    sd.registerTempTable(stable)
  //    var end = Calendar.getInstance.getTimeInMillis.toInt;
  //    println("\n======== student 将数据映射为表，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
  //
  //    hBaseConf.set(TableInputFormat.INPUT_TABLE, ctable);
  //
  //    // 从数据源获取数据
  //    var cHbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
  //
  //    import sqlContext.implicits._
  //
  //    begin = Calendar.getInstance.getTimeInMillis.toInt;
  //    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
  //    val cd = sHbaseRDD.map(r => (
  //      Bytes.toString(r._2.getValue(Bytes.toBytes(cfamily), Bytes.toBytes("id"))),
  //      Bytes.toString(r._2.getValue(Bytes.toBytes(cfamily), Bytes.toBytes("name"))),
  //      Bytes.toString(r._2.getValue(Bytes.toBytes(cfamily), Bytes.toBytes("created_at")))))
  //      .toDF("id", "name", "created_at");
  //    sd.registerTempTable(ctable)
  //    end = Calendar.getInstance.getTimeInMillis.toInt;
  //    println("\n======== cls 将数据映射为表，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
  //
  //    begin = Calendar.getInstance.getTimeInMillis.toInt;
  //    val df = sqlContext.sql("select * from student left join cls on student.cls_id=cls.id ")
  //    end = Calendar.getInstance.getTimeInMillis.toInt;
  //    println("\n======== 获取所有表数据，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
  //    println("================================ ")
  //    df.show(10);
  //    println("================================ ")
  //
  //  }

  def analyzerSimpleTable(sparkConf: SparkConf, hBaseConf: Configuration): Unit = {

    var table = "student";
    var family = "stu";
    var offset = 50000;

    hBaseConf.set(TableInputFormat.INPUT_TABLE, table);

    // 创建 spark context
    val sc = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sc);

    // 从数据源获取数据
    // var hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
    var hbaseRDD = sqlContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);

    import sqlContext.implicits._

    var begin = Calendar.getInstance.getTimeInMillis.toInt;
    // 将数据映射为表  也就是将 RDD转化为 dataframe schema

    var d = sqlContext.read.format("com.teclan.sparksql.hbase").options(Map(
      "sparksql_table_schema" -> "(row_key string, id string, name string, cls_id string,created_at string)",
      "hbase_table_name" -> "student",
      "hbase_table_schema" -> "(:key , stu:id , stu:name , stu:cls_id,stu:created_at )")).load()

    d.registerTempTable(table)

    var end = Calendar.getInstance.getTimeInMillis.toInt;

    println("\n======== 将数据映射为表，耗时（秒）: " + ((end - begin) * 1.0 / 1000))

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    val df = sqlContext.sql("select * from " + table)
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("\n======== 获取所有表数据，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    println("================================ ")
    df.show(10);
    //    df.collect().foreach(println(_) + "\n")
    println("================================ ")

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    val df1 = sqlContext.sql("select count(*) from " + table)
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("================================ ")
    df1.collect().foreach("=======记录总数:" + println(_));
    println("\n======== 查询记录总数，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    println("================================ ")

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    val df2 = sqlContext.sql("select count(*) from " + table + " where id>" + offset)
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("================================ ")
    df2.collect().foreach("======= id > " + offset + ",记录数:" + println(_));
    println("\n======== 查询 id > " + offset + " 记录数，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    println("================================ ")

  }

}