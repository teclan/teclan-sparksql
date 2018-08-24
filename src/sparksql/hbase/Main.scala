package sparksql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Calendar

object Main {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("spark://10.0.88.42:7077").setAppName("teclan");
    var zookeeperClientPort = "2181";
    var zookeeperQuorum = "10.0.88.42";

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    var student = sqlContext.read.format("sparksql.hbase").options(Map(
      "sparksql_table_schema" -> "(id string, name string,cls_id string,created_at string)",
      "hbase_table_name" -> "student1",
      "hbase_table_schema" -> "(stu:id , stu:name,stu:cls_id,stu:created_at)",
      "zookeeper_clientPort" -> zookeeperClientPort,
      "zookeeper_quorum" -> zookeeperQuorum)).load()
    student.printSchema()
    student.registerTempTable("student")

    var cls = sqlContext.read.format("sparksql.hbase").options(Map(
      "sparksql_table_schema" -> "(id string, name string,created_at string)",
      "hbase_table_name" -> "cls",
      "hbase_table_schema" -> "(cls:id , cls:name,cls:created_at)",
      "zookeeper_clientPort" -> zookeeperClientPort,
      "zookeeper_quorum" -> zookeeperQuorum)).load()
    cls.printSchema()
    cls.registerTempTable("cls")

    var begin = Calendar.getInstance.getTimeInMillis.toInt;
    var stuRecords = sqlContext.sql("SELECT * from student limit 10");
    var end = Calendar.getInstance.getTimeInMillis.toInt;
    println("\n======== SELECT * from student，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    stuRecords.show(10);

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    var stuCount = sqlContext.sql("SELECT count(*) from student ");
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("\n======== SELECT count(*) from student，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    stuCount.show(1);

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    var clsRecords = sqlContext.sql("SELECT * from cls limit 10");
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("\n======== SELECT * from cls，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    clsRecords.show(10);

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    var clsCount = sqlContext.sql("SELECT count(*) from cls");
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("\n======== SELECT count(*) from cls，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    clsCount.show(1);

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    var joinRecords = sqlContext.sql("SELECT student.*,cls.name as class_name from student left join cls where student.cls_id=cls.id limit 10");
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("\n======== 联合查询，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    joinRecords.show(10);

    begin = Calendar.getInstance.getTimeInMillis.toInt;
    var joinCount = sqlContext.sql("SELECT count(student.id) from student left join cls where student.cls_id=cls.id");
    end = Calendar.getInstance.getTimeInMillis.toInt;
    println("\n======== 联合统计，耗时（秒）: " + ((end - begin) * 1.0 / 1000))
    joinCount.show(1);

  }
}