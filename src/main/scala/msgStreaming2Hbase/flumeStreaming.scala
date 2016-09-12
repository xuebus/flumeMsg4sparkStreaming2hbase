package msgStreaming2Hbase

import java.net.InetSocketAddress

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Success

/**
  * Created by uul on 16-9-7.
  * 启动方式
  * bin/spark-submit --class msgStreaming2Hbase.flumeStreaming /home/hadoop/sparkstringing2hbase.jar spark://master:7077  /home/hadoop/sparkstringing2hbase.jar  6  10.0.138.222,40333,10.0.138.223,40333,10.0.138.224,40333,10.0.138.225,40333,10.0.138.226,40333,10.0.138.227,40333,10.0.138.228,40333,10.0.138.229,40333
  */
object flumeStreaming {
  var nodes = Seq(
    InetSocketAddress.createUnresolved("10.0.138.222",40333),
    InetSocketAddress.createUnresolved("10.0.138.223",40333),
    InetSocketAddress.createUnresolved("10.0.138.224",40333),
    InetSocketAddress.createUnresolved("10.0.138.225",40333),
    InetSocketAddress.createUnresolved("10.0.138.226",40333),
    InetSocketAddress.createUnresolved("10.0.138.227",40333),
    InetSocketAddress.createUnresolved("10.0.138.228",40333),
    InetSocketAddress.createUnresolved("10.0.138.229",40333)
  )
  var sprakMaster = "spark://master:7077"
  var jars = List("/home/uul/sparkstringing2hbase.jar")
  var time = 6
  def main(args: Array[String]): Unit = {
    init(args)
    val sc = new SparkConf().setAppName("get flume msg ").setMaster(sprakMaster).setJars(jars)
      .set("spark.executor.memory","10g")
    val ssc = new StreamingContext(sc,Seconds(10))
    for (address <- nodes) getFlumeMsg2SparkStream(ssc,address)
    ssc.start
    ssc.awaitTermination
  }


  def getFlumeMsg2SparkStream(ssc:StreamingContext,address:InetSocketAddress): Unit = {
    val flumeStream = FlumeUtils.createStream(ssc,address.getHostString,address.getPort,StorageLevel.MEMORY_AND_DISK_SER)
    flumeStream.map( e => new String(e.event.getBody.array)).map(_.split("\n").map(x => x.split(" "))).
      map(_.map(x=>if (x.length == 3)(x(0)+","+x(1),x(2))else("",""))).map(_(0)).reduceByKey((x,y)=>y).foreachRDD{
        rdd=>rdd.foreachPartition{ partitionOfRecords =>
          val conf = HBaseConfiguration.create
          conf.set("hbase.master", "master:60000")
          conf.set("hbase.zookeeper.property.clientPort", "2181")
          conf.set("hbase.zookeeper.quorum", "master,datanode1,datanode2,datanode3,datanode4,datanode5,datanode6,datanode7")
          conf.addResource("/home/uul/hbase-site.xml")
          val conn = HConnectionManager.createConnection(conf)
          val hTbale:HTableInterface = conn.getTable("test")
          partitionOfRecords.foreach{
            kv=>
              if (kv._2.length!=0&&kv._1.length!=0) {
                var numLong = 1L
                val result = hTbale.getScanner(new Scan(kv._1.substring(0,kv._1.length-1).getBytes,new PageFilter(1)))
                val rs = result.next
                if (rs != null && rs.containsColumn("cf".getBytes,"num".getBytes))
                  numLong = new String(rs.getValue("cf".getBytes,"num".getBytes)).toLong + 1
                result.close
                val put = new Put(kv._1.getBytes)
                put.add("cf".getBytes,"num".getBytes,numLong.toString.getBytes)
                put.add("cf".getBytes,"helo".getBytes,kv._2.getBytes)
                hTbale.put(put)
              }
          }
          hTbale.close
          conn.close
        }
    }
  }

  def init(args:Array[String]): Unit = {
    if (args.length == 0) return
    if (args.length != 4) {
      System.err.println("Usage: getFlumeMsg2SparkStream <Master> <jarFile> <time> <socket*>")
      System.exit(1)
    }
    sprakMaster = args(0)
    jars = List(args(1))
    scala.util.Try(args(2).toInt) match {
      case Success(_) =>
      case _ =>  {
        System.err.println("Parameter err : <time> is Int")
        System.exit(1)
      }
    }
    time = args(2).toInt
    val strs = args(3).split(",")
    if (strs.length%2 == 1) {
      System.err.println("Parameter err : <socket*> not format , length not format")
      System.exit(1)
    }
    for (x <- 0 to strs.length-1 if (x%2 == 1)) scala.util.Try(strs(x).toInt) match {
      case Success(_) =>
      case _ =>  {
        System.err.println("Parameter err : <socket*> not format , port not format")
        System.exit(1)
      }
    }
    nodes = for (x <- 0 to strs.length-1 if (x%2 == 0)) yield InetSocketAddress.createUnresolved(strs(x),strs(x+1).toInt)
  }
}
