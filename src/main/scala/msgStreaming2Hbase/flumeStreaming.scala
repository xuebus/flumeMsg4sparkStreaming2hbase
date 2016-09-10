package msgStreaming2Hbase

import java.net.InetSocketAddress

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by uul on 16-9-7.
  */
object flumeStreaming {
  val nodes = Seq(
    InetSocketAddress.createUnresolved("10.0.138.222",40333),
    InetSocketAddress.createUnresolved("10.0.138.223",40333),
    InetSocketAddress.createUnresolved("10.0.138.224",40333),
    InetSocketAddress.createUnresolved("10.0.138.225",40333),
    InetSocketAddress.createUnresolved("10.0.138.226",40333),
    InetSocketAddress.createUnresolved("10.0.138.227",40333),
    InetSocketAddress.createUnresolved("10.0.138.228",40333),
    InetSocketAddress.createUnresolved("10.0.138.229",40333)
  )
  val sprakMaster = "spark://master:7077"
  val jars = List("/home/uul/sparkstringing2hbase.jar")
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("get flume msg ").setMaster(sprakMaster).setJars(jars)
      .set("spark.executor.memory","10g")
    val ssc = new StreamingContext(sc,Seconds(10))
    for (address <- nodes) getFlumeMsg2SparkStream(ssc,address)
    ssc.start
    ssc.awaitTermination
  }


  def getFlumeMsg2SparkStream(ssc:StreamingContext,address:InetSocketAddress) = {
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
}
