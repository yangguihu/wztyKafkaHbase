package cn.com.wiseweb.gather

import java.text.SimpleDateFormat
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangguihu on 2016/8/22.
  */
object KafkaHbase {
  /**
    * 把字符串格式“yyyy-MM-dd HH:mm:ss” 转换成对应的时间戳
    *
    * @param date
    * @return
    */
  def getStrToTime(date:String):String={
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      String.valueOf(format.parse(date).getTime)
  }

  def main(args: Array[String]) {
//    LoggerLevels.setStreamingLogLevels()
    Logger.getRootLogger.setLevel(Level.WARN)

    //kafak 参数
    val zkQuorum = "node1:2181,node2:2181,node3:2181";
    val group = "kafkahbase";
    val topics = "wiseweb_crawler_paper";
    val numThreads = "5";

    val sparkConf = new SparkConf().setAppName("Kafka-Hbase")
    //获取间隔时间为5秒
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val sc: SparkContext = ssc.sparkContext
    //广播hbase表名和列族
    val broadcastTableName = sc.broadcast("newspaper")
    val broadcastC1 = sc.broadcast("c1")
    val broadcastC2 = sc.broadcast("c2")
    val broadcastJson = sc.broadcast(new ObjectMapper)  //json to map对象

    ssc.checkpoint("hdfs://mycluster/wzty/kafka-hbase-ck")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //读取kafka中的数据
    val data=KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK)
    //设置检查点
    data.checkpoint(Seconds(30))
    //遍历dstream中的rdd
    val words = data.foreachRDD(t=>{

      t.foreachPartition(x=>{   //遍历rdd的每个分区
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3");// zookeeper地址
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        //获取hbase连接
        val connection = HConnectionManager.createConnection(hbaseConf)
        val table = connection.getTable(broadcastTableName.value); //user

        table.setAutoFlush(false,true)
        //创建批量put list
        var arraylist=new util.ArrayList[Put]

        //获取json转换对象
        val mapper = broadcastJson.value
//        var map=Map[String,String]
        var map: util.HashMap[String,String]=null

        val c1 = broadcastC1.value
        val c2 = broadcastC2.value

        //遍历每个分区中的每条消息
        x.foreach(record=>{
          //通过jack对象将json转成map,拼装hbase的插入记录
          map = mapper.readValue(record._2,classOf[util.HashMap[String,String]])
          val urlhash: String = map.get("urlhash")
          val strpubdate=getStrToTime(map.get("publishdate"))

          //添加put  rowkey
          val put=new Put(Bytes.toBytes(urlhash+"_"+strpubdate))

          for(key <- map.keySet().toArray){
            val v=map.get(key)
            if(key == "snapshot"){
              if(StringUtils.isNotBlank(v))
                put.add(Bytes.toBytes(c2), Bytes.toBytes(key.toString), Bytes.toBytes(v.toString))
              else
                put.add(Bytes.toBytes(c2), Bytes.toBytes(key.toString), Bytes.toBytes(""))
            }else{
              if (StringUtils.isNotBlank(v))
                put.add(Bytes.toBytes(c1), Bytes.toBytes(key.toString), Bytes.toBytes(v.toString))
              else
                put.add(Bytes.toBytes(c1), Bytes.toBytes(key.toString), Bytes.toBytes(""))
            }
          }
          //添加进集合
          arraylist.add(put)

        })
        table.put(arraylist)//插入数据
        table.flushCommits()//提交
        //关闭表和连接
        table.close()
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
