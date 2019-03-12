package cn.com.wiseweb.gather

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 采用直连的方式来连接
  */
object KafkaHbaseDirect {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

//    val Array(brokers, topics) = args
    val brokers = "node1:9092,node2:9092,node3:9092";
    val topics = "testnew";
    val group = "KafkaHbaseDirect";

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("hdfs://mycluster/wzty/kh-direct-ck")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //广播hbase表名和列族
    val broadcastTableName = sc.broadcast("newspaper3")
    val broadcastC1 = sc.broadcast("c1")
    val broadcastC2 = sc.broadcast("c2")
    val broadcastJson = sc.broadcast(new ObjectMapper)  //json to map对象

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group
      ,"auto.offset.reset" -> "smallest"
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.checkpoint(Seconds(30))

    //遍历dstream中的rdd
    val words = messages.foreachRDD(t=>{

      t.foreachPartition(x=>{   //遍历rdd的每个分区

        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3");// zookeeper地址
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        //获取hbase连接
        val connection = HConnectionManager.createConnection(hbaseConf)
        val table = connection.getTable(broadcastTableName.value); //user

        table.setAutoFlush(false,true)
        //        table.setWriteBufferSize(534534534);
        //创建批量put list
        var arraylist=new util.ArrayList[Put]

        //获取json转换对象
        val mapper = broadcastJson.value
        var map=new util.HashMap[String,String]

        val c1 = broadcastC1.value
        val c2 = broadcastC2.value

        //遍历每个分区中的没条消息
        x.foreach(record=>{
          //通过jack对象将json转成map,拼装hbase的插入记录
          map = mapper.readValue(record._2,classOf[util.HashMap[String,String]])
          //          val urlhash: String = map.get("urlhash")
          //          val publishdate=KafkaHbaseTools.getStrToTime(KafkaHbaseTools.getDate())
          //          val strpubdate=KafkaHbaseTools.getStrToTime(map.get("publishdate"))
          val urlhash = KafkaHbaseTools.getUrlHash
          val strpubdate = KafkaHbaseTools.getStrToTime2

          val author: String = map.get("author")
          val sim_id: String = map.get("sim_id")

          //添加put
          val put=new Put(Bytes.toBytes(urlhash+"_"+strpubdate))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("site_id"), Bytes.toBytes(map.get("site_id")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("site_name"), Bytes.toBytes(map.get("site_name")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("site_url"), Bytes.toBytes(map.get("site_url")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("group_id"), Bytes.toBytes(map.get("group_id")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("title"), Bytes.toBytes(map.get("title")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("content"), Bytes.toBytes(map.get("content")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("url"), Bytes.toBytes(map.get("url")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("urlhash"), Bytes.toBytes(urlhash))
//          put.add(Bytes.toBytes(c1), Bytes.toBytes("publishtime"), Bytes.toBytes(publishtime))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("gathertime"), Bytes.toBytes(map.get("gathertime")))
          put.add(Bytes.toBytes(c1), Bytes.toBytes("inserttime"), Bytes.toBytes(map.get("inserttime")))
          //作者和指纹 可能为空
          if(StringUtils.isNotBlank(author)){
            put.add(Bytes.toBytes(c1), Bytes.toBytes("author"), Bytes.toBytes(author))
          }
          if(StringUtils.isNotBlank(sim_id)){
            put.add(Bytes.toBytes(c1), Bytes.toBytes("sim_id"), Bytes.toBytes(sim_id))
          }
          //添加快照字段
          put.add(Bytes.toBytes(c2), Bytes.toBytes("snapshot"), Bytes.toBytes(map.get("snapshot")))

          //添加进集合
          arraylist.add(put)
        })
        table.put(arraylist)//插入数据
        table.flushCommits()//提交

        table.close()
        connection.close()
      })
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
