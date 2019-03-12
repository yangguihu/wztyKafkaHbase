package cn.com.wiseweb.gather

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 采用直连的方式连接到kafka 并将记录放入到zk中
  * 采集铱星平台的数据到hhbase
  * 对应的hbase 表为yixing  列族只有c1
  */
object KHDZKYixing {
  def main(args: Array[String]) {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_yixing"
    val group = "KHDZKYixing"

    // Create context with 5 second batch interval
    //每秒每个分区拉取的数据量，防止第一次过大
    val sparkConf = new SparkConf().setAppName("KHDZKYixing")
      .set("spark.streaming.kafka.maxRatePerPartition","200")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("hdfs://mycluster/wzty/KHDZKYixing-ck")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播hbase表名和列族
    val broadcastTableName: Broadcast[String] = sc.broadcast("yixing")
    val broadcastC1: Broadcast[String] = sc.broadcast("c1")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group
      ,"auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 遍历每个分区
        rdd.foreachPartition(it=>{
          //处理消息记录
          processPartition(it,broadcastTableName,broadcastC1)
        })
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 处理每个分区的消息，遍历分区创建hbase连接批量发送消息
    *
    * @param partion              分区
    * @param broadcastTableName   表名
    * @param broadcastC1          列族1
    */
  def processPartition(
      partion: Iterator[(String, String)], broadcastTableName: Broadcast[String],
      broadcastC1: Broadcast[String]):Unit = {
      //遍历rdd的每个分区
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      //获取hbase连接
      val connection = HConnectionManager.getConnection(hbaseConf)

      try{
        val table = connection.getTable(broadcastTableName.value); //user

        table.setAutoFlush(false, true)
        //创建批量put list
        var arraylist = new util.ArrayList[Put]
        //获取json转换对象
        val c1 = broadcastC1.value
        //遍历每个分区中的每条消息
        partion.foreach(record => {
          //通过jack对象将json转成map,拼装hbase的插入记录
          val map: JSONObject = JSON.parseObject(record._2)
          //urlhash 和 gathertime 采集时间，组合成rowkey
          val urlhash:String=map.get("urlhash").toString.trim
          val strGather=getStrToTime(map.get("gathertime").toString)

          if(StringUtils.isNotBlank(urlhash) && !"0".equals(strGather)){
            //添加put  rowkey
            val put=new Put(Bytes.toBytes(urlhash+"_"+strGather))

            for(key <- map.keySet().toArray){
              val v=map.get(key).toString
              if (StringUtils.isNotBlank(v))
                put.add(Bytes.toBytes(c1), Bytes.toBytes(key.toString), Bytes.toBytes(v))
              else
                put.add(Bytes.toBytes(c1), Bytes.toBytes(key.toString), Bytes.toBytes(""))
            }
            //添加进集合
            arraylist.add(put)
          }
        })

        table.put(arraylist) //插入数据
        table.flushCommits() //提交

        table.close()
      }catch {
        case x:Exception=>{
          x.printStackTrace()
          connection.close()
        }
      }
  }

  /**
    * 把字符串格式“yyyy-MM-dd HH:mm:ss” 转换成对应的时间戳
    *
    * @param date
    * @return
    */
  def getStrToTime(date:String):String={
    try {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      String.valueOf(format.parse(date).getTime)
    } catch {
      //ParseException 解析错误 直接返回“0”
      case ex:Exception => {
        return "0"
      }
    }
  }
}
