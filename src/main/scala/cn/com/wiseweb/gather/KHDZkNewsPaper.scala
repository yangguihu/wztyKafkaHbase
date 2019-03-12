package cn.com.wiseweb.gather

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * 采用直连的方式连接到kafka并将记录放入到zk中
  */
object KHDZkNewsPaper {
  //接口定义的字段list
  private val keyList = util.Arrays.asList(
    "site_id","site_name","site_url","group_id","title","content","url","urlhash",
    "publishtime","gathertime","inserttime","author","sim_id","snapshot"
  )
  //做成set 唯一不重复
  private val weiboKeySet=new util.HashSet[String](keyList)

  def main(args: Array[String]) {
    //val Array(brokers, topics) = args
    val brokers = "node1:9092,node2:9092,node3:9092";
    val topics = "wiseweb_crawler_paper";
    val group = "KafkaHbaseDirect";

    //获取传入参数，便于调试
    var record="150"
    var interval="8"
    var tableName="paper"
    if(args.length!=0){
      record=args(0)
      interval=args(1)
      tableName=args(2)
    }

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("KHDZkNewsPaper")
      .set("spark.streaming.kafka.maxRatePerPartition",record)
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

    ssc.checkpoint("hdfs://mycluster/wzty/kh-direct-ck")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播hbase表名和列族
    val broadcastTableName: Broadcast[String] = sc.broadcast(tableName)
//    val broadcastTableName: Broadcast[String] = sc.broadcast(args(0))
    val bcWeiboKeySet = sc.broadcast(weiboKeySet)
    val bcWeiboKeyList = sc.broadcast(keyList)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group
      //从最新的开始读取
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
          processPartition(it,broadcastTableName,bcWeiboKeySet,bcWeiboKeyList)
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
    * @param partion
    * @param broadcastTableName
    */
  def processPartition(
    partion: Iterator[(String, String)], broadcastTableName: Broadcast[String],
    bcWeiboKeySet :Broadcast[util.HashSet[String]],bcWeiboKeyList :Broadcast[util.List[String]]):Unit = {

      //遍历rdd的每个分区

      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3") // zookeeper地址
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      //获取hbase连接
      val connection = HConnectionManager.getConnection(hbaseConf)
      try{
        val table = connection.getTable(broadcastTableName.value); //user

        table.setAutoFlush(false, true)
        //设置连接20m
        table.setWriteBufferSize(20971520);
        //创建批量put list
        var arraylist = new util.ArrayList[Put]
        //获取接口定义字段名集合
        var weiboKeys=new util.HashSet[String](bcWeiboKeyList.value)

        //遍历每个分区中的没条消息
        partion.foreach(record => {
          var map: JSONObject = new JSONObject(14)
          try {
            map=JSON.parseObject(record._2)
          }catch {
            case x:Exception=>{
              println(x.getMessage)
              break //处理下一条
            }
          }
          val urlhash: String = map.get("urlhash").toString
          val strGather=getStrToTime(map.get("gathertime"))

          //添加put  rowkey
          val put=new Put(Bytes.toBytes(urlhash+"_"+strGather))

          for(key <- map.keySet().toArray){
            val v=map.get(key)
            key match {
              case "snapshot"=> {
                put.add(Bytes.toBytes("c2"), Bytes.toBytes(key.toString), Bytes.toBytes(v.toString))
              }
              case _ => put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(v.toString))
            }
          }
          weiboKeys.removeAll(map.keySet)
          //添加空字段
          for (key <- weiboKeys.toArray){
            key match {
              case "snapshot" => put.add(Bytes.toBytes("c2"), Bytes.toBytes("snapshot"), Bytes.toBytes(""))
              case "group_id" => {
                val group_id: String = KafkaHbaseUtils.siteGroupMapping(map.get("site_id"),map.get("url"),"5")
                put.add(Bytes.toBytes("c1"), Bytes.toBytes("group_id"), Bytes.toBytes(group_id))
              }
              case _ => put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
            }
          }
          //恢复标准
          weiboKeys.addAll(bcWeiboKeySet.value)

          //添加进集合
          arraylist.add(put)
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
  def getStrToTime(date:Object):String={
    try {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        String.valueOf(format.parse(date.toString).getTime)
    } catch {
      //ParseException 解析错误 直接获取当前时间
      case ex:Exception => {
        String.valueOf(new Date().getTime)
      }
    }
  }
}
