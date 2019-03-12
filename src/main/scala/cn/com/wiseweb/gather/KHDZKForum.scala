package cn.com.wiseweb.gather

import java.text.SimpleDateFormat
import java.util
import java.util.Date

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

import scala.util.control.Breaks._

/**
  * 采用直连的方式连接到kafka并将记录放入到zk中
  * 采集元搜索数据到hhbase
  */
object KHDZKForum {
  //接口定义的字段list
  private val keyList = util.Arrays.asList("Id","author","click","content","domain_1",
    "domain_2","gathertime","group_id","inserttime","publishtime","reply","replycontent",
    "reposts_count","site_id","site_name","site_url","title","url","urlhash");

  //做成set 唯一不重复
  private val weiboKeySet=new util.HashSet[String](keyList)

  def main(args: Array[String]) {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_forum1"
    val group = "KHDZKForum1"
    //获取传入参数，便于调试
    var record="150"
    var interval="6"
    var tableName="forum1"
    //5 m
    var writeBuffer="5242880"
    var fetchs="20971520"
    if(args.length!=0){
      record=args(0)
      interval=args(1)
      tableName=args(2)
      writeBuffer=args(3)
      fetchs=args(4)
    }

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("KHDZKForum1")
      .set("spark.streaming.kafka.maxRatePerPartition",record)
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

    ssc.checkpoint("hdfs://mycluster/wzty/KHDZKForum1-ck")
//    ssc.checkpoint("c://sparkck/KHDZKForum-local")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播hbase表名和列族
    val broadcastTableName: Broadcast[String] = sc.broadcast(tableName)
    //恢复标准
    val bcWeiboKeySet = sc.broadcast(weiboKeySet)
    //能够操作的字段集合
    val bcWeiboKeyList = sc.broadcast(keyList)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group,
      //默认最大单条消费 20m 生产者设置的是20m
      "fetch.message.max.bytes"-> fetchs
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
          processPartition(it,broadcastTableName,bcWeiboKeySet,bcWeiboKeyList,writeBuffer)
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
    * @param bcWeiboKeySet   接口定义字段名（恢复标准）
    * @param bcWeiboKeyList
    */
    def processPartition(
          partion: Iterator[(String, String)], broadcastTableName: Broadcast[String],
          bcWeiboKeySet :Broadcast[util.HashSet[String]],bcWeiboKeyList :Broadcast[util.List[String]],
          writeBuffer: String):Unit = {
      //遍历rdd的每个分区
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      //获取hbase连接
      val connection = HConnectionManager.getConnection(hbaseConf)
      try{
        val table = connection.getTable(broadcastTableName.value); //user
        table.setAutoFlush(false, true)
        //设置连接5m
        table.setWriteBufferSize(java.lang.Long.valueOf(writeBuffer));
        //table.setWriteBufferSize(20971520);
        //创建批量put list
        var arraylist = new util.ArrayList[Put]

        //获取接口定义字段名集合
        var weiboKeys=new util.HashSet[String](bcWeiboKeyList.value)

        //遍历每个分区中的每条消息
        partion.foreach(record => breakable{
          //通过jack对象将json转成map,拼装hbase的插入记录
          var map: JSONObject = new JSONObject(19)
          try {
            map=JSON.parseObject(record._2)
          }catch {
            case x:Exception=>{
              println(x.getMessage)
              break //处理下一条
            }
          }
          //获取id并处理 和采集时间，组合成rowkey
          if(!map.containsKey("urlhash")){
            break   //没有进行下一条，跳转至breakable 处
          }
          val urlhash = map.get("urlhash").toString
          val strGather=getStrToTime(map.get("gathertime"))

          if(StringUtils.isNotBlank(urlhash)){
            //添加put  rowkey
            val put=new Put(Bytes.toBytes(urlhash+"_"+strGather))

            for(key <- map.keySet().toArray){
              val v=map.get(key).toString
              //现有字段放到列族c1中
              put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(v))
            }
            weiboKeys.removeAll(map.keySet)
            //添加空字段
            for (key <- weiboKeys.toArray){
              put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
            }
            //恢复标准
            weiboKeys.addAll(bcWeiboKeySet.value)
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
