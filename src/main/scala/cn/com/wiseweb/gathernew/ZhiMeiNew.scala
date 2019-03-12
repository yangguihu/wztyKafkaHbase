package cn.com.wiseweb.gathernew

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.com.wiseweb.gather.KafkaHbaseUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * 采用直连的方式连接到kafka并将记录放入到zk中
  * 采集元搜索数据到hhbase
  */
object ZhiMeiNew extends Serializable{

  //接口定义的字段list
  private val keyList = util.Arrays.asList(
    "site_id","site_name","site_url","group_id","title","content","url","urlhash",
    "publishtime","gathertime","inserttime","author","sim_id","snapshot")

  def main(args: Array[String]): Unit = {
    //ask-answer blog  mobileclient
    var topicName="blog"
    //获取传入参数，便于调试
    var record="30"
    var interval="65"
    if(args.length==1){
      topicName=args(0)
    }else if(args.length>1){
      topicName=args(0)
      record=args(1)
      interval=args(2)
    }
    var tableName=topicName
    if(args.length==4){
      tableName=args(3)
    }
    val common=topicName
    val brokers = "node1:9092,node2:9092,node3:9092"
    val topics = "wiseweb_crawler_"+topicName
    val group = common+"_hbase"

    //设置hbase的东西
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.client.write.buffer","10485760")

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(hbaseConf)
    jobConf.set("mapreduce.output.fileoutputformat.outputdir", "/wzty/"+topicName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName(common)
      .set("spark.streaming.kafka.maxRatePerPartition",record)
//          .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播接口定义字段Set
    val bcWeiboKeyList = sc.broadcast(keyList)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group,
      "fetch.message.max.bytes" -> "10485760"
      ,"auto.offset.reset" -> "smallest"
    )
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //处理消息
    messages.foreachRDD(rdd=>{
      if (!rdd.isEmpty()) {
        val norrdd =rdd.map(record => {
          parseSensor(record._2,bcWeiboKeyList)
        }).filter(tup=>{
          !" ".equals(tup._1)
        })
        //转换成put并保存
        norrdd.map(tup=>{
          convertToPut(tup._1,tup._2)
        }).saveAsHadoopDataset(jobConf)
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  // function to parse line of sensor data into Sensor class
  def parseSensor(record: String,bcWeiboKeyList :Broadcast[util.List[String]]): (String,JSONObject) = {

    //通过jack对象将json转成map,拼装hbase的插入记录
    var map: JSONObject = new JSONObject()
    try {
      map = JSON.parseObject(record)
      for(key <- map.keySet().toArray){  //空字段处理
        if(map.get(key.toString)==null){
          map.put(key.toString,"")
        }
      }

      var urlHash = map.getString("urlhash")
      //val strGather = getStrToTime(map.get("gathertime"))
      if (urlHash==null) {//没有urlhash 直接回插入空 且只会插入一条
        urlHash=" "
      }
      //根据site_id解析group_id ，在新闻总默认为新闻 1
      //val group_id: String = KafkaHbaseUtils.siteGroupMapping(map.get("site_id"),map.get("url"),"5")
      //map.put("group_id", group_id)

      //获取接口定义字段名集合
      val weiboKeys=new util.HashSet[String](bcWeiboKeyList.value)
      weiboKeys.removeAll(map.keySet)
      //添加空字段
      for (key <- weiboKeys.toArray) {
        key match {
          case "group_id" => {
            val group_id: String = KafkaHbaseUtils.siteGroupMapping(map.get("site_id"),map.get("url"),"5")
            map.put("group_id", group_id)
          }
          case _ => map.put(key.toString, "")
        }
      }
      //返回map
      (urlHash,map)
    } catch {
      case x: Exception => {
        println(record)
        println(x.getMessage)
        (" ",null)
      }
    }
  }


  def convertToPut(rowkey: String,map: JSONObject): (ImmutableBytesWritable, Put) = {
    //val rowkey=map.get("rowkey").toString
    val put = new Put(Bytes.toBytes(rowkey))
    for(key <- map.keySet().toArray){
      val v=map.get(key).toString
      key match {
        case "snapshot" => put.add(Bytes.toBytes("c2"), Bytes.toBytes("snapshot"), Bytes.toBytes(v))
        case _ => put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(v))
      }
    }
    (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
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
