package cn.com.wiseweb.local

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.com.wiseweb.gather.{DomainUtils, KafkaHbaseUtils}
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
object WebpageNew_local extends Serializable{

  //接口定义的字段list
  private val keyList = util.Arrays.asList(
    "site_id","site_name","site_url","title","summary","content",
    "url","urlhash","click","reply","source","area","publishtime",
    "gathertime","inserttime","author","tendency","sim_id","snapshot")

  //做成set 唯一不重复
  private val weiboKeySet=new util.HashSet[String](keyList)

  def main(args: Array[String]): Unit = {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_webpage"
    val group = "webpagenew_local"
    //获取传入参数，便于调试
    var record="5"
    var interval="8"
    var tableName="webpage1"
    if(args.length!=0){
      record=args(0)
      interval=args(1)
      tableName=args(2)
    }

    //设置hbase的东西
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(hbaseConf)
    jobConf.set("mapreduce.output.fileoutputformat.outputdir", "/home/hadoop/spark_job/out")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("webpagenew_local")
      .set("spark.streaming.kafka.maxRatePerPartition",record)
          .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

//    ssc.checkpoint("hdfs://mycluster/wzty/webpagenew-ck")
//        ssc.checkpoint("c://sparkck/webpagenew-local")
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播接口定义字段Set
    val bcWeiboKeyList = sc.broadcast(keyList)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group
      ,"auto.offset.reset" -> "smallest"
    )
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //处理消息
    messages.foreachRDD(rdd=>{
      if (!rdd.isEmpty()) {
        rdd.map(record => {
          val map = parseSensor(record._2,bcWeiboKeyList)
          println(map)
//          convertToPut(map._1,map._2)
        })
          .collect()
          //.saveAsHadoopDataset(jobConf)

        // 再更新offsets
        //km.updateZKOffsets(rdd)
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
      //println("=================")
      //println(map)
      for(key <- map.keySet().toArray){  //空字段处理
        if(map.get(key.toString)==null){
          map.put(key.toString,"");
        }
      }

      val urlHash = map.get("urlhash")
      val strGather = getStrToTime(map.get("gathertime"))

      var rowkey= " "
      if (urlHash!=null) {//没有urlhash 直接回插入空 且只会插入一条
        rowkey=urlHash + "_" + strGather
      }
      //根据site_id解析group_id ，在新闻总默认为新闻 1
      val group_id = KafkaHbaseUtils.siteGroupMapping(map.get("site_id"), map.get("url"), "1")
      map.put("group_id", group_id)

      //根据url 解析 domain_1和domain_2
      val url = map.get("url").toString
      map.put("domain_1", DomainUtils.getRE_TOP1(url))
      map.put("domain_2", DomainUtils.getRE_TOP2(url))

      //获取接口定义字段名集合
      val weiboKeys=new util.HashSet[String](bcWeiboKeyList.value)
      //println("==========之前=====")
      //println(weiboKeys)
      weiboKeys.removeAll(map.keySet)
      //println("========sfs后=====")
      //println(weiboKeys)
      //添加空字段
      for (key <- weiboKeys.toArray) {
        map.put(key.toString, "")
      }
      //println(map)
      //返回map
      (rowkey,map)
      //map
    } catch {
      case x: Exception => {
        println(record)
        println(x.getMessage)
        break //处理下一条
      }
    }
  }


  def convertToPut(rowkey: String,map: JSONObject): (ImmutableBytesWritable, Put) = {
    //val rowkey=map.get("rowkey").toString
    val put = new Put(Bytes.toBytes(rowkey))
    for(key <- map.keySet().toArray){
      val v=map.get(key).toString
      put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(v))
    }
    (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
  }

  /**
    * 把字符串格式“yyyy-MM-dd HH:mm:ss” 转换成对应的时间戳
    * 也有可能是yyyMMddHHmmss形式
    *
    * @param date
    * @return
    */
  def getStrToTime(date:Object):String={
    try {
      val str=date.toString
      if(str.length <=14){
        val format = new SimpleDateFormat("yyyyMMddHHmmss")
        String.valueOf(format.parse(str).getTime)
      }else{
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        String.valueOf(format.parse(str).getTime)
      }
    } catch {
      //ParseException 解析错误 直接获取当前时间
      case ex:Exception => {
        String.valueOf(new Date().getTime)
      }
    }
  }
}
