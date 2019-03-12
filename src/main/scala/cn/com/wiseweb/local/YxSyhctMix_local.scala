package cn.com.wiseweb.local

import java.text.SimpleDateFormat
import java.util
import java.util.Date

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

/**
  * 同事采集yixing和homepagehct（首页火车头）topic的数据
  * 火车头数据在传递过来的时候group_id默认给了0  0为首页  非0为铱星
  */
object YxSyhctMix_local{

  //接口定义的字段list 铱星
  private val yxkeyList = util.Arrays.asList(
    "group_id","layer","click","tendency","site_name","url","content",
    "inserttime","urlhash","title","source","publishtime","gathertime","reply")

  def main(args: Array[String]): Unit = {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_yixing,wiseweb_hct_homepage"
    val group = "yxsyhctmix_local"
    //获取传入参数，便于调试
    var record="3"
    var interval="30"
    if(args.length!=0){
      record=args(0)
      interval=args(1)
    }

    //设置hbase的东西
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置10m的本地缓存，减少rpc通信次数
    hbaseConf.set("hbase.client.write.buffer","10485760")

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val yxConf = new JobConf(hbaseConf)
    yxConf.set("mapreduce.output.fileoutputformat.outputdir", "/wzty/yixing")
    yxConf.setOutputFormat(classOf[TableOutputFormat])
    yxConf.set(TableOutputFormat.OUTPUT_TABLE, "test2")

    val syConf = new JobConf(hbaseConf)
    syConf.set("mapreduce.output.fileoutputformat.outputdir", "/wzty/homepage_hct")
    syConf.setOutputFormat(classOf[TableOutputFormat])
    syConf.set(TableOutputFormat.OUTPUT_TABLE, "test3")

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("yxsyhctmix")
      .set("spark.streaming.kafka.maxRatePerPartition",record)
          .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播接口定义字段Set
    val bcYxkeyList = sc.broadcast(yxkeyList)

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
          parseSensor(record._2,bcYxkeyList)
        }).filter(tup=>{
          !" ".equals(tup._1)
        })
        //首页
        norrdd.filter(tup=>tup._3=="0").map(tup=>{

          println(tup)
          convertToPut(tup._1,tup._2)
        }).saveAsHadoopDataset(syConf)

        //yixing
        norrdd.filter(tup=>tup._3!="0").map(tup=>{
          println(tup)
          convertToPut(tup._1,tup._2)
        }).saveAsHadoopDataset(yxConf)
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  // function to parse line of sensor data into Sensor class
  def parseSensor(record: String,bcYxkeyList :Broadcast[util.List[String]]): (String,JSONObject,String) = {

    //通过jack对象将json转成map,拼装hbase的插入记录
    var map: JSONObject = new JSONObject()
    var groupid= ""
    try {
      map = JSON.parseObject(record)
      for(key <- map.keySet().toArray){  //空字段处理
        if(map.get(key.toString)==null){
          map.put(key.toString,"");
        }
      }
      groupid = map.getString("group_id")

      val urlHash = map.get("urlhash")
      var rowkey=" "

      if(groupid.equals("0")){
         val strInsert = map.get("inserttime")
         if (urlHash!=null) {//没有urlhash 直接回插入空 且只会插入一条
           rowkey=urlHash + "_" + strInsert
         }
      }else{ //yixing
        val strGather = getStrToTime(map.get("gathertime"))

        if (urlHash!=null) {//没有urlhash 直接回插入空 且只会插入一条
          rowkey=urlHash + "_" + strGather
        }
        //获取接口定义字段名集合
        val weiboKeys=new util.HashSet[String](bcYxkeyList.value)
        weiboKeys.removeAll(map.keySet)
        //添加空字段
        for (key <- weiboKeys.toArray) {
          map.put(key.toString, "")
        }
      }
      //返回map
      (rowkey,map,groupid)
      //map
    } catch {
      case x: Exception => {
        println(record)
        println(x.getMessage)
        (" ",null,groupid)
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
