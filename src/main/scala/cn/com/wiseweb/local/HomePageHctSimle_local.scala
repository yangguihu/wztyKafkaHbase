package cn.com.wiseweb.local

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 简单处理火车头采集首页
  */
object HomePageHctSimle_local {

  def main(args: Array[String]): Unit = {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_hct_homepage"
    val group = "HomePageHct_local"
    //获取传入参数，便于调试
    var record="15"
    var interval="8"
    var tableName="test"
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
    jobConf.set("mapreduce.output.fileoutputformat.outputdir", "/wzty/homepage_hct")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("HomePageHct_local")
      .set("spark.streaming.kafka.maxRatePerPartition",record)
          .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")

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
        //解析并过滤错误数据
        val norrdd =rdd.map(record => {
          parseSensor(record._2)
        }).filter(tup=>{
          !" ".equals(tup._1)
        })
        //转换成put并保存
        norrdd.map(tup=>{
          println(tup)
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
  def parseSensor(record: String): (String,JSONObject) = {

    //通过jack对象将json转成map,拼装hbase的插入记录
    var map: JSONObject = new JSONObject()
    try {
      map = JSON.parseObject(record)
      for(key <- map.keySet().toArray){  //空字段处理
        if(map.get(key.toString)==null){
          map.put(key.toString,"");
        }
      }

      val urlHash = map.get("urlhash")
      //val strInsert = getStrToTime(map.get("inserttime"))
      val strInsert = map.get("inserttime")

      var rowkey= " "
      if (urlHash!=null) {//没有urlhash 直接回插入空 且只会插入一条
        rowkey=urlHash + "_" + strInsert
      }
      //返回map
      (rowkey,map)
    } catch {
      case x: Exception => {
        println(record)
        println(x.getMessage)
        (" ",null)
      }
    }
  }


  def convertToPut(rowkey: String,map: JSONObject): (ImmutableBytesWritable, Put) = {
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
