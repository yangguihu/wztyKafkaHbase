package cn.com.wiseweb.gathernew

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

import scala.collection.mutable

/**
  * 处理论坛回帖数据，没条回帖内容根据|||划分出楼层
  */
object RepliesMult{

  def main(args: Array[String]): Unit = {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_replies"
    val group = "replies_hbase"
    //获取传入参数，便于调试
    var record="30"
    var interval="62"
    var tableName="replies"
    if(args.length!=0){
      record=args(0)
      interval=args(1)
      tableName=args(2)
    }

    //设置hbase的东西
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.client.write.buffer","10485760")

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(hbaseConf)
    jobConf.set("mapreduce.output.fileoutputformat.outputdir", "/wzty/replies")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val sparkConf = new SparkConf().setAppName("replies")
      .set("spark.streaming.kafka.maxRatePerPartition",record)
//          .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group,
      "fetch.message.max.bytes" -> "10485760"
//      ,"auto.offset.reset" -> "smallest"
    )
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //处理消息
    messages.foreachRDD(rdd=>{
      if (!rdd.isEmpty()) {
        val norrdd=rdd.flatMap(record => {
          //解析，规整数据
          parseSensor(record._2)
        })
        //转换成put并保存
        norrdd.map(tup=>{
          convertToPut(tup)
        }).saveAsHadoopDataset(jobConf)
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def parseSensor(record: String): mutable.HashSet[mutable.Map[String, Any]] = {
    var repliesSet = new mutable.HashSet[mutable.Map[String, Any]]()
    //通过jack对象将json转成map,拼装hbase的插入记录
    var map: JSONObject = new JSONObject()
    try {
      map = JSON.parseObject(record)

      var kafkatime= map.getString("kafkatime")
      var url = map.getString("url")
      var urlhash = map.getString("urlhash")

      //获取回帖
      var replycontents = map.getString("replycontent").split("\\|\\|\\|")

      var floors=replycontents.length+1;
      for (i <- 2 to floors){
        val floorurl=url+"#"+i
        val replycontent = replycontents(i - 2)
        if(replycontent.length!=0){
          var replieMap: mutable.Map[String, Any] = mutable.Map(
            "url" -> floorurl,
            "floorid" -> i,
            "urlhash" -> urlhash,
            "kafkatime" -> kafkatime,
            "replycontent" -> replycontent
          )
          repliesSet += replieMap
        }
      }
      repliesSet
    } catch {
      case x: Exception => {
        println(record)
        println(x.getMessage)
        //出错的话直接返回一个空集合
        repliesSet
      }
    }
  }


  def convertToPut(map: mutable.Map[String, Any]): (ImmutableBytesWritable, Put) = {
    val urlhash = map.getOrElse("urlhash","").toString
    val floorid = map.getOrElse("floorid","").toString
    val rowkey=urlhash+"_"+floorid
    val put = new Put(Bytes.toBytes(rowkey))
    map.map(tup=>{
      put.add(Bytes.toBytes("c1"), Bytes.toBytes(tup._1), Bytes.toBytes(tup._2.toString))
    })
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
