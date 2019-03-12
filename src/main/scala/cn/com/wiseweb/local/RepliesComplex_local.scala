package cn.com.wiseweb.local

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.com.wiseweb.util.SelfConf
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class HbaseConfSensor(hbaseConf: Configuration)
case class JobConfSensor(jobConf: JobConf)


/**
  * Created by root on 2016/5/14.
  */
object RepliesComplex_local {
  def main(args: Array[String]) {
    //非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("Test")
      .setMaster("local")
      //.registerKryoClasses(Array(classOf[Configuration], classOf[JobConf]))

    //.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //设置日志级别
    sc.setLogLevel("WARN")

    //设置hbase的东西
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    //var hbsensor = HbaseConfSensor(hbaseConf)
    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new SelfConf(hbaseConf)

    jobConf.set("mapreduce.output.fileoutputformat.outputdir", "/wzty/test")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test")


    val rdd = sc.textFile("data/test.txt")
//    rdd.count()
//    println(rdd)
    if (!rdd.isEmpty()) {
      //var sc: SparkContext = rdd.context
      val setrdd= rdd.flatMap(record => {
        println(record)
        //解析，规整数据
        parseSensor(record)
      })
//      setrdd.map(convertToPut(_)).saveAsHadoopDataset(jobConf)
    }
    sc.stop()
  }
  // function to parse line of sensor data into Sensor class
  def parseSensor(record: String):ArrayBuffer[java.util.HashMap[String,Object]] = {

    //通过jack对象将json转成map,拼装hbase的插入 记录
    var map: JSONObject = new JSONObject()
    var arr = ArrayBuffer[java.util.HashMap[String,Object]]()
    try {
      map = JSON.parseObject(record)
      var inserttime= map.getString("inserttime")
      var url = map.getString("url")
      var urlhash = map.getString("urlhash")

      var replycontents = map.getString("replycontent").split("\\|\\|\\|")

      var floors=replycontents.length+1;
      for (i <- 2 to floors){
        val flourl=url+"#"+i
        val repMap=new java.util.HashMap[String,Object]()
        repMap.put("url" , flourl)
        repMap.put("floorid" , Integer.valueOf(i))
        repMap.put("urlhash" , urlhash)
        repMap.put("replycontent" , replycontents(i-2))

        arr += repMap
      }
      println(arr)
      arr
    } catch {
      case x: Exception => {
        println(record)
        println(x.getMessage)
      }
        return arr
    }
  }

  def convertToPut(map: java.util.Map[String,Object]): (ImmutableBytesWritable, Put) = {
      var rowkey = map.get("urlhash").toString
      val put = new Put(Bytes.toBytes(rowkey))
      for(key <- map.keySet.toArray){
        val v = map.get(key).toString
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
