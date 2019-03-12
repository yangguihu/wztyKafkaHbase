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
object KHDZKWeibo {

  //接口定义的字段list
  private val keyList = util.Arrays.asList(
    "id", "aid", "created_at", "mid", "text", "source", "source_type", "geo", "user", "retweeted_status",
    "reposts_count", "comments_count", "attitudes_count", "thumbnail_pic", "original_pic", "attribute",
    "gathertime", "inserttime", "screen_name", "url", "pid", "keywords", "snapshot", "tendency", "sim_id")

  //做成set 唯一不重复
  private val weiboKeySet=new util.HashSet[String](keyList)

  def main(args: Array[String]) {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_weibo"
    val group = "KHDZKWeibo"

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("KHDZKWeibo")
      .set("spark.streaming.kafka.maxRatePerPartition","200")
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("hdfs://mycluster/wzty/KHDZKWeibo-ck")
//    ssc.checkpoint("c://sparkck/metasearch1-local")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播hbase表名和列族
    val broadcastTableName: Broadcast[String] = sc.broadcast("weibo")
    //广播接口定义字段Set
    val bcWeiboKeySet: Broadcast[util.HashSet[String]] = sc.broadcast(weiboKeySet)
    val bcWeiboKeyList = sc.broadcast(keyList)

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
    * @param partion              分区
    * @param broadcastTableName   表名
    * @param bcWeiboKeySet   接口定义字段名
    * @param bcWeiboKeyList
    */
    def processPartition(
          partion: Iterator[(String, String)], broadcastTableName: Broadcast[String],
          bcWeiboKeySet :Broadcast[util.HashSet[String]],bcWeiboKeyList :Broadcast[util.List[String]]):Unit = {
      //遍历rdd的每个分区
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
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

        //遍历每个分区中的每条消息
        partion.foreach(record => breakable{
          //通过jack对象将json转成map,拼装hbase的插入记录
          val map: JSONObject = JSON.parseObject(record._2)
          //获取id并处理 和采集时间，组合成rowkey
          if(!map.containsKey("url")){
            break   //没有进行下一条，跳转至breakable 处
          }

          val urlHash =HashUtil.xxHash(map.get("url")).toString
          val strGather=getStrToTime(map.get("gathertime"))

          if(!"0".equals(urlHash)){
            //添加put  rowkey
            val put=new Put(Bytes.toBytes(urlHash+"_"+strGather))

            for(key <- map.keySet().toArray){
              val v=map.get(key).toString
              key match {
                case "snapshot" => {
                  //现有字段放到列族c1中
                  if (StringUtils.isNotBlank(v))
                    put.add(Bytes.toBytes("c2"), Bytes.toBytes(key.toString), Bytes.toBytes(v))
                  else
                    put.add(Bytes.toBytes("c2"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
                }
                  //纠正错误数据
                case "insertime" => {
                  if (StringUtils.isNotBlank(v))
                    put.add(Bytes.toBytes("c1"), Bytes.toBytes("inserttime"), Bytes.toBytes(v))
                  else
                    put.add(Bytes.toBytes("c1"), Bytes.toBytes("inserttime"), Bytes.toBytes(""))
                }

                case _ => {
                  //现有字段放到列族c1中
                  if (StringUtils.isNotBlank(v))
                    put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(v))
                  else
                    put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
                }
              }
            }

            weiboKeys.removeAll(map.keySet)
            //添加空字段
            for (key <- weiboKeys.toArray){
              key match {
                  //快照字段放在列族2
                case "snapshot" => put.add(Bytes.toBytes("c2"), Bytes.toBytes("snapshot"), Bytes.toBytes(""))
                case "insertime" => put.add(Bytes.toBytes("c1"), Bytes.toBytes("inserttime"), Bytes.toBytes(map.get(key).toString))
                case _ => put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
              }
            }
            //加一个group_id
            put.add(Bytes.toBytes("c1"), Bytes.toBytes("group_id"), Bytes.toBytes("4"))
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
