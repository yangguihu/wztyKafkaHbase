package cn.com.wiseweb.gather

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.ExecutorService

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
object KHDZKWebpage {

  //接口定义的字段list
  private val keyList = util.Arrays.asList(
    "site_id","site_name","site_url","title","summary","content",
    "url","urlhash","click","reply","source","area","publishtime",
    "gathertime","inserttime","author","tendency","sim_id","snapshot")

  //做成set 唯一不重复
  private val weiboKeySet=new util.HashSet[String](keyList)

  def main(args: Array[String]) {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_webpage"
    val group = "KHDZKWebpage"
    //获取传入参数，便于调试
    var record="150"
    var interval="8"
    var tableName="webpage"
    if(args.length!=0){
      record=args(0)
      interval=args(1)
      tableName=args(2)
    }

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("KHDZKWebpage")
      .set("spark.streaming.kafka.maxRatePerPartition",record)
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(java.lang.Long.valueOf(interval)))

    ssc.checkpoint("hdfs://mycluster/wzty/KHDZKWebpage-ck")
//    ssc.checkpoint("c://sparkck/metasearch1-local")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播hbase表名和列族
    val broadcastTableName: Broadcast[String] = sc.broadcast(tableName)
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
      val connection = HConnectionManager.createConnection(hbaseConf)
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
        var map: JSONObject = new JSONObject(19)
        try {
          map=JSON.parseObject(record._2)
        }catch {
          case x:Exception=>{
            println("=================传递消息不是json================")
            println(record._2)
            println("=================================================")
            println(x.getMessage)
            break //处理下一条
          }
        }
        //获取urlhash并处理 和采集时间，组合成rowkey
        if(!map.containsKey("urlhash")){
          break   //没有进行下一条，跳转至breakable 处
        }

        val urlHash =map.get("urlhash").toString
        val strGather=getStrToTime(map.get("gathertime"))

        if(StringUtils.isNotBlank(urlHash)){
          //添加put  rowkey
          val put=new Put(Bytes.toBytes(urlHash+"_"+strGather))

          for(key <- map.keySet().toArray){
            val v=map.get(key).toString
            key match {
              case "site_id"=> { //根据site_id解析group_id ，在新闻总默认为新闻 1
                val group_id = KafkaHbaseUtils.siteGroupMapping(v,map.get("url"),"1")
                put.add(Bytes.toBytes("c1"), Bytes.toBytes("site_id"), Bytes.toBytes(v))
                put.add(Bytes.toBytes("c1"), Bytes.toBytes("group_id"), Bytes.toBytes(group_id))
              }
              case "url" => { //根据url 解析 domain_1和domain_2
                put.add(Bytes.toBytes("c1"), Bytes.toBytes("url"), Bytes.toBytes(v))
                put.add(Bytes.toBytes("c1"), Bytes.toBytes("domain_1"), Bytes.toBytes(DomainUtils.getRE_TOP1(v)))
                put.add(Bytes.toBytes("c1"), Bytes.toBytes("domain_2"), Bytes.toBytes(DomainUtils.getRE_TOP2(v)))
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
//      connection.close();

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
