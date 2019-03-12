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
object KHDZKWeiboUser {
  //接口定义的字段list
  private val keyList = util.Arrays.asList(
    "id","aid","screen_name","name","province","city","description","location",
    "url","profile_image_url","cover_image_phone","gender","domain","followers_count",
    "friends_count","statuses_count","pagefriends_count","bi_followers_count",
    "favourites_count","following","allow_all_act_msg","allow_all_comment",
    "geo_enabled","remark","avatar_large","avatar_hd","verified","verified_type",
    "verified_contact_email","verified_contact_mobile","verified_contact_name",
    "verified_level","verified_state","verified_reason","verified_reason_modified",
    "verified_reason_url","verified_trade","verified_source","verified_source_url",
    "follow_me","mbtype","mbrank","level","extend","badge","badge_top","created_at",
    "credit_score","lang","star","type","containerid","attribute","gathertime","inserttime")

  //做成set 唯一不重复
  private val weiboKeySet=new util.HashSet[String](keyList)

  def main(args: Array[String]) {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_weibouser"
    val group = "KHDZKWeiboUser"

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("KHDZKWeiboUser")
      .set("spark.streaming.kafka.maxRatePerPartition","200")
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("hdfs://mycluster/wzty/KHDZKWeiboUser-ck")
//    ssc.checkpoint("c://sparkck/metasearch1-local")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播hbase表名和列族
    val broadcastTableName: Broadcast[String] = sc.broadcast("weibouser")
    //恢复标准
    val bcWeiboKeySet = sc.broadcast(weiboKeySet)
    //能够操作的字段集合
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
    * @param bcWeiboKeySet   接口定义字段名（恢复标准）
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
          if(!map.containsKey("id")){
            break   //没有进行下一条，跳转至breakable 处
          }
          val id = map.get("id").toString
          val strGather=getStrToTime(map.get("gathertime"))

          if(StringUtils.isNotBlank(id)){
            //添加put  rowkey
            val put=new Put(Bytes.toBytes(id+"_"+strGather))

            for(key <- map.keySet().toArray){
              val v=map.get(key).toString
              key match {
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
                case "insertime" => put.add(Bytes.toBytes("c1"), Bytes.toBytes("inserttime"), Bytes.toBytes(map.get(key).toString))
                case _ => put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
              }
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
