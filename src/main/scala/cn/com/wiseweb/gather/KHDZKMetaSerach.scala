package cn.com.wiseweb.gather

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 采用直连的方式连接到kafka并将记录放入到zk中
  * 采集元搜索数据到hhbase
  */
object KHDZKMetaSerach {
  //实际中是int类型
  val sengineMap =Map(
    "1" -> "百度","2"-> "360搜索","3"-> "搜狗","4"-> "中国搜索","5"-> "中搜","6"-> "天涯社区","7"-> "凯迪社区",
  "8"-> "必应","9"-> "红网","10"-> "西祠","11"-> "强国论坛","12"-> "新华社区","13"-> "中新搜索","14"-> "新浪搜索",
  "15"-> "博客中国","16"-> "腾讯博客","17"-> "必应(英文)","18"-> "谷歌","19"-> "谷歌(英文)","20"-> "雅虎(英文)","21"-> "Rambler(俄文)")
  //实际中是int类型
  val stypeMap= Map("0" -> "1", "1" -> "2", "2" -> "0", "3" -> "11", "4" -> "3", "5" -> "4")

  def main(args: Array[String]) {
    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
    val topics = "wiseweb_crawler_metasearch"
    val group = "KHDZKMetaSerach"

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("KHDZKMetaSerach")
      .set("spark.streaming.kafka.maxRatePerPartition","200")
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("hdfs://mycluster/wzty/KHDZKMetaSerach-ck")
//    ssc.checkpoint("c://sparkck/metasearch1-local")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val sc: SparkContext = ssc.sparkContext
    //设置日志级别
    sc.setLogLevel("WARN")
    //广播hbase表名和列族
    val broadcastTableName: Broadcast[String] = sc.broadcast("metasearch")
    //广播映射map   sengine  --> metasearch  | stype --> groupID
    val broadcastSengineMap: Broadcast[Map[String, String]] = sc.broadcast(sengineMap)
    val broadcastStypeMap: Broadcast[Map[String, String]] = sc.broadcast(stypeMap)

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
          processPartition(it,broadcastTableName,broadcastSengineMap,broadcastStypeMap)
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
    * @param broadcastSengineMap   表名
    * @param broadcastStypeMap   表名
    */
    def processPartition(
          partion: Iterator[(String, String)], broadcastTableName: Broadcast[String],
          broadcastSengineMap :Broadcast[Map[String, String]],broadcastStypeMap :Broadcast[Map[String, String]]):Unit = {
      //获取hbase连接
      //遍历rdd的每个分区
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      val connection = HConnectionManager.getConnection(hbaseConf)

      try{
        val table = connection.getTable(broadcastTableName.value); //user
        table.setAutoFlush(false, true)
        //设置连接20m
        table.setWriteBufferSize(20971520);
        //创建批量put list
        var arraylist = new util.ArrayList[Put]

        //共享添加字段
        var rows=new util.HashSet[String](util.Arrays.asList("content","click","reply","author","tendency","sim_id","snapshot"))
        //添加字段恢复标准
        val rows1=new util.HashSet[String](util.Arrays.asList("content","click","reply","author","tendency","sim_id","snapshot"))

        //获取映射map
        val stypeMap: Map[String, String] = broadcastStypeMap.value
        val sengineMap: Map[String, String] = broadcastSengineMap.value

        //遍历每个分区中的每条消息
        partion.foreach(record => {
          //通过jack对象将json转成map,拼装hbase的插入记录
          val map: JSONObject = JSON.parseObject(record._2)
          //获取id并处理 和采集时间，组合成rowkey
          val urlHash:String=getUrlHash(map.get("id").toString)
          val strGather=getStrToTime(map.get("gathertime").toString)

          if(StringUtils.isNotBlank(urlHash) && !"0".equals(strGather)){
            //添加put  rowkey
            val put=new Put(Bytes.toBytes(urlHash+"_"+strGather))

            for(key <- map.keySet().toArray){
              val v=map.get(key).toString
              //现有字段放到列族c1中
              if (StringUtils.isNotBlank(v))
                put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(v))
              else
                put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
              //累加器中
              if(rows.contains(key.toString)){
                rows.remove(key.toString)
              }

              key match{
                //添加domain_1 和 domain_2
                case "url" => {
                  put.add(Bytes.toBytes("c1"), Bytes.toBytes("domain_1"), Bytes.toBytes(DomainUtils.getRE_TOP1(v)))
                  put.add(Bytes.toBytes("c1"), Bytes.toBytes("domain_2"), Bytes.toBytes(DomainUtils.getRE_TOP2(v)))
                }
                  //根据stype  添加group_id
                case "stype" => put.add(Bytes.toBytes("c1"), Bytes.toBytes("group_id"), Bytes.toBytes(stypeMap.getOrElse(v,"0")))
                //根据sengine 添加metasearch
                case "sengine" => put.add(Bytes.toBytes("c1"), Bytes.toBytes("metasearch"), Bytes.toBytes(sengineMap.getOrElse(v,"")))
                case _ =>
              }
            }
            //添加空字段
            for (key <- rows.toArray()){
              if("snapshot"!=key.toString)
                put.add(Bytes.toBytes("c1"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
              else
                put.add(Bytes.toBytes("c2"), Bytes.toBytes(key.toString), Bytes.toBytes(""))
            }
            //恢复标准
            rows.addAll(rows1)

            //添加urlhash
            put.add(Bytes.toBytes("c1"), Bytes.toBytes("urlhash"), Bytes.toBytes(urlHash))
            //添加inserttime
            val inserttime=KafkaHbaseTools.getDate()
            put.add(Bytes.toBytes("c1"), Bytes.toBytes("inserttime"), Bytes.toBytes(inserttime))

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
  def getStrToTime(date:String):String={
    try {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      String.valueOf(format.parse(date).getTime)
    } catch {
      //ParseException 解析错误 直接返回“0”
      case ex:Exception => {
        return "0"
      }
    }
  }

  /**
    * 置换id 解决数据倾斜问题
    *
    * @param id
    * @return
    */
  def getUrlHash(id:String):String={
    if(id.isEmpty){
      return ""
    }
    val ids: Array[String] = id.split("-")
    if(ids.length==2){
      return ids(1)
    }else{
      return id
    }
  }
}
