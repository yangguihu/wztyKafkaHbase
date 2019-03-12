package cn.com.wiseweb.hbse

import java.text.SimpleDateFormat
import java.util

import cn.com.wiseweb.gather.{HashUtil, KafkaHbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created by yangguihu on 2016/11/1.
  */
object SparkHabseHandleOld {

  //接口定义的字段list
  private val keyList = util.Arrays.asList(
    "site_id","site_name","site_url","group_id","title","content","url","urlhash",
    "publishtime","gathertime","inserttime","author","sim_id","snapshot")

  //做成set 唯一不重复
  private val weiboKeySet=new util.HashSet[String](keyList)

  //列族Set  和c2的快照
  private val cfSet=new util.HashSet[String](util.Arrays.asList(
    "content","gathertime","inserttime","site_id","site_name","site_url","title","url"
  ))

  def main(args: Array[String]) {
    //获取传入参数，便于调试
    var inTable="newspaper"
    var outTable="paper1"
    var cache="10000"
    var count="2000"
    if(args.length!=0){
      inTable=args(0)
      outTable=args(1)
      cache=args(2)
      count=args(3)
    }

    val sparkConf = new SparkConf().setAppName("SparkHabseHandle")
//          .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //habse 配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, inTable)

    var scan = new Scan()
    scan.setCaching(10000)
    scan.setCacheBlocks(false)
    var proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    var ScanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set(TableInputFormat.SCAN, ScanToString)

//    hbaseConf.set(TableInputFormat.INPUT_TABLE, "newspaper_new")
    //获取hbase连接
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //设置日志级别
    sc.setLogLevel("WARN")
    val bcWeiboKeySet = sc.broadcast(weiboKeySet)
    val bcWeiboKeyList = sc.broadcast(keyList)
    val bcCfSet = sc.broadcast(cfSet)

    val bcOutTable = sc.broadcast(outTable)
    val bcSubmitcount = sc.broadcast(Integer.valueOf(count))

        // 遍历每个分区
    hBaseRDD.foreachPartition(it=>{
          //处理消息记录
          processPartition(it,bcWeiboKeySet,bcWeiboKeyList,bcCfSet,bcOutTable,bcSubmitcount)
        })
  }

  /**
    * 处理每个分区的消息，遍历分区创建hbase连接批量发送消息
    *
    * @param partion              分区
    * @param bcWeiboKeySet   接口定义字段名
    * @param bcWeiboKeyList
    *   bcCfSet
    */
  def processPartition(partion: Iterator[(ImmutableBytesWritable, Result)],bcWeiboKeySet :Broadcast[util.HashSet[String]],
                       bcWeiboKeyList :Broadcast[util.List[String]],bcCfSet :Broadcast[util.HashSet[String]],
                       bcOutTable :Broadcast[String],bcSubCon :Broadcast[Integer]):Unit = {
    //遍历rdd的每个分区
    val hbcon = HBaseConfiguration.create()
    hbcon.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbcon.set("hbase.zookeeper.property.clientPort", "2181")
    //获取hbase连接
    val connection = HConnectionManager.createConnection(hbcon)
    val table = connection.getTable(bcOutTable.value); //user

    table.setAutoFlush(false, true)
    //设置连接20m
    table.setWriteBufferSize(20971520);
    //创建批量put list
    var arraylist = new util.ArrayList[Put]
    //获取接口定义字段名集合
    var weiboKeys=new util.HashSet[String](bcWeiboKeyList.value)
    val subCon = bcSubCon.value
    //遍历每个分区中的每条消息
    partion.foreach(record => breakable{
      try {
        val re = record._2
        var url= Bytes.toString(re.getValue(Bytes.toBytes("c1"), Bytes.toBytes("url")))
        var gathertime= Bytes.toString(re.getValue(Bytes.toBytes("c1"), Bytes.toBytes("gathertime")))

        var urlhash= HashUtil.xxHash(url)
        var strGather=getStrToTime(gathertime)
        if (urlhash!=0L){
          val put=new Put(Bytes.toBytes(urlhash+"_"+strGather))
          for(cf <- bcCfSet.value.toArray()){
            put.add(Bytes.toBytes("c1"), Bytes.toBytes(cf.toString), re.getValue(Bytes.toBytes("c1"), Bytes.toBytes(cf.toString)))
          }
          //修改urlhash
          put.add(Bytes.toBytes("c1"), Bytes.toBytes("urlhash"), Bytes.toBytes(urlhash))
          //修改字段 出本时间
          var publishtime=Bytes.toString(re.getValue(Bytes.toBytes("c1"), Bytes.toBytes("publishdate")))
            if(publishtime==null){
              publishtime=Bytes.toString(re.getValue(Bytes.toBytes("c1"), Bytes.toBytes("publishtime")))
            }
          put.add(Bytes.toBytes("c1"), Bytes.toBytes("publishtime"), Bytes.toBytes(publishtime))

          //group_id
          val site_id= Bytes.toString(re.getValue(Bytes.toBytes("c1"), Bytes.toBytes("site_id")))
          val group_id=KafkaHbaseUtils.siteGroupMapping(site_id,url,"5")
          put.add(Bytes.toBytes("c1"), Bytes.toBytes("group_id"), Bytes.toBytes(group_id))

          //快照
          put.add(Bytes.toBytes("c2"), Bytes.toBytes("snapshot"), re.getValue(Bytes.toBytes("c2"), Bytes.toBytes("snapshot")))

          put.add(Bytes.toBytes("c1"), Bytes.toBytes("author"), Bytes.toBytes(""))
          put.add(Bytes.toBytes("c1"), Bytes.toBytes("sim_id"), Bytes.toBytes(""))

          //添加进集合
          arraylist.add(put)
          if(arraylist.size()==subCon){
            table.put(arraylist) //插入数据
            table.flushCommits() //提交
            arraylist.clear()
          }
        }
      }catch {
        case x:Exception=>{
          println("=================传递消息不是json================")
          println(record._2)
          println("=================================================")
          println(x.getMessage)
          break //处理下一条
        }
      }
    })
    table.put(arraylist) //插入数据
    table.flushCommits() //提交
//    table.close()
//    connection.close()
  }

  /**
    * 把字符串格式“yyyy-MM-dd HH:mm:ss” 转换成对应的时间戳
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
}
