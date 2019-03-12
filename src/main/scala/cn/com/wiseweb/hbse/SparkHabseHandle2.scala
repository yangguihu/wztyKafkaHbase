package cn.com.wiseweb.hbse

import java.text.SimpleDateFormat
import java.util

import cn.com.wiseweb.gather.{HashUtil, KafkaHbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created by yangguihu on 2016/11/1.
  */
object SparkHabseHandle2 {
  //列族Set  和c2的快照
  private val cfSet=new util.HashSet[String](util.Arrays.asList(
    "content","gathertime","inserttime","site_id","site_name","site_url","title","url"
  ))

  def main(args: Array[String]) {
    //获取传入参数，便于调试
    var INPUT_TABLE="newspaper_new"
    var OUTPUT_TABLE="paper1"
    if(args.length!=0){
      INPUT_TABLE=args(0)
      OUTPUT_TABLE=args(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkHabseHandle2")
          .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //设置日志级别
    sc.setLogLevel("WARN")

//    val table=args(0)
//    val submitcount=Integer.valueOf(args(1))

    val bcCfSet = sc.broadcast(cfSet)

    //habse 配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node2,node3,node1") // zookeeper地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, INPUT_TABLE)
    hbaseConf.set(TableInputFormat.SCAN_BATCHSIZE,"500")

    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, OUTPUT_TABLE)


    val job = new Job(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //获取hbase连接
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

//    println(hBaseRDD.count())
        // 遍历每个分区
    hBaseRDD.foreachPartition(it=>{
          //处理消息记录
          processPartition(it,bcCfSet)
        })
    //保存rdd
    hBaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }

  /**
    * 处理每个分区的消息，遍历分区创建hbase连接批量发送消息
    *
    * @param partion              分区
    *   bcCfSet
    */
  def processPartition(partion: Iterator[(ImmutableBytesWritable, Result)],
                       bcCfSet :Broadcast[util.HashSet[String]]):Unit = {
    //遍历每个分区中的每条消息
    partion.foreach(record => breakable{
      try {
        val re = record._2
        var url= Bytes.toString(re.getValue(Bytes.toBytes("c1"), Bytes.toBytes("url")))
        var gathertime= Bytes.toString(re.getValue(Bytes.toBytes("c1"), Bytes.toBytes("gathertime")))

        var urlhash= HashUtil.xxHash(url)
        var strGather=getStrToTime(gathertime)

        val put=new Put(Bytes.toBytes(urlhash+"_"+strGather))

        if (urlhash!=0L){
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
        }

        //返回集合
        (new ImmutableBytesWritable, put)
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
