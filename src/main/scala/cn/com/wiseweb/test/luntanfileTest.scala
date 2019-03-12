package cn.com.wiseweb.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yangguihu on 2016/11/23.
  */
object luntanfileTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("hahah")
     // .set("spark.streaming.kafka.maxRatePerPartition",record)
      .setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val sc: SparkContext = ssc.sparkContext
    sc.setLogLevel("WARN")

    var dStream= ssc.textFileStream("hdfs://node4:8020/wc/")
    dStream.foreachRDD(
      rdd=>{
        rdd.foreach(println(_))
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
