package cn.com.wiseweb.gather

import org.apache.spark.streaming.{Duration, Seconds}

/**
  * Created by yangguihu on 2016/10/12.
  */
object Testn {
//  def main(args: Array[String]) {
//    var rows= new util.HashSet[String]()
//    rows.add("content")
//    rows.add("click")
//    rows.add("reply")
//    rows.add("author")
//    rows.add("tendency")
//    rows.add("sim_id")
//    rows.add("gathertime")
//    rows.add("id")
//
//    import scala.util.control.Breaks._
//    val arr3 = Array("hadoop", "storm", "spark")
////    arr3.foreach(str=> breakable {
////      if(str.equals("hadoop"))
////        break
////      else
////        println(str)
////    })
//
//    val str=""
//    var ol = fsjfks(str)
//    var str1 = ol.toString
//    println(str1)
//    println("0".equals(str1))
//
//
////    var rows1= new util.HashSet[String]()
////    rows1.add("content")
////    rows1.add("click")
////    rows1.add("reply")
////    rows1.add("author")
////    rows1.add("tendency")
////
////    rows.removeAll(rows1)
////    val it: util.Iterator[String] = rows.iterator()
////    while(it.hasNext){
////      println(it.next())
////    }
//  }
//
//  def fsjfks(str:String):Long ={
//  0L
//  }

  def main(args: Array[String]) {
//println(args.length)
    var record="200"
    var interval="3"
    if(args.length!=0){
      record=args(0)
      interval=args(1)
    }

    var seconds: Duration = Seconds(java.lang.Long.valueOf(interval))
//    var seconds: Duration = Seconds(interval.asInstanceOf[java.lang.Long])
    println(seconds)
    println(record)
  }

}
