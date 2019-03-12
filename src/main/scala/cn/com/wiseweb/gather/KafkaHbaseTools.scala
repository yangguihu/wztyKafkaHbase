package cn.com.wiseweb.gather

import java.text.SimpleDateFormat
import java.util.{Date, Random}

import scala.io.{BufferedSource, Source}

/**
  * Created by yangguihu on 2016/8/30.
  */
object KafkaHbaseTools {
   private val WEIBOUSER_EXCEPTION="weibouser_exception"
  def main(args: Array[String]) {

//    val str=siteGroupMapping("59999","http://blog.sina.com.cn/s/blog_53d2ac990102wwmq.html?tj=1","1")
//    println(str)
  }

//  /**
//    * 根据site_id 解析出group_id
//    * @param site_id   site_id
//    * @param url       url地址,根据url区分blog
//    * @param groupid   传递的默认的group_id
//    * @return           通过site_id解析
//    */
//  def siteGroupMapping(site_id: AnyVal,url: Object,groupid: String):String ={
//    try{
//      //返回group_id
//      var group_id= ""
//      val siteid = Integer.valueOf(site_id.toString)
//      //新闻
//      if(1<=siteid && siteid <=59999){
//        if(url.toString.contains("blog"))
//          group_id= "3"   //包含blog 为播客
//        else
//          group_id= "1"   //新闻
//      }
//      //论坛
//      if(60000<=siteid && siteid <=69999){
//        group_id= "2"
//      }
//      //外媒
//      if(90000<=siteid && siteid <=99999){
//        group_id= "7"
//      }
//      //纸媒
//      if(siteid>=200000){
//        if(siteid==200598 || siteid==200599){
//          group_id= "1" //新闻
//        }else if(siteid==200611|| siteid==200621|| siteid== 200622){
//          group_id= "3" //播客
//        }else if(siteid == 200620){
//          group_id = "14"  //问答
//        }else{
//          group_id="5"  //纸媒
//        }
//      }
//      group_id
//    }catch {
//      case ex:Exception => {
//        groupid
//      }
//    }
//  }

  /**
    * 读取文件添加进hashSet
    *
    * @param filePath
    * @return
    */
  def fileToSet(filePath:String): java.util.HashSet[String] = {
    val filesSet=new java.util.HashSet[String]
    try{
      val file: BufferedSource = Source.fromFile(filePath,"utf-8")
      //val lineIterator: Traversable[String] = file.getLines().toTraversable
      val lineIterator = file.getLines()
      for (line <- lineIterator){
        filesSet.add(line)
      }
      filesSet
    }catch {
      case ex:Exception => {
        filesSet.add(WEIBOUSER_EXCEPTION)
        filesSet
      }
    }
  }


  def getUrlHash(): String = {
    var str=new StringBuilder();//定义变长字符串
    val random=new Random
    //随机生成数字，并添加到字符串
    var flage=random.nextInt(10);
    if(flage>4){
      str.append("-")
    }
    for(i <- 1 to 19) {
      val nextInt = random.nextInt(10)
      str.append(nextInt)
    }
    return str.toString();
  }

  def getDate():String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    return format.format(new Date())
  }

  /**
    * 把字符串格式“yyyy-MM-dd HH:mm:ss” 转换成对应的时间戳
    *
    * @param date
    * @return
    */
  def getStrToTime(date:String):String={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    String.valueOf(format.parse(date).getTime)
  }

 //直接获取时间
  def getStrToTime2():String={
   String.valueOf(new Date().getTime)
  }

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
