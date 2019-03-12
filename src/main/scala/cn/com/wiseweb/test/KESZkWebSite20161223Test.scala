//package cn.com.wiseweb.test
//
//import java.io.ByteArrayInputStream
//import java.net.URL
//import java.text.SimpleDateFormat
//import java.util.regex.Pattern
//
//import com.alibaba.fastjson.{JSON, JSONObject}
//import kafka.serializer.StringDecoder
//import net.jpountz.xxhash.{StreamingXXHash64, XXHashFactory}
//import org.apache.spark.sql.{Row, SQLContext}
//import org.elasticsearch.spark.rdd.EsSpark
//
//import scala.util.control.Breaks._
//import scala.util.matching.Regex
////import org.elasticsearch.spark.rdd.EsSpark
//import org.apache.spark.streaming.kafka.KafkaManager
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
////import scala.util.control.Breaks._
//
//
///**
//  * 采用直连的方式连接到kafka并将记录放入到zk中
//  */
//object KESZkWebSite20161223Test {
//  /**
//    * 把string 转为urlhash
//    * @param str
//    * @return
//    */
//  def xxHash(str: String): Long = {
//    try {
//      val factory: XXHashFactory = XXHashFactory.fastestInstance
//      val data: Array[Byte] = str.getBytes("UTF-8")
//      val in: ByteArrayInputStream = new ByteArrayInputStream(data)
//      val seed: Int = 0
//      val hash64: StreamingXXHash64 = factory.newStreamingHash64(seed)
//      val buf: Array[Byte] = new Array[Byte](64)
//      breakable {
//        while (true) {
//          {
//            val read: Int = in.read(buf)
//            if (read == -1) {
//              break
//            }
//            hash64.update(buf, 0, read)
//          }
//        }
//      }
//      val hash: Long = hash64.getValue
//      return hash
//    }
//    catch {
//      case e: Exception => {
//        return 0L
//      }
//    }
//  }
//
//  /**
//    * 判断string字符串的key是否存在，如果存在进行非空判断
//    *
//    * @param parseObject
//    * @param str
//    * @return
//    */
//  def isEmptyFileds(parseObject: JSONObject,str: String): String ={
//    val fileds=if(parseObject.containsKey(str)){
//      if ("".equals(parseObject.get(str)) || parseObject.get(str).equals("(null)") || parseObject.get(str)==null ){
//        ""
//      }else{
//        parseObject.get(str).toString
//      }
//    }else{
//      ""
//    }
//    fileds
//  }
//
//
//  /**
//    * 判断string字符串的key是否存在，如果存在进行非空判断
//    *
//    * @param parseObject
//    * @param str
//    * @return
//    */
//  def isEmptyFiledsNum(parseObject: JSONObject,str: String): String ={
//    val fileds=if(parseObject.containsKey(str)){
//      if ("".equals(parseObject.get(str)) || parseObject.get(str).equals("(null)") || parseObject.get(str)==null ){
//        ""
//      }else{
//        parseObject.get(str).toString
//      }
//    }else{
//      "1"
//    }
//    fileds
//  }
//
//
//
//  def getTopDomainWithoutSubdomain(url: String,regular: String): String ={
//    try {
//      val host = new URL(url).getHost().toLowerCase()
//      val pattern = Pattern.compile(regular)
//      val matcher = pattern.matcher(host)
//      if(matcher.find())
//        matcher.group()
//      else
//        "根据规则未发现一二级域名"
//    } catch {
//      case ex:Exception => {
//        return "一二级域名解析有误"
//      }
//    }
//  }
//
//  /**
//    * 把字符串格式“yyyy-MM-dd HH:mm:ss” 转换成对应的时间戳
//    *
//    * @param date
//    * @return
//    */
//  def getStrToTime(date:String):String={
//    val formatdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val formatdate1 = new SimpleDateFormat("yyyyMMddHHmmss")
//    if(date.contains("-")){
//      try {
//        formatdate.parse(date)
//        date
//      } catch {
//        case ex:Exception => {
//          "1970-01-01 00:00:00"
//        }
//      }
//    }else{
//      val datelength=date.length
//      datelength match {
//        case 14 => date.substring  (0,4).concat("-").concat(date.substring(4,6)).concat("-").concat(date.substring(6,8)).concat(" ").concat(date.substring(8,10)).concat(":").concat(date.substring(10,12)).concat(":").concat(date.substring(12,14))
//        case 13 => date.substring  (0,4).concat("-").concat(date.substring(4,6)).concat("-").concat(date.substring(6,8)).concat(" ").concat(date.substring(8,10)).concat(":").concat(date.substring(10,12)).concat(":").concat("00")
//        case 12 => date.substring  (0,4).concat("-").concat(date.substring(4,6)).concat("-").concat(date.substring(6,8)).concat(" ").concat(date.substring(8,10)).concat(":").concat(date.substring(10,12)).concat(":").concat("00")
//        case 8 => date.substring  (0,4).concat("-").concat(date.substring(4,6)).concat("-").concat(date.substring(6,8)).concat(" ").concat("00").concat(":").concat("00").concat(":").concat("00")
//        case _ =>  "1970-01-01 00:00:00"
//      }
//    }
//  }
//
//
//
//  def main(args: Array[String]) {
////    Logger.getRootLogger.setLevel(Level.WARN)
//
////    val Array(secTime, records) = args
//    val brokers = "node1:9092,node2:9092,node3:9092,node6:9092,node7:9092"
//    val topics = "wiseweb_crawler_outside"
//    val group = "wiseweb_crawler_outsideGroupTest"
//
//
//    //一级域名提取
//    val RE_TOP1 = "(\\w*\\.?){1}\\.(com.cn|com.mo|com.ph|com.pk|com.au|com.sg|co.za|co.uk|ne.jp|net.cn|org.cn|gov.cn|gov.ru|lastampa.it|intoday.in|com|sg|net|cn|org|gov|cc|it|vn|co|me|tel|mobi|asia|biz|info|name|tv|hk|in|ca|tj|xin|ltd|store|vip|mom|game|lol|work|pub|club|xyz|top|ren|bid|loan|red|win|link|wang|date|party|site|online|tech|website|space|live|studio|press|news|video|click|trade|science|wiki|design|pics|photo|help|gift|rocks|band|market|software|social|lawyer|engineer|so|ru|am|公司|中国|网络)$";
//    // 二级域名提取
//    val RE_TOP2 = "(\\w*\\.?){2}\\.(com.cn|com.mo|com.ph|com.pk|com.au|com.sg|co.za|co.uk|ne.jp|net.cn|org.cn|gov.cn|gov.ru|lastampa.it|intoday.in|com|sg|net|cn|org|gov|cc|it|vn|co|me|tel|mobi|asia|biz|info|name|tv|hk|in|ca|tj|xin|ltd|store|vip|mom|game|lol|work|pub|club|xyz|top|ren|bid|loan|red|win|link|wang|date|party|site|online|tech|website|space|live|studio|press|news|video|click|trade|science|wiki|design|pics|photo|help|gift|rocks|band|market|software|social|lawyer|engineer|so|ru|am|公司|中国|网络)$";
//
//
//    // Create context with 5 second batch interval
////    val sparkConf = new SparkConf().setAppName("KafkaESDirect").setMaster("local[*]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val sparkConf = new SparkConf().setAppName("KafkaESWebPageDirect").setMaster("local[*]").set("spark.streaming.kafka.maxRatePerPartition","10").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
////    val sparkConf = new SparkConf().setAppName("KafkaESWebSiteDirect").set("spark.streaming.kafka.maxRatePerPartition","6")
//    sparkConf.set("es.index.auto.create", "true")
////    sparkConf.set("pushdown","true")
//    sparkConf.set("es.nodes", "10.251.5.150")
////    sparkConf.set("es.mapping.date.rich","false")
//    sparkConf.set("es.port", "9200")
//    val ssc = new StreamingContext(sparkConf, Seconds(60))
////    ssc.checkpoint("C:\\wzty")
////    ssc.checkpoint("hdfs://mycluster/wzty/es-directWebSites-ck")
//    // Create direct kafka stream with brokers and topics
//    val topicsSet = topics.split(",").toSet
//    val sc: SparkContext = ssc.sparkContext
//    val sqlContext = new SQLContext(sc)
//    sc.setLogLevel("WARN")
//    //把可变Map放入广播变量
////    val broadcastMap = sc.broadcast(scala.collection.mutable.HashMap[String, String]())
//    val docinfo = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://10.44.26.228:3306/weibo_data", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "idvname", "user" -> "root", "password" -> "BidData123")).load()
//    val docinfoLanguage = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://10.44.26.228:3306/weibo_data", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "language", "user" -> "root", "password" -> "BidData123")).load()
//    //拼map,Map(site_id,site_name ...)
//    val maplist = scala.collection.mutable.HashMap[String, String]()
//    val collect: Array[Row] = docinfo.collect()
//    for (i <- collect){
//      maplist+=((i(0).toString,i(1).toString))
//    }
//    //外媒规格库读取 add by 2016 12 06
//    val mapLanguage = scala.collection.mutable.HashMap[String, String]()
//    val collectLanguage: Array[Row] = docinfoLanguage.collect()
//    for (p <- collectLanguage){
//      mapLanguage+=((p(0).toString,p(1).toString))
//    }
//    val broadcastLanguage = sc.broadcast(mapLanguage)//广播外媒规则库
//    val broadcastMap = sc.broadcast(maplist)   //广播site_id对应site_name规则库
//    println(broadcastMap.value.size)
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers,
//      "group.id" -> group
//      //从最新的开始读取
////      ,"auto.offset.reset" -> "largest"
//      ,"auto.offset.reset" -> "smallest"
//    )
//    val km = new KafkaManager(kafkaParams)
//    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicsSet)
//    println("==========================================================")
//    messages.foreachRDD(rdd=>{
//      if (!rdd.isEmpty()) {
////        val rdd12 = rdd.map(t=>{
//          val rdd12 = rdd.map(t=>{
//          var parseObject = new JSONObject(19)
//           try {
//             parseObject = JSON.parseObject(t._2)
//           }catch {
//             case x:Exception => {
//               //println("==========json格式不对==============")
//               // println(t._2)
//               val maps1= Map("site_id" -> "", "site_name" -> "",
//                 "site_url" -> "","group_id" -> "","title" -> "",
//                 "summary" -> "","content" -> "",
//                 "url" -> "","urlhash" -> "",
//                 "click" -> "","reply" -> "",
//                 "source" -> "",
//                 "area" -> "","publishtime" -> "1970-01-01 00:00:00","author" -> "",
//                 "gathertime" -> "1970-01-01 00:00:00","inserttime" -> "1970-01-01 00:00:00",
//                 "domain_1" -> "","domain_2" -> "","tendency" -> "","sim_id" -> "")
//               maps1
//               //               break //处理下一条
//             }
//           }
//                        val site_id: String = isEmptyFileds(parseObject,"site_id")
//                        var site_name: String = isEmptyFileds(parseObject,"site_name")
//                        if ("".equals(site_name)) {
//                          //根据获取到的site_id去规则库匹配相应的site_name
//                          site_name = broadcastMap.value.getOrElse(site_id, "")
//                        }
////                        val title: String = isEmptyFileds(parseObject,"title")
//                          val title: String = {
//                            //对于title字段等于""的赋值为"0^A"
//                            if (parseObject.get("title")==null||"".equals(parseObject.get("title").toString)||"null".equals(parseObject.get("title").toString)||"(null)".equals(parseObject.get("title").toString)){
//                              "0^A"
//                            }else{
//                              parseObject.get("title").toString
//                            }
//                          }
//                        val summary: String = isEmptyFileds(parseObject,"summary")
////                        val content: String = isEmptyFileds(parseObject,"content")
//                          val content: String = {
//                            //对于title字段等于""的赋值为"0^A"
//                            if ("".equals(parseObject.get("content"))||"null".equals(parseObject.get("content"))||"(null)".equals(parseObject.get("content"))){
//                              "0^A"
//                            }else{
//                              parseObject.get("content").toString
//                            }
//                          }
//                        val site_url: String = isEmptyFileds(parseObject,"site_url")
//                        val urlhash: String = isEmptyFileds(parseObject,"urlhash")
//                        //针对火车头外媒数据
////                        val click: String = isEmptyFiledsNum(parseObject,"click")
////                        val reply: String = isEmptyFiledsNum(parseObject,"reply")
//                        val click: String = isEmptyFileds(parseObject,"click")
//                         val reply: String = isEmptyFileds(parseObject,"reply")
//                        val source: String = isEmptyFileds(parseObject,"source")
//                        val area: String = isEmptyFileds(parseObject,"area")
//                        val author: String = isEmptyFileds(parseObject,"author")
//                        val tendency: String = isEmptyFileds(parseObject,"tendency")
//                        val sim_id: String = isEmptyFileds(parseObject,"sim_id")
//                         val urlstr: String = isEmptyFileds(parseObject,"url")
//                         val groupIds: String = isEmptyFileds(parseObject,"group_id")
//                          var languages=""   //语种
//                            if (("".equals(site_id)) && (!"".equals(groupIds))){
//                              languages=isEmptyFileds(parseObject,"language")
//                            }
//                          //对于group_id有值的情况  edit by 2017-01-05
//                          val groupId = if ((!"".equals(site_id)) && ("".equals(groupIds))){
//                            val site_idInt: Int = site_id.toInt
//                            if (site_idInt<=59999 && site_idInt>=1){
//                              if (urlstr.contains("blog")){
//                                3     //博客
//                              }else{
//                                1    //新闻
//                              }
//                            }
//                            //=======微信================
//                            else if(urlstr.contains("http://mp.weixin.qq.com")){11}
////                            else if(site_idInt>= (-3602) && site_idInt <=(-3598)){11}
//                            //                            //银行微信
//                            //                            else if((site_idInt>=(-5242) && site_idInt <=(-5238))||site_idInt==(-4999)||site_idInt==(-7651)){11}
//                            //                            //银行微信公众号
//                            //                            else if(site_idInt>= (-7972) && site_idInt <=(-7762)){11}
//                            //                            //食药微信
//                            //                            else if(((-11712) <= site_idInt&&site_idInt <=(-11618))||site_idInt==(-11714)||site_idInt==(-14880)){11}
//                            //                            //食药微信公众号
//                            //                            else if((-14974) <= site_idInt&&site_idInt <=(-14881)){11}
//                            //                            //中国兵团微信
//                            //                            else if((-16255) <= site_idInt&&site_idInt <=(-16171)){11}
//                            //                            //央视微信
//                            //                            else if((-20445) <= site_idInt&&site_idInt <=(-20439)){11}
//                            //                            //央视微信公众号
//                            //                            else if((-21570) <= site_idInt&&site_idInt <=(-21386)){11}
//                            //                            //日照银行微信
//                            //                            else if((-72231) <= site_idInt&&site_idInt <=(-72174)){11}
//                            //                            //影视微信
//                            //                            else if((-87948) <= site_idInt&&site_idInt <=(-85475)){11}
//                            //                            //影视微信公众号
//                            //                            else if((-88554) <= site_idInt&&site_idInt <=(-88546)){11}
//                            else if (site_idInt>=60000 && site_idInt<=69999){
//                              2      //论坛
//                            }
//                            else if (site_idInt>=70000 && site_idInt<=89999){
//                              1      //新闻
//                            }
//                            else if (site_idInt>=100000 && site_idInt<200000){
//                              1      //新闻
//                            }
//                              //纸媒
//                            else if (site_idInt>=200000){
////                              if (site_idInt==200598 || site_idInt==200599 || (site_idInt>=200642 && site_idInt<=200646)){
//                              //                                1     //新闻
//                              //                              }
//                              if(site_idInt==200611 || site_idInt==200621 || site_idInt==200622 || site_idInt==200628){
//                                3     //博客
//                              }
//                              else if(site_idInt==200620){
//                                14   //问答
//                              }
//                              else if((site_idInt>=200623 && site_idInt<=200627)||(site_idInt>=200629 && site_idInt<=200635)){
//                                13   //新闻客户端
//                              }
//                              else if(site_idInt>=300001 && site_idInt<=300065){
//                                1   //首页  edit by 2016 12 02
//                              }else{
//                                5    //纸媒
//                              }
//                            }
//                            else{
//                              //如果是外媒的情况
//                              languages = broadcastLanguage.value.getOrElse(site_id,"")
//                              if (!"".equals(languages)){
//                                7      //外媒
//                              }else{
//                                1       //新闻
//                              }
//                            }
//                          }else{
//                                groupIds
//                          }
//                      val topDoamin1 = getTopDomainWithoutSubdomain(urlstr,RE_TOP1);
//                      val topDoamin2 = getTopDomainWithoutSubdomain(urlstr,RE_TOP2);
//                      val insertTime ="1970-01-01 00:00:00";
//                      //判断发布时间和收集时间的值是否是空，如果是空，给默认值是当前插入时间
//                    val gathertime = if (parseObject.containsKey("gathertime")) {
//                      if ("".equals(parseObject.get("gathertime")) || parseObject.get("gathertime").equals("NULL") || parseObject.get("gathertime") == null) {
//                        insertTime
//                      } else {
//                        getStrToTime(parseObject.get("gathertime").toString)
//                      }
//                    }else{
//                      insertTime
//                    }
//                    //正则匹配2000到2039年
//                    val pattern = new Regex("((20[0-3][0-9]-(0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9]")
//                    var publishtime_str=""
//                    val publishtime = if (parseObject.containsKey("publishtime")) {
//                      if ("".equals(parseObject.get("publishtime")) || parseObject.get("publishtime").equals("NULL") || parseObject.get("publishtime") == null) {
//                        gathertime
//                      } else {
//                        // getStrToTime(parseObject.get("publishtime").toString)
//                        // getStrToTimePub(parseObject.get("publishtime").toString,gathertime)
//                        val date_str=parseObject.get("publishtime").toString
//                        if(date_str.contains("-")){
//                          if (date_str.length==16 && date_str.contains(" ")) {
//                            date_str.concat(":00")
//                          }else{
//
//                            if ((pattern findAllIn date_str).mkString(",") == null || "".equals((pattern findAllIn date_str).mkString(","))) {
//                              publishtime_str = date_str
//                              gathertime
//                            } else {
//                              date_str
//                            }
//                          }
//                        }else{
//                          val datelength=date_str.length
//                          datelength match {
//                            case 14 => date_str.substring(0,4).concat("-").concat(date_str.substring(4,6)).concat("-").concat(date_str.substring(6,8)).concat(" ").concat(date_str.substring(8,10)).concat(":").concat(date_str.substring(10,12)).concat(":").concat(date_str.substring(12,14))
//                            case 13 => date_str.substring(0,4).concat("-").concat(date_str.substring(4,6)).concat("-").concat(date_str.substring(6,8)).concat(" ").concat(date_str.substring(8,10)).concat(":").concat(date_str.substring(10,12)).concat(":").concat("00")
//                            case 12 => date_str.substring(0,4).concat("-").concat(date_str.substring(4,6)).concat("-").concat(date_str.substring(6,8)).concat(" ").concat(date_str.substring(8,10)).concat(":").concat(date_str.substring(10,12)).concat(":").concat("00")
//                            case 8 => date_str.substring(0,4).concat("-").concat(date_str.substring(4,6)).concat("-").concat(date_str.substring(6,8)).concat(" ").concat("00").concat(":").concat("00").concat(":").concat("00")
//                            case _ =>
//                              publishtime_str=date_str
//                              gathertime
//                          }
//                        }
//                      }
//                    }else{
//                      gathertime
//                    }
//                    //处理完publishtime,再过滤下，验证年月日正确性，仅仅通过上面方式，如果是yyyyMMddHHmmss方式拼接，会有月，或者日，或时分秒,格式不挣钱情况，如：有的月份没有31天，有的月份超月了
//                    val publishtimes= {//add by 2016 12月 05日
//                      if (publishtime.contains("&quot;")){
//                        publishtime_str = publishtime
//                        gathertime
//                      }else {
//                        if ((pattern findAllIn publishtime).mkString(",") == null || "".equals((pattern findAllIn publishtime).mkString(","))) {
//                          publishtime_str = publishtime
//                          gathertime
//                        } else {
//                          publishtime
//                        }
//                      }
//                    }
//
//                    val inserttime =if (parseObject.containsKey("inserttime")) {
//                      if ("".equals(parseObject.get("inserttime")) || parseObject.get("inserttime").equals("NULL") || parseObject.get("inserttime") == null) {
//                        insertTime
//                      } else {
//                        getStrToTime(parseObject.get("inserttime").toString)
//                      }
//                    }else{
//                      insertTime
//                    }
////         val wxhash=if(!"11".equals(groupId)){""}else{xxHash(source.concat(title)).toString}
//           val maps= Map("site_id" -> site_id, "site_name" -> site_name,
//             "site_url" -> site_url,"group_id" -> groupId,"title" -> title,
//             "summary" -> summary,"content" -> content,
//             "url" -> urlstr,"urlhash" -> urlhash,
//             "click" -> click,"reply" -> reply,
//             "source" -> source,"agg_domain_1" -> topDoamin1,
//             "area" -> area,"publishtime" -> publishtimes,"author" -> author,"language" -> languages,
//             "gathertime" -> gathertime,"inserttime" -> inserttime,"publishtime_str" -> publishtime_str,
//             "domain_1" -> topDoamin1,"domain_2" -> topDoamin2,"tendency" -> tendency,"sim_id" -> sim_id)
//            maps
//        })
//        if (!rdd12.isEmpty()){
//          //对group_id=11 微信的摘出来
////          val rddgroup = rdd12.filter(linep=>{
////            linep.getOrElse("group_id","100").toString.toInt==11
////          })
////          EsSpark.saveToEs(rddgroup,"wiseweb_crawler_wechat/wechat",Map("es.mapping.id" -> "urlhash"))
//          //            val collects: Array[Map[String, Any]] = rddgroup.collect()
//          //            val filterGroup: RDD[Map[String, Any]] = rdd12.filter(!collects.contains(_))
//          //排除微信情况后 ，对site_id 在300001和300065范围进行处理（首页）
//          val rddsite = rdd12.filter(linet=>{
//            if((!"7".equals(linet.getOrElse("group_id","100").toString))&&(!"".equals(linet.getOrElse("site_id","100").toString))){
//              linet.getOrElse("site_id","15").toString.toInt>=300001 && linet.getOrElse("site_id","15").toString.toInt<=300065
//            }else{
//              false
//            }
//
//          })
//          EsSpark.saveToEs(rddsite,"souye/sy",Map("es.mapping.id" -> "url"))
//
//          //对title是null,(null),""的单独拉取一个索引里
////          val rddtitle = rdd12.filter(linetitle=>{
////            "0^A".equals(linetitle.getOrElse("title","1^A").toString) || "null".equals(linetitle.getOrElse("title","1^A").toString) || "(null)".equals(linetitle.getOrElse("title","1^A").toString)
////          })
////          EsSpark.saveToEs(rddtitle,"wiseweb_crawler_errors/errors",Map("es.mapping.id" -> "url"))
//
//          // 对group_id=7 外媒的摘出来
//          val rddgroup7 = rdd12.filter(line7=>{
//            line7.getOrElse("group_id","100").toString.toInt==7
//          })
//          EsSpark.saveToEs(rddgroup7,"weiwap/test",Map("es.mapping.id" -> "url"))
//
//          val rddfinal = rdd12.filter(linet=>{
//            if (!"".equals(linet.getOrElse("site_id","100").toString)){
//              (linet.getOrElse("group_id","100").toString.toInt!=11) && (linet.getOrElse("group_id","100").toString.toInt!=7) && (linet.getOrElse("site_id","15").toString.toInt<300001 || linet.getOrElse("site_id","15").toString.toInt>300065) && (!"0^A".equals(linet.getOrElse("title","1^A").toString) || !"null".equals(linet.getOrElse("title","1^A").toString) || !"(null)".equals(linet.getOrElse("title","1^A").toString))
//            }else{
//              false
//            }
//
//          })
//          EsSpark.saveToEs(rddfinal,"zong/tests",Map("es.mapping.id" -> "url"))
//        }else{
//          EsSpark.saveToEs(rdd12,"zong/tests",Map("es.mapping.id" -> "url"))
//        }
//        km.updateZKOffsets(rdd)
//      }
//
//    })
//    println("--------------------End-----------------")
//    // Start the computation
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}