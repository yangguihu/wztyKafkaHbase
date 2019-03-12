//import java.sql.{Connection, ResultSet}
//import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
//import org.slf4j.LoggerFactory
//
//object ConnectionPool {
//
//  val logger = LoggerFactory.getLogger(this.getClass)
//  private val connectionPool = {
//    try {
//      Class.forName("com.mysql.jdbc.Driver")
//      val config = new BoneCPConfig()
//      config.setJdbcUrl("jdbc:mysql://192.168.0.46:3306/test")
//      config.setUsername("test")
//      config.setPassword("test")
//      config.setMinConnectionsPerPartition(2)
//      config.setMaxConnectionsPerPartition(5)
//      config.setPartitionCount(3)
//      config.setCloseConnectionWatch(true)
//      config.setLogStatementsEnabled(true)
//      Some(new BoneCP(config))
//    } catch {
//      case exception: Exception =>
//        logger.warn("Error in creation of connection pool" + exception.printStackTrace())
//        None
//    }
//  }
//
//  def getConnection: Option[Connection] = {
//    connectionPool match {
//      case Some(connPool) => Some(connPool.getConnection)
//      case None => None
//    }
//  }
//
//  def closeConnection(connection: Connection): Unit = {
//    if (!connection.isClosed) connection.close()
//  }
//}
//
//import java.sql.{Connection, DriverManager, PreparedStatement}
//
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.slf4j.LoggerFactory
//
///**
//  * 记录最近五秒钟的数据
//  */
//object RealtimeCount1 {
//
//  case class Loging(vtime: Long, muid: String, uid: String, ucp: String, category: String, autoSid: Int, dealerId: String, tuanId: String, newsId: String)
//
//  case class Record(vtime: Long, muid: String, uid: String, item: String, types: String)
//
//
//  val logger = LoggerFactory.getLogger(this.getClass)
//
//  def main(args: Array[String]) {
//    val argc = new Array[String](4)
//    argc(0) = "10.0.0.37"
//    argc(1) = "test-1"
//    argc(2) = "test22"
//    argc(3) = "1"
//    val Array(zkQuorum, group, topics, numThreads) = argc
//    val sparkConf = new SparkConf().setAppName("RealtimeCount").setMaster("local[2]")
//    val sc = new SparkContext(sparkConf)
//    val ssc = new StreamingContext(sc, Seconds(5))
//
//
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(x => x._2)
//
//    val sql = "insert into loging_realtime1(vtime,muid,uid,item,category) values (?,?,?,?,?)"
//
//    val tmpdf = lines.map(_.split("\t")).map(x => Loging(x(9).toLong, x(1), x(0), x(3), x(25), x(18).toInt, x(29), x(30), x(28))).filter(x => (x.muid != null && !x.muid.equals("null") && !("").equals(x.muid))).map(x => Record(x.vtime, x.muid, x.uid, getItem(x.category, x.ucp, x.newsId, x.autoSid.toInt, x.dealerId, x.tuanId), getType(x.category, x.ucp, x.newsId, x.autoSid.toInt, x.dealerId, x.tuanId)))
//    tmpdf.filter(x => x.types != null).foreachRDD { rdd =>
//      //rdd.foreach(println)
//      rdd.foreachPartition(partitionRecords => {
//        val connection = ConnectionPool.getConnection.getOrElse(null)
//        if (connection != null) {
//          partitionRecords.foreach(record => process(connection, sql, record))
//          ConnectionPool.closeConnection(connection)
//        }
//      })
//    }
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  def getItem(category: String, ucp: String, newsId: String, autoSid: Int, dealerId: String, tuanId: String): String = {
//    if (category != null && !category.equals("null")) {
//      val pattern = "http://www.ihaha.com/\\d{4}-\\d{2}-\\d{2}/\\d{9}.html"
//      val matcher = ucp.matches(pattern)
//      if (matcher) {
//        ucp.substring(33, 42)
//      } else {
//        null
//      }
//    } else if (autoSid != 0) {
//      autoSid.toString
//    } else if (dealerId != null && !dealerId.equals("null")) {
//      dealerId
//    } else if (tuanId != null && !tuanId.equals("null")) {
//      tuanId
//    } else {
//      null
//    }
//  }
//
//  def getType(category: String, ucp: String, newsId: String, autoSid: Int, dealerId: String, tuanId: String): String = {
//    if (category != null && !category.equals("null")) {
//      val pattern = "100000726;100000730;\\d{9};\\d{9}"
//      val matcher = category.matches(pattern)
//
//      val pattern1 = "http://www.chexun.com/\\d{4}-\\d{2}-\\d{2}/\\d{9}.html"
//      val matcher1 = ucp.matches(pattern1)
//
//      if (matcher1 && matcher) {
//        "nv"
//      } else if (newsId != null && !newsId.equals("null") && matcher1) {
//        "ns"
//      } else if (matcher1) {
//        "ne"
//      } else {
//        null
//      }
//    } else if (autoSid != 0) {
//      "as"
//    } else if (dealerId != null && !dealerId.equals("null")) {
//      "di"
//    } else if (tuanId != null && !tuanId.equals("null")) {
//      "ti"
//    } else {
//      null
//    }
//  }
//
//  def process(conn: Connection, sql: String, data: Record): Unit = {
//    try {
//      val ps: PreparedStatement = conn.prepareStatement(sql)
//      ps.setLong(1, data.vtime)
//      ps.setString(2, data.muid)
//      ps.setString(3, data.uid)
//      ps.setString(4, data.item)
//      ps.setString(5, data.types)
//      ps.executeUpdate()
//    } catch {
//      case exception: Exception =>
//        logger.warn("Error in execution of query" + exception.printStackTrace())
//    }
//  }
//}