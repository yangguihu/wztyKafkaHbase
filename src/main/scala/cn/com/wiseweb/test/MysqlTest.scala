//package cn.com.wiseweb.test
//
///**
//  * Created by yangguihu on 2016/11/25.
//  */
//object MysqlTest {
//
//  def getPositionSubDataMap(sqlPoolConf: SqlPoolConf): util.HashMap[Int, util.LinkedList[PositionSubData]] = {
//    val currentTime: Long = getCurrentTime
//    // todo: 如果数据量比较大的话，判断时间语句直接放到mysql查询的时候
//    val sqlStr =
//      """some sqls"""
//    // 这里告诉我们，写代码的时候不要盲目建立资源池，不要简单的东西复杂化
//    val url = sqlPoolConf.jdbcUrl
//    val user = sqlPoolConf.user
//    val password = sqlPoolConf.password
//    var sqlConn: Connection = null
//    //    var pstmt: PreparedStatement = null
//    //    var rs: ResultSet = null
//    val positionSubDataMap: util.HashMap[Int, util.LinkedList[PositionSubData]] = new util.HashMap[Int, util.LinkedList[PositionSubData]]()
//    try {
//      sqlConn = DriverManager.getConnection(url, user, password)
//      val pstmt: PreparedStatement = sqlConn.prepareStatement(sqlStr)
//      val rs: ResultSet = pstmt.executeQuery()
//      //    var positionSubDataList = ArrayBuffer[PositionSubData]
//      while (rs.next()) {
//        val subId: Long = rs.getLong("sub_id")
//        val spId: String = rs.getString("sp_id")
//        val locationId: String = rs.getString("location_id")
//        val provId: String = rs.getString("prov_id")
//        val cityCode: String = rs.getString("city_code")
//        val intervalTime: Int = rs.getInt("interv")
//        val available: Int = rs.getInt("available")
//        val startTime = rs.getLong("start_time")
//        val endTime = rs.getLong("end_time")
//        val centerLongitude: Double = rs.getDouble("center_longitude")
//        val centerLatitude: Double = rs.getDouble("center_latitude")
//        val radius: Int = rs.getInt("radius")
//        val shape: String = rs.getString("shape")
//        //      printLog.info("cityCode:" + cityCode)
//        val positionSubData: PositionSubData = PositionSubData(subId, spId, locationId, provId, cityCode, intervalTime,
//          available, startTime, endTime, centerLongitude, centerLatitude, radius, shape)
//        //      printLog.info("positionSubData: " + positionSubData)
//        if ((startTime < currentTime) && (endTime > currentTime)) {
//          //        positionSubDataList. += positionSubData
//          val cityCodeArray: Array[String] = cityCode.split("\\|")
//          for (eachCityCode <- cityCodeArray) {
//            if (positionSubDataMap.get(eachCityCode.toInt) == null) {
//              // 代表以该城市为key没有其它景区
//              val positionSubDataList: util.LinkedList[PositionSubData] = new util.LinkedList[PositionSubData]
//              positionSubDataList.add(positionSubData)
//              //            printLog.info("1positionSubDataList：" + positionSubDataList)
//              positionSubDataMap.put(eachCityCode.toInt, positionSubDataList)
//              //            printLog.info("1positionSubDataMap: " + positionSubDataMap)
//            } else {
//              // 代表以该城市为key有其它景区，并且已经记录在案
//              val positionSubDataList: util.LinkedList[PositionSubData] = positionSubDataMap.get(eachCityCode.toInt)
//              positionSubDataList.add(positionSubData)
//              //            printLog.info("2positionSubDataList：" + positionSubDataList)
//              //            printLog.info("2eachCityCode.toInt:" + eachCityCode.toInt)
//              positionSubDataMap.put(eachCityCode.toInt, positionSubDataList)
//            }
//          }
//        }
//      }
//      if (rs != null) {
//        rs.close()
//      }
//      if (pstmt != null) {
//        pstmt.close()
//      }
//    } catch {
//      case e: Exception => {
//        printLog.error("数据库连接错误：e" + e)
//      }
//    } finally {
//      if (sqlConn != null) {
//        sqlConn.close()
//      }
//    }
//    positionSubDataMap
//  }
//
//}
