package cn.com.wiseweb.hbse;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseTest2 {

	/**
	 * 配置ss
	 */
	static Configuration config = null;
	private static HConnection connection = null;
	private static HTableInterface table = null;

	public static void init() throws Exception {
		config = HBaseConfiguration.create();// 配置
		config.set("hbase.zookeeper.quorum", "node1,node2,node3");// zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		connection = HConnectionManager.createConnection(config);
		table = connection.getTable("newspaper_new"); //user
	}



	public static void main(String[] args) throws Exception{
		init();
		Scan scan = new Scan();
//		scan.addFamily(Bytes.toBytes("c1"));
//		scan.addColumn(Bytes.toBytes("url"), Bytes.toBytes("urlhash"));
//		scan.setStartRow(Bytes.toBytes("-1005712230176636433_1477953408000"));
//		scan.setStopRow(Bytes.toBytes("-1011271011660397154_1472293860000"));
		ResultScanner scanner = table.getScanner(scan);
		String url="";
		long urlHash=0L;

		for (Result result : scanner) {
			Bytes.toString(result.getValue(Bytes.toBytes("c1"), Bytes.toBytes("url")));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("c1"), Bytes.toBytes("urlhash"))));
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}
		close();

	}



	public static void close() throws Exception {
		table.close();
		connection.close();
	}

}