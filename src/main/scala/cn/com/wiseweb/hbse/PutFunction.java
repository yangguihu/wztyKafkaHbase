package cn.com.wiseweb.hbse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;
/**
 * Created by yangguihu on 2016/11/16.
 */
public class PutFunction implements Function<String, Put>{
    private static final long serialVersionUID = 1L;

    public PutFunction() {}

    public Put call(String v) throws Exception {
        String[] cells = v.split(",");
        Put put = new Put(Bytes.toBytes(cells[0]));
        put.add(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]), Bytes.toBytes(cells[3]));
        return put;
    }
}
