package cn.com.wiseweb.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.Serializable;

/**
 * Created by yangguihu on 2016/11/28.
 */
public class SelfConf extends JobConf implements Serializable{
    //private Configuration conf;
    public SelfConf(){}
    public SelfConf(Configuration conf){
        super(conf);
    }
}
