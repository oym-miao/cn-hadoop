package org.hadoop.conf;


import org.apache.hadoop.conf.Configuration;

/**
 * 获得hadoop连接配置
 * Created by shirukai on 2017/11/8.
 */
public class Conf {
    public static Configuration get (){
        //hdfs的链接地址
        String hdfsUrl = "hdfs://oym2.com:8082";
        //hdfs的名字11
        String hdfsName = "fs.defaultFS";
        //jar包文位置(上一个步骤获得的jar路径)
        String jarPath = "D:\\hadoopProject\\mapreduce\\wordcount\\out\\artifacts\\wordcount_jar\\wordcount.jar";

        Configuration conf = new Configuration();
        conf.set(hdfsName,hdfsUrl);
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.job.jar",jarPath);
        return conf;
    }
}

