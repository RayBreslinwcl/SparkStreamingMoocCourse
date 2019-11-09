package com.imooc.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2018/9/26.
 * HBase操作工具类:Java工具类建议采用单例模式封装
 *
 */

public class HBaseUtils {
    static HBaseAdmin admin=null;
    static Configuration configuration=null;

    //创建一个定长线程池，可控制线程最大并发数，超出的线程会在队列中等待
    static ExecutorService executorService=null;

    /**
     * 私有改造方法
     */
    private HBaseUtils(){
        configuration=new Configuration();
//        configuration.set("hbase.rootdir","hdfs://bigdata.ibeifeng.com:8020/hbase");
//        configuration.set("hbase.zookeeper.quorum","bigdata.ibeifeng.com:2181");
        configuration.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        configuration.set("hbase.zookeeper.quorum","hadoop:2181");

        try {
            admin =new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static HBaseUtils instance=null;

    public static synchronized HBaseUtils getInstance(){
        if (null==instance){
            instance=new HBaseUtils();
        }
        return instance;
    }

    public HTable getTable(String tableName){
        HTable table=null;

        try {
            table =new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 1.查询：通过日期：20191109，查询当前课程以及对应的个数
     * @param tablename 表名
     * @param dayCourse 日期20191109
     * @return
     * @throws Exception
     */
    public Map<String,Long> query(String tablename, String dayCourse) throws Exception{
        Map<String,Long> map =new HashMap<>();

        HTable table=getTable(tablename);
        String cf="info";
        String qualifier="click_count";
        Scan scan=new Scan();

        Filter filter=new PrefixFilter(Bytes.toBytes(dayCourse));
        scan.setFilter(filter);

        ResultScanner rs=table.getScanner(scan);
        for (Result result:rs){
            String row=Bytes.toString(result.getRow()) ;
            long clickcount=Bytes.toLong(result.getValue(cf.getBytes(),qualifier.getBytes()));
            map.put(row,clickcount);
        }
        return map;
    }
    public static void main(String[] args) throws Exception{
//        HTable table= HBaseUtils.getInstance().getTable("imooc_course_clickcount");
//
//        System.out.println(table.getName().getNameAsString());

        Map<String ,Long>map=HBaseUtils.getInstance().query("imooc_course_clickcount","20191107_146");

        for (Map.Entry<String,Long> entry:map.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }
}
