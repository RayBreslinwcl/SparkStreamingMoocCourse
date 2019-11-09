package com.imooc.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Administrator on 2019/11/8.
 */
public class HBaseUtils_threadpool {
    private static Connection connection;
    private static Configuration configuration;
    private static HBaseUtils_threadpool hBaseUtils_threadpool;
    private static Properties properties;

    /**
     * 创建连接池并且初始化环境配置
     */
    public void init(){
        if (configuration==null){
            configuration= HBaseConfiguration.create();
        }
        try {
            configuration.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
            configuration.set("hbase.zookeeper.quorum","hadoop:2181");
            if (connection==null||connection.isClosed()){
                connection= ConnectionFactory.createConnection(configuration);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接池
     */
    public static void close(){

        if(connection!=null) try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HBaseUtils_threadpool (){}


    /**
     * 唯一实例，线程安全，保证连接池唯一
     * @return
     */
    public static HBaseUtils_threadpool getInstance(){
        if (hBaseUtils_threadpool==null){
            synchronized (HBaseUtils_threadpool.class){
                if(hBaseUtils_threadpool ==null){
                    hBaseUtils_threadpool=new HBaseUtils_threadpool();
                    hBaseUtils_threadpool.init();
                }
            }
        }
        return hBaseUtils_threadpool;
    }


    /**
     * 1.获取单条数据
     * @param tablename 表名
     * @param row 行键
     * @return
     * @throws IOException
     */
    public static Result getRow(String tablename, byte[] row) throws IOException{
        Table table = null;
        Result result = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Get get = new Get(row);
            result = table.get(get);
        }finally {
            table.close();
        }
        return result;
    }


    /**
     * 2.查询多行信息
     * @param tablename 表名
     * @param rows 行键组信息
     * @return
     * @throws IOException
     */
    public static Result[] getRows(String tablename,List<byte[]> rows) throws  IOException{
        Table table = null;
        List<Get> gets = null;
        Result[] results = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            gets = new ArrayList<Get>();
            for (byte[] row : rows){
                if(row!=null){
                    gets.add(new Get(row));
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
        }
        return results;
    }


    /**
     * 3. 获取整个表的数据
     * @param tablename 表名称
     * @return
     * @throws IOException
     */
    public static ResultScanner getTable(String tablename) throws IOException {
        Table table=null;
        ResultScanner results=null;

        try {
            table=connection.getTable(TableName.valueOf(tablename));
            Scan scan=new Scan();
            scan.setCaching(10000);
            results = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
        }

        return results;
    }


    /**
     * 4.单行插入数据
     * @param tablename 表名
     * @param rowkey 行键
     * @param family 列簇
     * @param columns 列
     * @throws IOException
     */
    public static void put(String tablename, String rowkey, String family, Map<String,String> columns) throws IOException{
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Put put = new Put(rowkey.getBytes());
            for (Map.Entry<String,String> entry : columns.entrySet()){
                put.addColumn(family.getBytes(),entry.getKey().getBytes(),entry.getValue().getBytes());
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
            close();
        }
    }

}
