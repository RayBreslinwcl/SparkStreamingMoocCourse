package com.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by Administrator on 2018/11/22.
 * HBase操作工具类
 * 单例模式返回工具类
 */
public class HBaseUtils {
    HBaseAdmin admin=null;
    Configuration configuration=null;


    private HBaseUtils(){
        configuration=new Configuration();
        configuration.set("hbase.rootdir",PropertiesUtils.getProperties().getProperty("hbase.rootdir"));
        configuration.set("hbase.zookeeper.quorum",PropertiesUtils.getProperties().getProperty("hbase.zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort",PropertiesUtils.getProperties().getProperty("hbase.zookeeper.property.clientPort"));
//        configuration.set("hbase.regionserver.dns.nameserver","hadoop");

        try {
            admin=new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private volatile static HBaseUtils instance=null;

    public static HBaseUtils getInstance(){

        if (instance==null){
           synchronized (HBaseUtils.class){
               if(instance==null){
                   instance=new HBaseUtils();
               }
           }
        }
        return  instance;

    }

    /**
     * 依据表名字获得表
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName){
        HTable table=null;

        try {
            table=new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  table;
    }

    /**
     * 添加一列
     * @param tableName Hbase表名
     * @param rowkey rowkey
     * @param cf HBase的columnnfamily
     * @param column HBase列
     * @param value 写入HBase表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value) {
        HTable table=getTable(tableName);
        Put put=new Put(Bytes.toBytes(rowkey));

//        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询hbase的表20181118
     * @param tablename 表单名称
     * @param dayCourse 关键列包含字段
     * @return
     * @throws Exception
     */
    public Map<String,Long> query(String tablename,String dayCourse) throws IOException {
        Map<String,Long> map=new HashMap<String, Long>();
        HTable table=getTable(tablename);
        String cf="info";
        String column="click_count";
        Scan scan=new Scan();

        Filter filter=new PrefixFilter(Bytes.toBytes(dayCourse));
        scan.setFilter(filter);

        ResultScanner rs=table.getScanner(scan);
        for (Result result:rs){
            String row =Bytes.toString(result.getRow());
            long clickcount=Bytes.toLong(result.getValue(cf.getBytes(),column.getBytes()));
            map.put(row,clickcount);
        }
        return map;
    }

    public static void main(String[] args) {
//        【pass】
//        HTable table= HBaseUtils.getInstance().getTable("student2");
//        System.out.println(table.getName().getNameAsString());


        //输入hbase测试
        String tablename="web_clickcount";
        String rowkey="20181122_88";
        String cf="info";
        String column="click_count";
        String value="20";

        HBaseUtils hu=  HBaseUtils.getInstance();
        hu.put(tablename,rowkey,cf,column,value);


    }
}
