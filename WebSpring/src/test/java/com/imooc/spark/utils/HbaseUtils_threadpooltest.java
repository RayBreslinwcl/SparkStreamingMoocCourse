package com.imooc.spark.utils;

import com.imooc.utils.HBaseUtils_threadpool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2019/11/8.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HbaseUtils_threadpooltest {

    String cf="info";
    String qualifier="click_count";


    /**
     * 测试查询单条数据
     * 测试结果：
     * rowkey=20191107_146, clickcount=2
     */
    @Test
    public void test01(){
        HBaseUtils_threadpool hBaseUtils_threadpool=HBaseUtils_threadpool.getInstance();
        try {
            Result result=hBaseUtils_threadpool.getRow("imooc_course_clickcount","20191107_146".getBytes());

            String row= Bytes.toString(result.getRow()) ;
            long clickcount=Bytes.toLong(result.getValue(cf.getBytes(),qualifier.getBytes()));
            System.out.println("rowkey="+row+", clickcount="+clickcount);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 2.测试查询多条信息
     */
    @Test
    public void test02(){
        HBaseUtils_threadpool hBaseUtils_threadpool=HBaseUtils_threadpool.getInstance();
        List<byte[]> rows=new ArrayList<>();
        rows.add("20171111_8".getBytes());
        rows.add("20191107_112".getBytes());
        Result[] result=null;
        try {
            result=hBaseUtils_threadpool.getRows("imooc_course_clickcount",rows);
            for (Result r : result) {
                String row= Bytes.toString(r.getRow()) ;
                long clickcount=Bytes.toLong(r.getValue(cf.getBytes(),qualifier.getBytes()));
                System.out.println("rowkey="+row+", clickcount="+clickcount);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            hBaseUtils_threadpool.close();
        }
    }


    /**
     * 3.测试获得表的所有数据
     * 测试结果：
     *==========================================
     rowkey=20191107_145	列簇：info	列：click_count	值：       	时间戳：1573140666567
     ==========================================
     rowkey=20191107_146	列簇：info	列：click_count	值：       	时间戳：1573140666564
     ==========================================
      */
    @Test
    public void test03(){
        HBaseUtils_threadpool hBaseUtils_threadpool=HBaseUtils_threadpool.getInstance();

        ResultScanner results=null;
        try {
            results=hBaseUtils_threadpool.getTable("imooc_course_clickcount");
            for (Result r : results) {
                System.out.print("rowkey="+Bytes.toString(r.getRow())+"\t");
                for(Cell cell: r.rawCells()){
                    System.out.print("列簇："+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                    System.out.print("列："+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                    System.out.print("值："+Bytes.toString(CellUtil.cloneValue(cell))+"\t");
                    System.out.println("时间戳："+cell.getTimestamp());
                }
                System.out.println("==========================================");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            results.close();
            HBaseUtils_threadpool.close();
        }
    }

    /**
     * 4.测试插入值
     * 测试结果：
     * hbase shell查询
     *  20191108_131                column=info:click_count, timestamp=1573217751573, value=19
     */
    @Test
    public void test04(){
        HBaseUtils_threadpool hBaseUtils_threadpool=HBaseUtils_threadpool.getInstance();

        try {
            Map<String,String> columns=new HashMap<>();
            columns.put("click_count","19");
            hBaseUtils_threadpool.put("imooc_course_clickcount","20191108_131","info",columns);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            HBaseUtils_threadpool.close();
        }
    }

}
