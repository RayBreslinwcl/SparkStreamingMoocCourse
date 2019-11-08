package com.imooc.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by Administrator on 2019/11/8.
 * Hbase的增删改查，直接测试，这个是个独立的测试类，测试对象就是本身
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HbaseUtils_DDLtest {

    /**
     * 1.创建namespace和table
     * 参考hbase shell命令create 'namespace:tablename','column family1','column family1'
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    @SuppressWarnings("resource")
	@Test
    public void createTable() throws Exception {
        String nameSpace = "hadoop";
        String tbName = "actor";
        String colFamily = "info";
        // 1.读取配置文件
        //默认读取的是："E:\Tools\apache-maven-3.3.9\Repository\org\apache\hbase\hbase-common\1.2.0-cdh5.7.0\hbase-common-1.2.0-cdh5.7.0.jar"中
        //的hbase-default.xml，也可以把hbase-site.xml放到resources中
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        // 2.创建一个管理员，管理表格的创建和删除
        HBaseAdmin admin = new HBaseAdmin(conf);
        // 3.创建命名空间描述器
        NamespaceDescriptor ns = NamespaceDescriptor.create(nameSpace).build();
        // 4.创建表格，表格对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + tbName);
        // 创建表的描述器
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        // 5.创建列簇的描述器
        HColumnDescriptor columnDesc = new HColumnDescriptor(colFamily);
        // 6.将列簇添加到表的描述器
        tableDesc.addFamily(columnDesc);
        // 7.创建命名空间，创建表
        admin.createNamespace(ns);
        admin.createTable(tableDesc);
        System.out.println("创建表   " + nameSpace + ":" + tbName + "  成功");
    }


    /**
     * 2.删除表
     */
	@Test
    public void deleteTable() throws Exception {
        String nameSpace = "hadoop";
        String tbName = "actor";
        // 读取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        // 创建一个管理员，管理表格的创建和删除
        @SuppressWarnings("resource")
        HBaseAdmin admin = new HBaseAdmin(conf);
        // 创建表格对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + tbName);
        // 删除操做
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println("表已经删除");
        admin.deleteNamespace(nameSpace);
        System.out.println("命名空间已经删除");
    }


    /**
     * 3.添加数据
     * put 'namespace:tableName','rowKey','column falmily:column','value'
     */
	@Test
    public void putData() throws Exception {
        String nameSpace = "hadoop";
        String tbName = "actor";
        String colFamily = "info";
        String rowkey = "10086";// 10010
        String column = "name";
        String value = "李四3";
        // 1.读取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        // 2.创建表对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + tbName);
        @SuppressWarnings("resource")
        HTable table = new HTable(conf, tableName);

        // 3.创建put对象
        Put put = new Put(Bytes.toBytes(rowkey));

        // 4.获取所有的列簇，封装到数组中，然后进行判断
        // 如果是该列簇下的，才插入到相应的列簇。所以需要判断
        HColumnDescriptor[] columns = table.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < columns.length; i++) {
            String columnfamilys = columns[i].getNameAsString();
            // 判断
            if (columnfamilys.equals(colFamily)) {
                // 添加数据
                put.add(Bytes.toBytes(columnfamilys), Bytes.toBytes(column), Bytes.toBytes(value));
                put.add(Bytes.toBytes(columnfamilys), Bytes.toBytes("age"), Bytes.toBytes("23"));
            }
        }

        // 5.提交
        table.put(put);

        // 6.关闭资源0607update
        table.close();
        System.out.println("添加数据成功");
    }


    /**
     * 4.删除数据
     * delete 'namespace:tablename','rowkey','columnfamily:column'
     */
	@Test
    public void deleteData() throws Exception {
        String nameSpace = "hadoop";
        String tbName = "actor";
        String colFamily = "info";
        String rowkey = "10086";
        String column = "name";
        // 读取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        // 创建表对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + tbName);
        @SuppressWarnings("resource")
        HTable table = new HTable(conf, tableName);
        // 创建delete对象
        Delete del = new Delete(Bytes.toBytes(rowkey));
        // 删除某列的数据
        del.deleteColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column));
        // 删除某一行的数据,因为del对象已经指定了rowkey了，直接表调用就可以
        // 提交
        table.delete(del);
        System.out.println("删除数据成功");
    }



    /**
     * 5.查询数据get
     */
	@Test
    public void getData() throws Exception {
        String nameSpace = "hadoop";
        String tbName = "actor";
        String colFamily = "info";
        String rowkey = "10086";
        String column = "name";
        // 读取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        // 创建表对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + tbName);
        @SuppressWarnings("resource")
        HTable table = new HTable(conf, tableName);
        //创建get对象
        Get get = new Get(Bytes.toBytes(rowkey));  //指定rowkey查询
        //get.addFamily(Bytes.toBytes(colFamily)); //指定列簇查询
        //get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column)); //指定列查询
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("列簇："+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列："+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳："+cell.getTimestamp());
        }
    }



    /**
     * 6.查询数据scan
     */
	@Test
    public void scanData() throws Exception {
        String nameSpace = "hadoop";
        String tbName = "actor";
        // 读取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        // 创建表对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + tbName);
        @SuppressWarnings("resource")
        HTable table = new HTable(conf, tableName);
        //创建scan对象
        Scan scan = new Scan();
        //【这两个为查询起始和终止位置，2018/6/7】
        //scan.setStartRow(Bytes.toBytes("10010"));
        //scan.setStopRow(Bytes.toBytes("10080"));
//		*********************************
//		【update 20180607】
        //查询那些Column列
//		scan.addColumn(family, qualifier)
        //查询那些family
//		scan.addFamily(family)
//		**********************************
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            //打印每一条数据
            System.out.println("rowkey："+Bytes.toString(result.getRow()));
            //result.rawCells()得到一行的所有单元格
            for (Cell cell :result.rawCells() ) {
                System.out.println("列簇："+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列："+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳："+cell.getTimestamp());
            }
            System.out.println("----------------------------------------");
        }

    }

    //================================HBASE1.0+=======================================
    public static Configuration getHBaseConf(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://hadoop:8020/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        return conf;
    }

    /**
     * 备注：对于hbase1.+版本，建议采用下面接口创建连接操作Hbase
     * createTable：创建namespace，表名，列簇。
     * @throws Exception
     */
    @SuppressWarnings("resource")
	@Test
    public static void createTable_1plus() throws Exception {
        String nameSpace = "hadoop";
        String tbName = "actor_1pluse";
        String colFamily = "info";
        // 1.读取配置文件
        Configuration conf=HbaseUtils_DDLtest.getHBaseConf();

        Connection connection;
        connection = ConnectionFactory.createConnection(conf);

        // 2.创建一个管理员，管理表格的创建和删除
//        HBaseAdmin admin = new HBaseAdmin(conf);
        Admin admin=connection.getAdmin();

        // 3.创建命名空间描述器
        NamespaceDescriptor ns = NamespaceDescriptor.create(nameSpace).build();

        // 4.创建表格，表格对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + tbName);
        // 创建表的描述器
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        // 5.创建列簇的描述器
        HColumnDescriptor columnDesc = new HColumnDescriptor(colFamily);
        // 6.将列簇添加到表的描述器
        tableDesc.addFamily(columnDesc);
        // 7.创建命名空间，创建表
        admin.createNamespace(ns);
        admin.createTable(tableDesc);
        System.out.println("创建表   " + nameSpace + ":" + tbName + "  成功");

//        TableName tablename=TableName.valueOf("")
    }


}
