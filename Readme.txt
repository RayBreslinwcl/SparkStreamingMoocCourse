一.环境
1.Centos6.4
2.jdk1.8
3.Hadoop环境
(1)hadoop-2.6.0-cdh5.7.0
(2)flume-1.6.0-cdh5.7.0-bin
(3)kafka_2.11-0.9.0.0
(4)zookeeper-3.4.5-cdh5.7.0
(5)spark-2.1.0-bin-2.6.0-cdh5.7.0(自己编译)
(6)hbase-1.2.0-cdh5.7.0

二.项目:StreamingProduct
1.读取Kafka缓存的数据,进行处理,将结果存储到Hbase

三.项目:WebSpring
1.使用echarts展示:查询Hbase中统计结果

四.ToDO
1.添加Redis缓存,提升查询速度
2.添加日期插件,可以查询指定日期的访问量
3.创建hbase连接池,提升查询速度
4.自动刷新界面
5.扩展mybatis功能


