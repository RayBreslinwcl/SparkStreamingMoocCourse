package com.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;

/**
 * Created by Administrator on 2018/12/3.
 */
public class PhoenixJavaAPI {

    public static void main(String[] args) {
        PhoenixJavaAPI api=new PhoenixJavaAPI();
        api.create();
        api.upsert();
//        api.query();
    }
    public static Connection getConnection() {
        try {
            // load driver
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

            // get connection
            // jdbc 的 url 类似为 jdbc:phoenix [ :<zookeeper quorum> [ :<port number> ] [ :<root node> ] ]，
            // 需要引用三个参数：hbase.zookeeper.quorum、hbase.zookeeper.property.clientPort、and zookeeper.znode.parent，
            // 这些参数可以缺省不填而在 hbase-site.xml 中定义。
//            return DriverManager.getConnection("jdbc:phoenix:bigdata.pc1:2181/hbase");
            return DriverManager.getConnection("jdbc:phoenix:hadoop:2181/hbase");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
//    @Test
    public  void create() {
        Connection conn = null;
        try {
            // get connection
            conn = PhoenixJavaAPI.getConnection();

            // check connection
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }

            // check if the table exist
            ResultSet rs = conn.getMetaData().getTables(null, null, "USER01",
                    null);
            if (rs.next()) {
                System.out.println("table user01 is exist...");
                return;
            }
            // create sql
            String sql = "CREATE TABLE user01 (id varchar PRIMARY KEY,INFO.name varchar ,INFO.passwd varchar)";

            PreparedStatement ps = conn.prepareStatement(sql);

            // execute
            ps.execute();
            System.out.println("create success...");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
//    @Test
    public  void upsert() {

        Connection conn = null;
        try {
            // get connection
            conn = PhoenixJavaAPI.getConnection();

            // check connection
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }

            // create sql
            String sql = "upsert into user01(id, INFO.name, INFO.passwd) values('001', 'admin', 'admin')";

            PreparedStatement ps = conn.prepareStatement(sql);

            // execute upsert
            String msg = ps.executeUpdate() > 0 ? "insert success..."
                    : "insert fail...";

            // you must commit
            conn.commit();
            System.out.println(msg);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

//    @Test
    public  void query() {

        Connection conn = null;
        try {
            // get connection
            conn = PhoenixJavaAPI.getConnection();

            // check connection
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }

            // create sql
            String sql = "select * from us_population";

            PreparedStatement ps = conn.prepareStatement(sql);

            ResultSet rs = ps.executeQuery();

            System.out.println("state" + "\t" + "city" + "\t" + "population");
            System.out.println("======================");

            if (rs != null) {
                while (rs.next()) {
//                    System.out.print(rs.getString("STATE") + "\t");
//                    System.out.print(rs.getString("CITY") + "\t");
//                    System.out.println(rs.getString("POPULATION"));
                    System.out.print(rs.getString("state") + "\t");
                    System.out.print(rs.getString("city") + "\t");
                    System.out.println(rs.getString("population"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

//    @Test
    public  void delete() {

        Connection conn = null;
        try {
            // get connection
            conn = PhoenixJavaAPI.getConnection();

            // check connection
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }

            // create sql
            String sql = "delete from user01 where id='001'";

            PreparedStatement ps = conn.prepareStatement(sql);

            // execute upsert
            String msg = ps.executeUpdate() > 0 ? "delete success..."
                    : "delete fail...";

            // you must commit
            conn.commit();
            System.out.println(msg);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
//    @Test
    public  void drop() {

        Connection conn = null;
        try {
            // get connection
            conn = PhoenixJavaAPI.getConnection();

            // check connection
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }

            // create sql
            String sql = "drop table user01";

            PreparedStatement ps = conn.prepareStatement(sql);

            // execute
            ps.execute();

            System.out.println("drop success...");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
