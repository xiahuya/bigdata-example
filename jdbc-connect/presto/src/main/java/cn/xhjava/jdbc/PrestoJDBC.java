package cn.xhjava.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author Xiahu
 * @create 2021/1/28
 */
public class PrestoJDBC {

    static String sql = "select * from hid0101_cache_xdcs_pacs_hj.merge_test13";

    public static void main(String[] args) throws Exception {
        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        Connection connection = DriverManager.getConnection("jdbc:presto://node2:7670/hive/hid0101_cache_xdcs_pacs_hj", "presto", null);
        Statement stmt = connection.createStatement();
        connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        connection.close();
    }
}
