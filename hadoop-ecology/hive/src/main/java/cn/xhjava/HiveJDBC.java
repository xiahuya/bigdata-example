package cn.xhjava;

import jodd.exception.ExceptionUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Xiahu
 * @create 2021-06-30
 */
public class HiveJDBC {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://node2:10000", "", "");
        } catch (Exception e) {
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
