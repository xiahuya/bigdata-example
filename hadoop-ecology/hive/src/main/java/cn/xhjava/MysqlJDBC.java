package cn.xhjava;

import jodd.exception.ExceptionUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Xiahu
 * @create 2021-06-30
 */
public class MysqlJDBC {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://192.168.0.114:3306/?useSSL=false", "root", "root");
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
