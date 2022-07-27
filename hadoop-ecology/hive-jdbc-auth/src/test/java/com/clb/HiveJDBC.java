package com.clb;

import java.sql.*;

/**
 * @author Xiahu
 * @create 2021-06-30
 */
public class HiveJDBC {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://192.168.0.113:10000", "", "");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from ddm.mdm_patientempi");
            while (resultSet.next()){
                System.out.println(resultSet.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
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
