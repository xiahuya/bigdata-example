package cn.xhjava.jdbc;

import java.sql.*;

/**
 * @author Xiahu
 * @create 2021/3/5
 * 星环环境JDBC
 */
public class XingHuanJDBC {
    //Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //Hive2 JDBC URL with LDAP
        String jdbcURL = "jdbc:hive2://192.168.0.101:10000/default";

        Connection conn = DriverManager.getConnection(jdbcURL);
        Statement  stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("show databases");
        ResultSetMetaData rsmd = rs.getMetaData();
        int size = rsmd.getColumnCount();
        while(rs.next()) {
            StringBuffer value = new StringBuffer();
            for(int i = 0; i < size; i++) {
                value.append(rs.getString(i+1)).append("\t");
            }
            System.out.println(value.toString());
        }
        rs.close();
        stmt.close();
        conn.close();
    }
}
