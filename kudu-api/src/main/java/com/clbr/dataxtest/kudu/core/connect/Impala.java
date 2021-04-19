package com.clbr.dataxtest.kudu.core.connect;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author XIAHU
 * @create 2019/9/5
 */

public class Impala {
    private Connection connection;
    private Statement statement;
    public void Impala() {
        try {
            Class.forName("com.cloudera.impala.jdbc41.Driver");
            connection = DriverManager.getConnection("jdbc:impala://172.22.77.108:21050", "KLBR", "CQMYG@#14dss");
            statement = connection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void updataTable(String createSQL) {
        try {
            statement.executeUpdate(createSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}