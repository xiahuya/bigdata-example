package cn.xhjava.http.threads;

import cn.xhjava.http.util.BuildSql;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-07-12
 */
@Slf4j
public class JdbcRequestByThread implements Runnable {

    private CountDownLatch cdl;
    private int loopNumber;
    private String apiUrl;
    private BuildSql buildSql;


    public JdbcRequestByThread(String apiUrl, int loopNumber, CountDownLatch cdl) {
        this.cdl = cdl;
        this.loopNumber = loopNumber;
        this.apiUrl = apiUrl;
        buildSql = new BuildSql();
    }

    @Override
    public void run() {
        for (int i = 0; i < loopNumber; i++) {
            long startTime = System.currentTimeMillis();
            String sql = buildSql.buildRandomSql(2010, 2021);
            executor(apiUrl, sql);
            long endTime = System.currentTimeMillis();
            log.info("请求ElastucSearch JDBC 耗费时间: {} s", (endTime - startTime) / 1000.0);
            log.info(sql);
            log.info("===============================================================");
        }
        cdl.countDown();
    }


    public void executor(String elasticsearchAddress, String sql) {
        try {
            String address = "jdbc:es://http://" + elasticsearchAddress;
            Connection connection = DriverManager.getConnection(address);
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(buildSql.buildRandomSql(2010, 2021));
            statement.close();
            results.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
