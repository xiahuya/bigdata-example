package cn.xhjava.clickhouse.threads;

import cn.xhjava.clickhouse.util.BuildSql;
import cn.xhjava.connect.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-08-01
 */
@Slf4j
public class ClickHouseJdbcRequestThread implements Runnable {
    private TreeSet<Double> treeSet;
    private Integer loopNum;
    private CountDownLatch cdl;
    private ConnectionPool connectionPool;
    private int sqlVersion;

    public ClickHouseJdbcRequestThread(Integer loopNum, CountDownLatch cdl, ConnectionPool connectionPool, TreeSet<Double> treeSet, int sqlVersion) {
        this.loopNum = loopNum;
        this.cdl = cdl;
        this.connectionPool = connectionPool;
        this.treeSet = treeSet;
        this.sqlVersion = sqlVersion;
    }


    @Override
    public void run() {
        for (int i = 0; i < loopNum; i++) {
            long startTime = -1;
            long endTime = -1;
            Connection conn = connectionPool.getConnection();
            Statement statement = null;
            ResultSet resultSet = null;
            BuildSql buildSql = new BuildSql();
            String sql = buildSql.getSql(sqlVersion);

            try {
                statement = conn.createStatement();
                startTime = System.currentTimeMillis();
                log.info("{} 请求ClickHouse SQL: {}", Thread.currentThread().getName(), sql);
                resultSet = statement.executeQuery(sql);
                endTime = System.currentTimeMillis();
                double time = (endTime - startTime) / 1000.0;

                log.info("{} 请求ClickHouse SQL 耗费时间: {} s", Thread.currentThread().getName(), time);
                treeSet.add(time);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                connectionPool.close(conn, statement, resultSet);
            }
        }
        cdl.countDown();
    }
}
