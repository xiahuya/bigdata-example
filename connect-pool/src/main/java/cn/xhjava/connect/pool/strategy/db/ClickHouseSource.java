package cn.xhjava.connect.pool.strategy.db;

import cn.xhjava.connect.pool.domain.ConnectBean;
import cn.xhjava.connect.pool.strategy.DbSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Xiahu
 * @create 2021-08-01
 * jdbc:clickhouse://192.168.0.113:9000/default
 */
@Slf4j
public class ClickHouseSource implements DbSource {
    private LinkedBlockingQueue<Connection> connPool = null;

    @Override
    public LinkedBlockingQueue<Connection> buildConnPool(ConnectBean connectBean) {
        connPool = new LinkedBlockingQueue<>();
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            List<String> jdbcUrls = Arrays.asList(connectBean.getConnJdbcUrl().split(","));

            for (int i = 0; i < connectBean.getConnPoolSize(); i++) {
                String jdbcUrl = jdbcUrls.get(i % jdbcUrls.size());
                connPool.put(DriverManager.getConnection(jdbcUrl, connectBean.getConnUsername(), connectBean.getConnPassword()));
                log.debug("初始化数据库连接池,第 {} 个ClickHouse连接添加到池中", i);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connPool;
    }
}
