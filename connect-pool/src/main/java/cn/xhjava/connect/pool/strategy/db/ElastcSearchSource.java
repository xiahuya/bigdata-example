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
 * @create 2021-07-29
 */
@Slf4j
public class ElastcSearchSource implements DbSource {
    private LinkedBlockingQueue<Connection> connPool = null;

    @Override
    public LinkedBlockingQueue<Connection> buildConnPool(ConnectBean connectBean) {
        connPool = new LinkedBlockingQueue<>();
        for (int i = 1; i <= connectBean.getConnPoolSize(); i++) {
            List<String> jdbcUrls = Arrays.asList(connectBean.getConnJdbcUrl().split(","));
            try {
                String jdbcUrl = jdbcUrls.get(i % jdbcUrls.size());
                connPool.put(DriverManager.getConnection(jdbcUrl));
                log.debug("初始化数据库连接池,第 {} 个ElasticSearch连接添加到池中", i);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        return connPool;
    }
}
