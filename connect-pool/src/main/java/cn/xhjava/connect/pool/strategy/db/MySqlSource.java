package cn.xhjava.connect.pool.strategy.db;

import cn.xhjava.connect.pool.domain.ConnectBean;
import cn.xhjava.connect.pool.strategy.DbSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Xiahu
 * @create 2021-07-29
 */
@Slf4j
public class MySqlSource implements DbSource {
    private LinkedBlockingQueue<Connection> connPool = null;

    @Override
    public LinkedBlockingQueue<Connection> buildConnPool(ConnectBean connectBean) {
        connPool = new LinkedBlockingQueue<>();
        try {
            Class.forName(connectBean.getConnDriver());
            for (int i = 0; i < connectBean.getConnPoolSize(); i++) {
                connPool.put(DriverManager.getConnection(connectBean.getConnJdbcUrl(), connectBean.getConnUsername(), connectBean.getConnPassword()));
                log.info("初始化数据库连接池,第 {} 个MySql连接添加到池中", i);
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
