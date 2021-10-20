package cn.xhjava.connect.pool;

import cn.xhjava.connect.pool.domain.ConnectBean;
import cn.xhjava.connect.pool.strategy.DBSourceProducer;
import cn.xhjava.connect.pool.strategy.DbSource;
import cn.xhjava.connect.pool.strategy.db.ClickHouseSource;
import cn.xhjava.connect.pool.strategy.db.ElastcSearchSource;
import cn.xhjava.connect.pool.strategy.db.MySqlSource;
import cn.xhjava.connect.pool.strategy.db.OracleSource;
import cn.xhjava.connect.pool.util.ConnectBeanLoader;
import lombok.extern.slf4j.Slf4j;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingQueue;

import static cn.xhjava.connect.pool.domain.ConnectPoolConstant.*;

/**
 * @author Xiahu
 * @create 2021-07-29
 */
@Slf4j
public class ConnectionPool {
    //阻塞队列,用于多线程安全访问
    private static LinkedBlockingQueue<Connection> connPool = null;

    static {
        ConnectBean connectBean = ConnectBeanLoader.loadConnectBean();
        DBSourceProducer producer = new DBSourceProducer(connectBean);
        DbSource source = null;
        switch (connectBean.getConnType()) {
            case ELASTIC_SEARCH:
                source = new ElastcSearchSource();
                break;
            case MYSQL:
                source = new MySqlSource();
                break;
            case CLICK_HOUSE:
                source = new ClickHouseSource();
                break;
            case ORACLE:
                source = new OracleSource();
                break;
        }
        connPool = producer.produce(source);
    }

    public Connection getConnection() {
        Connection conn = null;
        try {
            conn = connPool.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return conn;

    }


    public void close(Connection conn, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }
            if (connPool != null) {
                connPool.put(conn);
                log.debug("归还Connection,当前池中Connection数量: {}", connPool.size());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
