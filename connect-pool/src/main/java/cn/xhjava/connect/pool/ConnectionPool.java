package cn.xhjava.connect.pool;

import cn.xhjava.connect.pool.domain.ConnectBean;
import cn.xhjava.connect.pool.domain.ConnectPoolConstant;
import cn.xhjava.connect.pool.strategy.DBSourceProducer;
import cn.xhjava.connect.pool.strategy.DbSource;
import cn.xhjava.connect.pool.strategy.db.ElastcSearchSource;
import cn.xhjava.connect.pool.strategy.db.MySqlSource;
import cn.xhjava.connect.pool.util.ConnectBeanLoader;
import lombok.extern.slf4j.Slf4j;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
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
        /*//返回Connection的代理对象
        return (Connection) Proxy.newProxyInstance(ConnectionPool.class.getClassLoader(), conn.getClass().getInterfaces(), new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if (!"close".equals(method.getName())) {
                    return method.invoke(conn, args);
                } else {
                    connPool.put(conn);
                    System.out.println("关闭连接，实际还给了连接池");
                    System.out.println("池中连接数为 " + connPool.size());
                    return null;
                }
            }
        });*/
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
                log.info("归还Connection,当前池中Connection数量: {}", connPool.size());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ConnectionPool connectionPool = new ConnectionPool();

        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                Random random = new Random();

                Connection connection = connectionPool.getConnection();
                log.info("{} 获取到Connection,现在开始休眠", Thread.currentThread().getName());

                try {
                    Thread.sleep((random.nextInt(20) + 10) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                connectionPool.close(connection, null, null);
            }).start();
        }
    }
}
