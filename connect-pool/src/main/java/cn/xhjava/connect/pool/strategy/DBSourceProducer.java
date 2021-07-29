package cn.xhjava.connect.pool.strategy;

import cn.xhjava.connect.pool.domain.ConnectBean;

import java.sql.Connection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Xiahu
 * @create 2021-07-29
 */
public class DBSourceProducer {
    private ConnectBean connectBean;

    public DBSourceProducer(ConnectBean connectBean) {
        this.connectBean = connectBean;
    }


    public LinkedBlockingQueue<Connection> produce(DbSource dbSource) {
        return dbSource.buildConnPool(connectBean);
    }
}
