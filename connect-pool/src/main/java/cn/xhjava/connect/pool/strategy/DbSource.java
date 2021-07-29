package cn.xhjava.connect.pool.strategy;

import cn.xhjava.connect.pool.domain.ConnectBean;

import java.sql.Connection;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 使用策略模式构建不同数据源
 * @author Xiahu
 * @create 2021-07-29
 */
public interface DbSource {
    LinkedBlockingQueue<Connection> buildConnPool(ConnectBean connectBean);
}
