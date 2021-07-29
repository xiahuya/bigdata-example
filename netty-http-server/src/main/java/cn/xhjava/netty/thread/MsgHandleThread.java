package cn.xhjava.netty.thread;

import cn.xhjava.connect.pool.ConnectionPool;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author Xiahu
 * @create 2021-07-28
 */
@Slf4j
public class MsgHandleThread implements Runnable {

    private ChannelHandlerContext ctx;
    private Object msg;
    private ConnectionPool connPool;

    public MsgHandleThread(ChannelHandlerContext ctx, Object msg, ConnectionPool connPool) {
        this.ctx = ctx;
        this.msg = msg;
        this.connPool = connPool;
    }

    @Override
    public void run() {
        String sql = (String) this.msg;
        Connection conn = connPool.getConnection();
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = conn.createStatement();
            log.info(sql);
            resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        connPool.close(conn, statement, resultSet);
        ctx.writeAndFlush(Unpooled.copiedBuffer("hello", CharsetUtil.UTF_8));
    }
}
