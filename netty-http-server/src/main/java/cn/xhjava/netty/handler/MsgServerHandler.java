package cn.xhjava.netty.handler;

import cn.xhjava.connect.pool.ConnectionPool;
import cn.xhjava.netty.thread.MsgHandleThread;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;


/**
 * @author Xiahu
 * @create 2021-07-28
 */
@Slf4j
public class MsgServerHandler extends ChannelInboundHandlerAdapter {
    private ExecutorService executorService;
    private ConnectionPool connPool;

    public MsgServerHandler(ExecutorService executorService,ConnectionPool connPool) {
        this.executorService = executorService;
        this.connPool = connPool;
    }


    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} 通道注册成功!", ctx.channel().remoteAddress());
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} 通道注销成功!", ctx.channel().remoteAddress());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} 客户端连接成功!", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} 客户端销毁成功!", ctx.channel().remoteAddress());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        executorService.submit(new MsgHandleThread(ctx, msg,connPool));
    }

}
