package cn.xhjava.netty;

import cn.xhjava.http.util.BuildSql;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

/**
 * @author Xiahu
 * @create 2020/12/29
 * <p>
 * 自定义客户端handler
 */
public class NettyClientHandler extends ChannelInboundHandlerAdapter {
    private BuildSql buildSql = null;

    public NettyClientHandler(BuildSql buildSql) {
        this.buildSql = buildSql;
    }


    /**
     * 通道就绪时被调用
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String sql = buildSql.buildRandomSql(2018, 2020);
        ctx.writeAndFlush(Unpooled.copiedBuffer(sql, CharsetUtil.UTF_8));
    }

    /**
     * 通道存在读取事件时被调用
     *
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("服务器回复消息: " + (String) msg);
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
