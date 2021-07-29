package cn.xhjava.netty;

import cn.xhjava.http.util.BuildSql;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * @author Xiahu
 * @create 2020/12/29
 */
public class NettyClient {
    public static void main(String[] args) throws InterruptedException {
        BuildSql buildSql = new BuildSql();
        for (int i = 0; i < 1000; i++) {
            new Thread(() -> {
                NioEventLoopGroup eventExecutors = new NioEventLoopGroup();
                //创建启动对象
                Bootstrap bootstrap = new Bootstrap();
                try {
                    //设置相关参数
                    bootstrap
                            .group(eventExecutors)
                            .channel(NioSocketChannel.class)
                            .handler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) throws Exception {
                                    ChannelPipeline pipeline = ch.pipeline();
                                    //绑定编码器
                                    pipeline.addLast("decoder", new StringDecoder());
                                    //绑定解码器
                                    pipeline.addLast("encoder", new StringEncoder());
                                    //自定义处理器
                                    ch.pipeline().addLast(new NettyClientHandler(buildSql));
                                }
                            });
                    ChannelFuture future = bootstrap.connect("0.0.0.0", 8080).sync();

                    future.channel().closeFuture().sync();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    //优雅的关闭
                    eventExecutors.shutdownGracefully();
                }
            }).start();
        }
    }
}
