package cn.xhjava.netty;

import cn.xhjava.connect.pool.ConnectionPool;
import cn.xhjava.netty.handler.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
//基于netty 开发http服务,使用http访问无法连接,需要使用socket编程
@Slf4j
public class HttpServer {
    public static void main(String[] args) {
        new HttpServer().serviceStart(8080);
    }


    public void serviceStart(int port) {
        EventLoopGroup workGroup = new NioEventLoopGroup();
        ExecutorService executorService = Executors.newFixedThreadPool(500);
        ConnectionPool connPool = new ConnectionPool();
        try {
            //具体业务逻辑
            ServerBootstrap bootstrap = new ServerBootstrap();
            //配置启动参数
            bootstrap.group(workGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            //ch.pipeline().addLast("codec", new HttpServerCodec());//http编解码器
                            //对httpmsg进行聚合 转化为fullHttpRequest 或者fullHttpResponse并设置最大数据长度
                            //ch.pipeline().addLast("aggregator", new HttpObjectAggregator(512 * 1024));
                            //ch.pipeline().addLast("logging", new FilterLogginglHandler());//日志
                            //拦截器配置
                            //ch.pipeline().addLast("Interceptor", new InterceptorHandler());
                            //ch.pipeline().addLast("HttpServerHandler", new HttpServerHandler());

                            ch.pipeline().addLast("codec", new StringDecoder());
                            ch.pipeline().addLast("MsgServerHandler", new MsgServerHandler(executorService,connPool));
                        }
                    }).option(ChannelOption.SO_BACKLOG, 0);

            //绑定端口
            ChannelFuture future = bootstrap.bind(port).sync();
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    log.info("Http Service http://localhost:{} start success!", port);
//                    System.out.println(String.format("Http Service http://localhost:%s start success!", port));
                }
            });

            future.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
    }
}
