package cn.xhjava.netty.handler;

import cn.xhjava.netty.http.NettyHttpRequest;
import cn.xhjava.netty.http.NettyHttpResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021/1/5
 */
@Slf4j
public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    /**
     * 线程工厂
     */
    private ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setName("NettyHttpHandler-" + thread.getName());
        return thread;
    });

    /*@Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        log.info("通道注册");
    }*/

    //读取事件触发该方法
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {

        FullHttpRequest copyRequest = request.copy();
        /**
         * 通过内部类的形式传入一个runnable的实现类并重写了run方法 线程池在执行的时候会调用这个方法
         */
        executor.execute(() -> onReceivedRequest(ctx, new NettyHttpRequest(copyRequest)));
    }


    /**
     * @param ctx
     * @param request
     */
    private void onReceivedRequest(ChannelHandlerContext ctx, NettyHttpRequest request) {
        //处理request请求
//        FullHttpResponse response = handleHttpRequest(request);
//
//        //通过channel将结果输出 并通过添加监听器的方式关闭channel通道
//        context.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
//
//        //释放bytebuf缓存
//        ReferenceCountUtil.release(request);
        String msg = request.contentText();
        System.out.println(msg);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ctx.writeAndFlush(HttpServerHandler.buildResponse("xiahu"));

    }


    /**
     * @param request NettyHttpRequest extends Fullhttpreuqest
     * @return 请求处理结果
     */
    /*private FullHttpResponse handleHttpRequest(NettyHttpRequest request) {

        IFunctionHandler functionHandler = null;
        */

    /**
     * 请求处理并根据不同的结果或者捕获的异常进行状态码转换并返回
     *//*
        try {
            functionHandler = matchFunctionHandler(request);
            Response response = functionHandler.execute(request);
            return NettyHttpResponse.ok(response.toJSONString());
        } catch (IllegalMethodNotAllowedException error) {
            return NettyHttpResponse.make(HttpResponseStatus.METHOD_NOT_ALLOWED);
        } catch (IllegalPathNotFoundException error) {
            return NettyHttpResponse.make(HttpResponseStatus.NOT_FOUND);
        } catch (Exception error) {
            LOGGER.error(functionHandler.getClass().getSimpleName() + " Error", error);
            return NettyHttpResponse.makeError(error);
        }
    }*/
    private static DefaultFullHttpResponse buildResponse(String result) {
        //回复消息
        ByteBuf byteBuf = Unpooled.copiedBuffer(result, CharsetUtil.UTF_8);
        //构造与httpRequest 相对应的 HttpResponse
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, byteBuf.readableBytes());
        return response;
    }
}
