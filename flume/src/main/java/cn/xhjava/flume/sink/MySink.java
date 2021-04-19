package cn.xhjava.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Xiahu
 * @create 2020/10/15
 * <p>
 * Sink不断地轮询Channel中的事件且批量地移除它们，并将这些事件批量写入到存储或索引系统、或者被发送到另一个Flume Agent。
 * Sink是完全事务性的。在从Channel批量删除数据之前，每个Sink用Channel启动一个事务。
 * 批量事件一旦成功写出到存储系统或下一个Flume Agent，Sink就利用Channel提交事务。
 * 事务一旦被提交，该Channel从自己的内部缓冲区删除事件。
 * Sink组件目的地包括hdfs、logger、avro、thrift、ipc、file、null、HBase、solr、自定义
 */
//需求:使用flume接收数据，并在Sink端给每条数据添加前缀和后缀，输出到控制台。前后缀可在flume任务配置文件中配置
public class MySink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(MySink.class);


    //前后缀
    private String prefix;
    private String suffix;


    @Override
    public Status process() throws EventDeliveryException {
        Status status;

        //获取当前Sink绑定的Channel
        Channel ch = getChannel();

        //事务
        Transaction txn = ch.getTransaction();

        //声明事件
        Event event;

        //开启事务
        txn.begin();

        //读取Channel中的事件，直到读取到事件结束循环
        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }
        try {
            //执行相应逻辑,这里只是打印日志信息
            LOG.info(prefix + new String(event.getBody()) + suffix);


            //事务提交
            txn.commit();
            status = Status.READY;
        } catch (Exception e) {

            //遇到异常，事务回滚
            txn.rollback();
            status = Status.BACKOFF;
        } finally {

            //关闭事务
            txn.close();
        }
        return status;

    }

    @Override
    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("prefix", "hello:");

        //读取配置文件内容，无默认值
        suffix = context.getString("suffix");
    }


    /**
     * # Name the components on this agent
     * a1.sources = r1
     * a1.sinks = k1
     * a1.channels = c1
     *
     * # Describe/configure the source
     * a1.sources.r1.type = netcat
     * a1.sources.r1.bind = localhost
     * a1.sources.r1.port = 44444
     *
     * # Describe the sink
     * a1.sinks.k1.type = com.atguigu.MySink
     * a1.sinks.k1.prefix = xhjava:
     * a1.sinks.k1.suffix = :xhjava
     *
     * # Use a channel which buffers events in memory
     * a1.channels.c1.type = memory
     * a1.channels.c1.capacity = 1000
     * a1.channels.c1.transactionCapacity = 100
     *
     * # Bind the source and sink to the channel
     * a1.sources.r1.channels = c1
     * a1.sinks.k1.channel = c1
     */


}
