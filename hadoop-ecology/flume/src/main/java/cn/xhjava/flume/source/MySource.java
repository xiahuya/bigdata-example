package cn.xhjava.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

/**
 * @author Xiahu
 * @create 2020/10/15
 *
 * 自定义source
 * 使用flume接收数据，并给每条数据添加前缀，输出到控制台。前缀可从flume配置文件中配置
 *
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
    //定义配置文件将来要读取的字段
    private Long delay;
    private String field;



    //主要处理方法
    @Override
    public Status process() throws EventDeliveryException {
        try {
            //创建事件头信息
            HashMap<String, String> hearderMap = new HashMap<>();
            //创建事件
            SimpleEvent event = new SimpleEvent();
            //模拟创建数据
            for (int i = 0; i < 5; i++) {
                //给事件设置头信息
                event.setHeaders(hearderMap);
                //给事件设置内容
                event.setBody((field + i).getBytes());
                //将事件写入channel
                getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;

    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


    //初始化配置信息
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field", "Hello!");
    }


    /**
     * 配置文件
     * # Name the components on this agent
     * a1.sources = r1
     * a1.sinks = k1
     * a1.channels = c1
     *
     * # Describe/configure the source
     * a1.sources.r1.type = com.atguigu.MySource
     * a1.sources.r1.delay = 1000
     * #a1.sources.r1.field = atguigu
     *
     * # Describe the sink
     * a1.sinks.k1.type = logger
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
