package cn.xhjava;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/7/21
 */
public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("hdfs://master:8020/flink/checkpoints"));


//        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
//        StateBackend stateBackend = new FsStateBackend("hdfs://master:8020/tmp/flink-checkpoint/");
//        env.setStateBackend(stateBackend);


        Properties prop = new Properties();
        prop.put("bootstrap.servers", "node2:9092");
        prop.put("zookeeper.connect", "2181/cdh_kafka");
        prop.put("group.id", "test-consumer");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");


        //添加数据源
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer011<>(
                "xiahu2",
                new SimpleStringSchema(),
                prop));

        dataStream.map(new MapFunction<String, Object>() {

            @Override
            public Object map(String s) throws Exception {
                log.info(s);
                return null;
            }
        });


        dataStream.print();


        env.execute("Flink add data source");
    }
}
