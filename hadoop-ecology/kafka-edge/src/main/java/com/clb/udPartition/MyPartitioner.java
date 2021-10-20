package com.clb.udPartition;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * 自定义分区器(用于消息发布在同一个topic不同分区)
 */
public class MyPartitioner implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(MyPartitioner.class);
    private static int partitionCount = 0;

    @Override
    public int partition(String topic, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
        partitionCount = cluster.partitionCountForTopic(topic);
        String message = (String) value;
        String[] split = message.split(",", 5);
        String dbTable = split[0].split(":", 2)[1].replace("\"", "");
        String op_time = split[4].split(":", 2)[1].replace("\"", "");
        message = dbTable + "-" + op_time;
        int partition = getHash(dbTable);
        LOG.info(String.format("key: %s partition: %s", key.toString(), partition));
        if (partition >= partitionCount) {
            partition = 0;
        }
        return partition;

    }


    public void close() {

    }

    public void configure(Map<String, ?> map) {
    }


    //取哈希,实现均匀分布
    private static int getHash(String name) {
        int i = name.hashCode();
        return i & (partitionCount - 1) % partitionCount;
    }


}
