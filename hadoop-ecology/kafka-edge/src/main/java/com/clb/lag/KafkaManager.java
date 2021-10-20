package com.clb.lag;

import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;


/**
 * @author Xiahu
 * @create 2021/1/8
 */
public class KafkaManager {
    public static void main(String[] args) {
        ConsumerGroupCommand.ConsumerGroupCommandOptions options = new ConsumerGroupCommand.ConsumerGroupCommandOptions(args);
        ConsumerGroupCommand.ConsumerGroupService consumerGroupService = new ConsumerGroupCommand.ConsumerGroupService(options);
        consumerGroupService.describeGroup();


    }


}
