package com.clb.lag;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author Xiahu
 * @create 2021/1/11
 */
public class KafkaOffsetTool {
    private AdminClient adminClient;
    private static final String MISSING_COLUMN_VALUE = "-";
    private KafkaConsumer consumer;

    public KafkaOffsetTool() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "node2:9092");
        //kerberos认证需要自己实现
        if (false) {
            properties.put("sasl.kerberos.service.name", "kafka");
            properties.put("sasl.mechanism", "GSSAPI");
            properties.put("security.protocol", "PLAINTEXT");
        }
        this.adminClient = AdminClient.create(properties);
    }


    public List<PartitionOffsetState> collectGroupOffsets(String group) throws Exception {
        List<PartitionOffsetState> result = new ArrayList<>();
        List<String> groupId = Arrays.asList(group);
        Map<String, KafkaFuture<ConsumerGroupDescription>> describedGroups = adminClient.describeConsumerGroups(groupId).describedGroups();
        ConsumerGroupDescription consumerGroup = describedGroups.get(group).get();
        ConsumerGroupState state = consumerGroup.state();
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = getCommitsOffsets(group);

        Collection<MemberDescription> memberDescriptions = consumerGroup.members();
        Set<MemberDescription> memberDescriptionSet = new HashSet<>();
        Iterator<MemberDescription> iterator = memberDescriptions.iterator();
        while (iterator.hasNext()) {
            MemberDescription memberDescription = iterator.next();
            if (null != memberDescription.assignment().topicPartitions()) {
                memberDescriptionSet.add(memberDescription);
            }
        }
        memberDescriptionSet.stream().sorted(new Comparator<MemberDescription>() {
            @Override
            public int compare(MemberDescription o1, MemberDescription o2) {
                if (o1.assignment().topicPartitions().size() >= o2.assignment().topicPartitions().size()) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }).forEach(new Consumer<MemberDescription>() {
            @Override
            public void accept(MemberDescription memberDescription) {
                Set<TopicPartition> topicPartitions = memberDescription.assignment().topicPartitions();
                for (TopicPartition tp : topicPartitions) {
                    long offset = committedOffsets.get(tp).offset();
                    PartitionOffsetState partitionOffsetState = new PartitionOffsetState();
                    partitionOffsetState.setGroup(group);
                    partitionOffsetState.setCoordinator(consumerGroup.coordinator().toString());
                    partitionOffsetState.setHost(memberDescription.host());
                    partitionOffsetState.setClientId(memberDescription.clientId());
                    partitionOffsetState.setConsumerId(memberDescription.consumerId());
                    partitionOffsetState.setPartition(tp.partition());
                    partitionOffsetState.setTopic(tp.topic());
                    partitionOffsetState.setOffset(offset);
                    result.add(partitionOffsetState);
                }
            }
        });

        //封装committedOffsets
        Iterator<Map.Entry<TopicPartition, OffsetAndMetadata>> entryIterator = committedOffsets.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<TopicPartition, OffsetAndMetadata> entry = entryIterator.next();
            PartitionOffsetState partitionOffsetState = new PartitionOffsetState();
            partitionOffsetState.setGroup(group);
            partitionOffsetState.setCoordinator(consumerGroup.coordinator().toString());
            partitionOffsetState.setHost(MISSING_COLUMN_VALUE);
            partitionOffsetState.setClientId(MISSING_COLUMN_VALUE);
            partitionOffsetState.setConsumerId(MISSING_COLUMN_VALUE);
            partitionOffsetState.setPartition(entry.getKey().partition());
            partitionOffsetState.setTopic(entry.getKey().topic());
            partitionOffsetState.setOffset(entry.getValue().offset());
            result.add(partitionOffsetState);
        }
        return result;
    }

    private Map<TopicPartition, OffsetAndMetadata> getCommitsOffsets(String groupId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> result = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        return result;
    }


    public List<PartitionOffsetState> getLag(List<PartitionOffsetState> partitionOffsetStateList,String groupId) {
        getConsumer(new Properties(), groupId);
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        for (PartitionOffsetState partitionOffset : partitionOffsetStateList) {
            topicPartitionList.add(new TopicPartition(partitionOffset.getTopic(), partitionOffset.getPartition()));
        }
        Map<TopicPartition, Long> map = consumer.endOffsets(topicPartitionList);
        for (PartitionOffsetState partitionOffset : partitionOffsetStateList) {
            for (Map.Entry<TopicPartition, Long> entry : map.entrySet()) {
                if (entry.getKey().topic().equals(partitionOffset.getTopic()) && entry.getKey().partition() == partitionOffset.getPartition()) {
                    partitionOffset.setLag(entry.getValue() - partitionOffset.getOffset());
                    partitionOffset.setLogEndOffset(entry.getValue());
                }
            }
        }
        return partitionOffsetStateList;

    }


    private KafkaConsumer getConsumer(Properties prop, String groupId) {
        if (consumer == null) {
            createConsumer(prop, groupId);
        }
        return consumer;
    }

    public void createConsumer(Properties prop, String groupId) {
        //kerberos认证需要自己实现
        if (false) {
            System.setProperty("java.security.krb5.conf", prop.getProperty(NuwaConstant.KERBEROS_KRB5));
            System.setProperty("java.security.auth.login.config", prop.getProperty(NuwaConstant.KERBEROS_LOGIN_CONFIG));
            prop.put(NuwaConstant.KAFKA_SECURITY_PROTOCOL, prop.getProperty(NuwaConstant.KAFKA_SECURITY_PROTOCOL));
            prop.put(NuwaConstant.KAFKA_SASL_MECHANISM, prop.getProperty(NuwaConstant.KAFKA_SASL_MECHANISM));
            prop.put(NuwaConstant.KAFKA_SASL_KERBEROS_SERVICE_NAME, prop.getProperty(NuwaConstant.KAFKA_SASL_KERBEROS_SERVICE_NAME));
        }

        String deserializer = StringDeserializer.class.getName();
        String broker = "node1:9092";
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        consumer = new KafkaConsumer(prop);
    }

    public static void main(String[] args) throws Exception {
        KafkaOffsetTool kafkaOffsetTool = new KafkaOffsetTool();
        List<PartitionOffsetState> partitionOffsetStates = kafkaOffsetTool.collectGroupOffsets("kafka-lag");
        partitionOffsetStates = kafkaOffsetTool.getLag(partitionOffsetStates,"kafka-lag");
        System.out.println(partitionOffsetStates);
    }
}
