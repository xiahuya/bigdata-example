package com.clb.lag;

import lombok.Data;

/**
 * @author Xiahu
 * @create 2021/1/11
 */
@Data
public class PartitionOffsetState {
    private String group;
    private String coordinator;
    private String topic;
    private int partition;
    private Long offset;
    private Long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private Long logEndOffset;
}
