package com.clb.domain;

/**
 * @author Xiahu
 * @create 2020/8/11
 */
public class NuwaConsumerResponse {
    private String hudiTable;
    private String hudiPartitionColumnName;
    private String hudiTablePath;
    private String namespace;
    private String sourceDataPath;
    private String writeBatchTime;
    private String writeSuccessTime;


    public String getHudiTable() {
        return hudiTable;
    }

    public void setHudiTable(String hudiTable) {
        this.hudiTable = hudiTable;
    }

    public String getHudiPartitionColumnName() {
        return hudiPartitionColumnName;
    }

    public void setHudiPartitionColumnName(String hudiPartitionColumnName) {
        this.hudiPartitionColumnName = hudiPartitionColumnName;
    }

    public String getHudiTablePath() {
        return hudiTablePath;
    }

    public void setHudiTablePath(String hudiTablePath) {
        this.hudiTablePath = hudiTablePath;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getSourceDataPath() {
        return sourceDataPath;
    }

    public void setSourceDataPath(String sourceDataPath) {
        this.sourceDataPath = sourceDataPath;
    }

    public String getWriteBatchTime() {
        return writeBatchTime;
    }

    public void setWriteBatchTime(String writeBatchTime) {
        this.writeBatchTime = writeBatchTime;
    }

    public String getWriteSuccessTime() {
        return writeSuccessTime;
    }

    public void setWriteSuccessTime(String writeSuccessTime) {
        this.writeSuccessTime = writeSuccessTime;
    }
}
