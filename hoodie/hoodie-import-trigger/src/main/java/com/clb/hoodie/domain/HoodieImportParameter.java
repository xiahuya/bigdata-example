package com.clb.hoodie.domain;

/**
 * @author Xiahu
 * @create 2020/7/9
 */
public class HoodieImportParameter {
    private String databse;
    private String table;
    private String inPutPath;
    private String outPutPath;
    private String isPartitionTable;
    private String partitionColumn;

    public String getDatabse() {
        return databse;
    }

    public void setDatabse(String databse) {
        this.databse = databse;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getInPutPath() {
        return inPutPath;
    }

    public void setInPutPath(String inPutPath) {
        this.inPutPath = inPutPath;
    }

    public String getOutPutPath() {
        return outPutPath;
    }

    public void setOutPutPath(String outPutPath) {
        this.outPutPath = outPutPath;
    }

    public String getIsPartitionTable() {
        return isPartitionTable;
    }

    public void setIsPartitionTable(String isPartitionTable) {
        this.isPartitionTable = isPartitionTable;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }
}
