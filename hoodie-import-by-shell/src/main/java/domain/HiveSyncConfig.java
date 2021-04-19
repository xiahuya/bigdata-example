package domain;

/**
 * @author Xiahu
 * @create 2020/6/1
 */
public class HiveSyncConfig {
    private String sourcePath; //hudi表源路径
    private String database;
    private String table;
    private String jdbcurl;
    private String username;
    private String password;
    private String partition_by; //分区列名
    private String partition_value_extractor;


    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getJdbcurl() {
        return jdbcurl;
    }

    public void setJdbcurl(String jdbcurl) {
        this.jdbcurl = jdbcurl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPartition_by() {
        return partition_by;
    }

    public void setPartition_by(String isPartition) {
        if (isPartition.equals("true")) {
            this.partition_by = "true";
        } else {
            this.partition_by = "null";
        }
    }

    public String getPartition_value_extractor() {
        return partition_value_extractor;
    }

    public void setPartition_value_extractor(String isPartitionTable) {
        if (isPartitionTable.equals("true")) {
            this.partition_value_extractor = "org.apache.hudi.hive.MultiPartKeysValueExtractor";
        } else {
            this.partition_value_extractor = "org.apache.hudi.hive.NonPartitionedExtractor";

        }
    }
}
