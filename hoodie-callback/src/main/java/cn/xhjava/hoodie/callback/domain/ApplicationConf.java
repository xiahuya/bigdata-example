package cn.xhjava.hoodie.callback.domain;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * @author Xiahu
 * @create 2021-06-18
 */
@Configuration
@Component
public class ApplicationConf {
    @Value("${hoodie.datasource.hive_sync.database}")
    private String hiveSyncDatabase;

    @Value("${hoodie.datasource.hive_sync.jdbcurl}")
    private String hiveSyncJdbcUrl;

    @Value("${hoodie.datasource.hive_sync.username}")
    private String hiveSyncUsername;


    @Value("${hoodie.datasource.hive_sync.password}")
    private String hiveSyncPassword;

    @Value("${hoodie.datasource.hive_sync.partition_fields}")
    private String getHivePartitionField;


    @Value("${ssh.shell.host}")
    private String sshShellHost;

    @Value("${ssh.shell.port}")
    private int sshShellPort;
    @Value("${ssh.shell.user}")
    private String sshShellUser;

    @Value("${ssh.shell.pass}")
    private String sshShellPass;

    public String getHiveSyncDatabase() {
        return hiveSyncDatabase;
    }

    public void setHiveSyncDatabase(String hiveSyncDatabase) {
        this.hiveSyncDatabase = hiveSyncDatabase;
    }

    public String getHiveSyncJdbcUrl() {
        return hiveSyncJdbcUrl;
    }

    public void setHiveSyncJdbcUrl(String hiveSyncJdbcUrl) {
        this.hiveSyncJdbcUrl = hiveSyncJdbcUrl;
    }

    public String getHiveSyncUsername() {
        return hiveSyncUsername;
    }

    public void setHiveSyncUsername(String hiveSyncUsername) {
        this.hiveSyncUsername = hiveSyncUsername;
    }

    public String getHiveSyncPassword() {
        return hiveSyncPassword;
    }

    public void setHiveSyncPassword(String hiveSyncPassword) {
        this.hiveSyncPassword = hiveSyncPassword;
    }

    public String getGetHivePartitionField() {
        return getHivePartitionField;
    }

    public void setGetHivePartitionField(String getHivePartitionField) {
        this.getHivePartitionField = getHivePartitionField;
    }

    public String getSshShellHost() {
        return sshShellHost;
    }

    public void setSshShellHost(String sshShellHost) {
        this.sshShellHost = sshShellHost;
    }

    public int getSshShellPort() {
        return sshShellPort;
    }

    public void setSshShellPort(int sshShellPort) {
        this.sshShellPort = sshShellPort;
    }

    public String getSshShellUser() {
        return sshShellUser;
    }

    public void setSshShellUser(String sshShellUser) {
        this.sshShellUser = sshShellUser;
    }

    public String getSshShellPass() {
        return sshShellPass;
    }

    public void setSshShellPass(String sshShellPass) {
        this.sshShellPass = sshShellPass;
    }
}
