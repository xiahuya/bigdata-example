package domain;

import org.apache.commons.lang.StringUtils;

/**
 * @author Xiahu
 * @create 2020/7/7
 */
public class HoodieConfig {
    private String storageType;
    private String rowkey;
    private String precombineField;
    private String partitionField;
    private String insertParallelism;
    private String upsertParallelism;
    private String kerberosEnable;
    private String krb5Path;
    private String userPrial;
    private String keytab;
    private String hadoopConfDir;

    private String jdbcUrl;
    private String username;
    private String password;
    private String sshHost;
    private String sshPort;
    private String sshUser;
    private String sshPass;
    private String sshShellPath;


    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getPrecombineField() {
        return precombineField;
    }

    public void setPrecombineField(String precombineField) {
        this.precombineField = precombineField;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public void setPartitionField(String partitionField) {
        this.partitionField = partitionField;
    }

    public String getInsertParallelism() {
        return insertParallelism;
    }

    public void setInsertParallelism(String insertParallelism) {
        this.insertParallelism = insertParallelism;
    }

    public String getUpsertParallelism() {
        return upsertParallelism;
    }

    public void setUpsertParallelism(String upsertParallelism) {
        this.upsertParallelism = upsertParallelism;
    }

    public String getKerberosEnable() {
        return kerberosEnable;
    }

    public void setKerberosEnable(String kerberosEnable) {
        if (StringUtils.isEmpty(kerberosEnable)) {
            this.kerberosEnable = "false";
        } else {
            this.kerberosEnable = kerberosEnable;
        }

    }

    public String getKrb5Path() {
        return krb5Path;
    }

    public void setKrb5Path(String krb5Path) {
        if (StringUtils.isEmpty(krb5Path)) {
            this.krb5Path = "/etc/krb5.conf";
        } else {
            this.krb5Path = krb5Path;
        }
    }

    public String getUserPrial() {
        return userPrial;
    }

    public void setUserPrial(String userPrial) {
        if (StringUtils.isEmpty(userPrial)) {
            this.userPrial = "";
        } else {
            this.userPrial = userPrial;
        }
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public void setHadoopConfDir(String hadoopConfDir) {
        if (StringUtils.isEmpty(hadoopConfDir)) {
            this.hadoopConfDir = "/etc/hadoop/conf";
        } else {
            this.hadoopConfDir = hadoopConfDir;
        }
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        if (StringUtils.isEmpty(keytab)) {
            this.keytab = "";
        } else {
            this.keytab = keytab;
        }
    }


    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
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

    public String getSshHost() {
        return sshHost;
    }

    public void setSshHost(String sshHost) {
        this.sshHost = sshHost;
    }

    public String getSshPort() {
        return sshPort;
    }

    public void setSshPort(String sshPort) {
        this.sshPort = sshPort;
    }

    public String getSshUser() {
        return sshUser;
    }

    public void setSshUser(String sshUser) {
        this.sshUser = sshUser;
    }

    public String getSshPass() {
        return sshPass;
    }

    public void setSshPass(String sshPass) {
        this.sshPass = sshPass;
    }

    public String getSshShellPath() {
        return sshShellPath;
    }

    public void setSshShellPath(String sshShellPath) {
        this.sshShellPath = sshShellPath;
    }
}
