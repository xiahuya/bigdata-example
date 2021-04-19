package com.clin.spark.loop.config;

/**
 * @Author: hj
 * @Date: 2020/7/16 上午9:49
 * @Desc:
 */
public class HoodieImportConstant {
    public static final String HOODIE_INSERT_SHUFFLE_PARALLELISM = "hoodie.insert.shuffle.parallelism";
    public static final String HOODIE_UNSERT_SHUFFLE_PARALLELISM = "hoodie.upsert.shuffle.parallelism";
    public static final String HOODIE_DATASOURCE_WRITE_STORAGE_TYPE = "hoodie.datasource.write.storage.type";
    public static final String HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD = "hoodie.datasource.write.recordkey.field";
    public static final String HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field";
    public static final String HOODIE_DATASOURCE_WRITE_PARTITIONPATH_FIELD = "hoodie.datasource.write.partitionpath.field";
    public static final String HOODIE_IMPORT_KERBER_ENABLE = "hoodie.import.kerberos.enable";
    public static final String HOODIE_IMPORT_KERBER_KRB5_PATH = "hoodie.import.kerberos.krb5.path";
    public static final String HOODIE_IMPORT_KERBER_KEYTAB = "hoodie.import.kerberos.keytab";
    public static final String HOODIE_IMPORT_KERBER_USER_PRINCIPAL = "hoodie.import.kerberos.user.principal";
    public static final String HADOOP_CONF_DIR = "hadoop.conf.dir";


    public static final String HOODIE_DATASOURCE_HIVE_SYNC_JDBCURL = "hoodie.datasource.hive_sync.jdbcurl";
    public static final String HOODIE_DATASOURCE_HIVE_SYNC_USERNAME = "hoodie.datasource.hive_sync.username";
    public static final String HOODIE_DATASOURCE_HIVE_SYNC_PASSWORD = "hoodie.datasource.hive_sync.password";

    public static final String HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_HOST = "hoodie.import.ssh.hudi.hive.sync.host";
    public static final String HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_PORT = "hoodie.import.ssh.hudi.hive.sync.port";
    public static final String HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_USER = "hoodie.import.ssh.hudi.hive.sync.user";
    public static final String HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_PASS = "hoodie.import.ssh.hudi.hive.sync.pass";
    public static final String HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_SHELL_PATH = "hoodie.import.ssh.hudi.hive.sync.shell.path";

}
