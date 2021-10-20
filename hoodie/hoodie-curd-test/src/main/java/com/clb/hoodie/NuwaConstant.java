package com.clb.hoodie;

/**
 * @author Xiahu
 * @create 2020/7/20
 */
public class NuwaConstant {
    public static final String NUWA_CONSUMER_KAFKA_TOPIC = "kafka.topics";
    public static final String NUWA_CONSUMER_KAFKA_BROKER = "kafka.brokers";
    public static final String NUWA_CONSUMER_KAFKA_GRROUP_ID = "kafka.group.id";
    public static final String NUWA_CONSUMER_ETL_URL = "mysql.partition.conf.url";
    public static final String NUWA_CONSUMER_ETL_USER = "mysql.partition.conf.user";
    public static final String NUWA_CONSUMER_ETL_PASS = "mysql.partition.conf.password";
    public static final String NUWA_CONSUMER_ETL_DATABASE = "mysql.partition.conf.database";
    public static final String NUWA_CONSUMER_ETL_TABLE = "mysql.partition.conf.table";
    public static final String NUWA_CONSUMER_OFFSET_URL = "mysql.kafka.offset.conf.url";
    public static final String NUWA_CONSUMER_OFFSET_USER = "mysql.kafka.offset.conf.user";
    public static final String NUWA_CONSUMER_OFFSET_PASS = "mysql.kafka.offset.conf.password";
    public static final String NUWA_CONSUMER_OFFSET_DATABASE = "mysql.kafka.offset.conf.database";
    public static final String NUWA_CONSUMER_OFFSET_TABLE = "mysql.kafka.offset.conf.table";
    public static final String NUWA_CONSUMER_FLINK_RUN_PARALLELISM = "flink.runing.parallelism";
    public static final String NUWA_CONSUMER_FLINK_WINDOW_TIME = "flink.window.interval.time";
    public static final String NUWA_CONSUMER_FLINK_SINK_HDFS_PARTITION_FORMAT = "flink.partition.format";
    public static final String NUWA_CONSUMER_FLINK_SINK_PARALLELISM = "flink.sink.parallelism";
    public static final String NUWA_CONSUMER_FLINK_CHECKPOINT_TYPE = "flink.checkpoint.statebackend.type";
    public static final String NUWA_CONSUMER_FLINK_CHECKPOINT_TIME = "flink.checkpoint.interval.time";
    public static final String NUWA_CONSUMER_FLINK_CHECKPOINT_DIR = "flink.checkpoint.path";
    public static final String NUWA_CONSUMER_FLINK_SINK_HDFS_PATH = "hdfs.path";
    public static final String NUWA_CONSUMER_FLINK_SINK_HDFS_LATE_PATH = "hdfs.latepath";

    public static final String KERBEROS_ENABLE = "nuwa.kerberos.enable";
    public static final String KERBEROS_KRB5 = "nuwa.kerberos.krb5.path";
    public static final String KERBEROS_KEYTAB = "nuwa.kerberos.keytab";
    public static final String KERBEROS_LOGIN_CONFIG = "nuwa.kerberos.login.config";
    public static final String KERBEROS_PRINCIPAL = "nuwa.kerberos.user.principal";
    public static final String HADOOP_CONF_DIR = "nuwa.hadoop.conf.dir";
    public static final String KAFKA_SECURITY_PROTOCOL = "security.protocol";
    public static final String KAFKA_SASL_MECHANISM = "sasl.mechanism";
    public static final String KAFKA_SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    public static final String ZOOKEEPER_JAVA_AUTH_LOGIN_CONFIG = "zookeeper.java.security.auth.login.config";
    public static final String KERBEROS_SPARK_SUBMIT_KEYTAB = "nuwa.import.spark.submit.keytab";
    public static final String KERBEROS_SPARK_SUBMIT_PRINCIPAL = "nuwa.import.spark.submit.principal";

    public static final String NUWA_PARQUET_WRITER_SIZE = "parquet.writer.size";

    public static final String NUWA_IMPORT_INSERT_SHUFFLE_PARALLELISM = "nuwa.import.hoodie.insert.shuffle.parallelism";
    public static final String NUWA_IMPORT_UNSERT_SHUFFLE_PARALLELISM = "nuwa.import.hoodie.upsert.shuffle.parallelism";
    public static final String NUWA_IMPORT_DATASOURCE_WRITE_STORAGE_TYPE = "nuwa.import.hoodie.datasource.write.storage.type";
    public static final String NUWA_IMPORT_DATASOURCE_WRITE_RECORDKEY_FIELD = "nuwa.import.hoodie.datasource.write.recordkey.field";
    public static final String NUWA_IMPORT_DATASOURCE_WRITE_PRECOMBINE_FIELD = "nuwa.import.hoodie.datasource.write.precombine.field";
    public static final String NUWA_IMPORT_HOODIE_BLOOM_TYPE = "nuwa.import.hoodie.bloom.type";

    public static final String NUWA_IMPORT_DATASOURCE_HIVE_SYNC_JDBCURL = "nuwa.import.hoodie.datasource.hive.sync.jdbcurl";
    public static final String NUWA_IMPORT_DATASOURCE_HIVE_SYNC_USERNAME = "nuwa.import.hoodie.datasource.hive.sync.username";
    public static final String NUWA_IMPORT_DATASOURCE_HIVE_SYNC_PASSWORD = "nuwa.import.hoodie.datasource.hive.sync.password";

    public static final String NUWA_IMPORT_SSH_HUDI_HIVE_SYNC_HOST = "nuwa.import.hoodie.import.ssh.hudi.hive.sync.host";
    public static final String NUWA_IMPORT_SSH_HUDI_HIVE_SYNC_PORT = "nuwa.import.hoodie.import.ssh.hudi.hive.sync.port";
    public static final String NUWA_IMPORT_SSH_HUDI_HIVE_SYNC_USER = "nuwa.import.hoodie.import.ssh.hudi.hive.sync.user";
    public static final String NUWA_IMPORT_SSH_HUDI_HIVE_SYNC_PASS = "nuwa.import.hoodie.import.ssh.hudi.hive.sync.pass";
    public static final String NUWA_IMPORT_SSH_HUDI_HIVE_SYNC_SHELL_PATH = "nuwa.import.hoodie.import.ssh.hudi.hive.sync.shell.path";


    public static final String NUWA_IMPORT_SPARK_SOCKET_PORT = "nuwa.import.spark.socket.port";
    public static final String NUWA_IMPORT_SPARK_SOCKET_IP = "nuwa.import.spark.socket.ip";


    public static final String SPARK_EXECUTOR_MEMORY = "spark.app.name";
    public static final String SPARK_DRIVER_HOST = "spark.driver.host";
    public static final String SPARK_DRIVER_PORT = "spark.driver.port";
    public static final String SPARK_DRIVER_MEMORY = "spark.driver.memory";
    public static final String SPARK_DRIVER_APPUIADDRESS = "spark.driver.appUIAddress";
    public static final String SPARK_DRIVER_MAXRESULTSIZE = "spark.driver.maxResultSize";

    public static final String SPARK_EXECUTOR_ID = "spark.executor.id";
    public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
    public static final String SPARK_EXECUTOR_INSTANCES = "spark.executor.instances";


    public static final String SPARK_HOME = "spark.home";
    public static final String SPARK_KRYOSERIALIZER_BUFFER_MAX = "spark.kryoserializer.buffer.max";
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_RDD_COMPRESS = "spark.rdd.compress";
    public static final String SPARK_SCHEDULER_MODE = "spark.scheduler.mode";
    public static final String SPARK_SERIALIZER = "spark.serializer";
    public static final String SPARK_SHUFFER_MEMORYFRACTION = "spark.shuffle.memoryFraction";
    public static final String SPARK_SHUFFER_SERVICE_ENABLE = "spark.shuffle.service.enabled";
    public static final String SPARK_SQL_HIVE_CONVERMETATOREPARQUET = "spark.sql.hive.convertMetastoreParquet";
    public static final String SPARK_STORAGE_MEMORYFRACTION = "spark.storage.memoryFraction";
    public static final String SPARK_STREAMING_BACKPRESSURE_ENBALE = "spark.streaming.backpressure.enabled";
    public static final String SPARK_STREAMING_KAFKA_MAXRATEPERPARTITION = "spark.streaming.kafka.maxRatePerPartition";
    public static final String SPARK_SUBMIT_DEPLOYMODE = "spark.submit.deployMode";
    public static final String SPARK_TASK_CPU = "spark.task.cpus";
    public static final String SPARK_TASK_MAXFAILURES = "spark.task.maxFailures";
    public static final String SPARK_YARN_DRIVER_MEMORYOVERHEAD = "spark.yarn.driver.memoryOverhead";
    public static final String SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = "spark.yarn.executor.memoryOverhead";
    public static final String SPARK_YARN_MAX_EXECUTOR_FAILURES = "spark.yarn.max.executor.failures";

    public static final String NUWA_CONSUMER_BATCH_TIME_INTERVAL = "nuwa.consumer.batch.time.interval";


    public static final String ZOOKEEPER_CONNECT_ADDRESS = "zookeeper.connetc.address";
    public static final String ZOOKEEPER_CONNECT_PORT = "zookeeper.connect.timeout";
    public static final String ZOOKEEPER_NODE_ROOT_PATH = "zookeeper.node.root.path";
    public static final String KAFKA_JAVA_AUTH_LOGIN_CONFIG = "kafka.java.security.auth.login.config";

    private static final String SUCCESSFUL_CODE = "000000";
    private static final String FAILED_CODE = "000001";
    public static final String SUCCESSFUL = "成功";
    public static final String FAILED = "失败";

    public static final String NUWA_UTIL_DATA_SCHEMA_URL = "mysql.data.schema.conf.url";
    public static final String NUWA_UTIL_DATA_SCHEMA_USER = "mysql.data.schema.conf.user";
    public static final String NUWA_UTIL_DATA_SCHEMA_PASS = "mysql.data.schema.conf.password";
    public static final String NUWA_UTIL_DATA_SCHEMA_DATABASE = "mysql.data.schema.conf.database";
    public static final String NUWA_UTIL_DATA_SCHEMA_TABLE = "mysql.data.schema.conf.table";

    public static final String NUWA_IMPORT_HOODIE_INDEX_HBASE_ZKQUORUM = "nuwa.import.hoodie.index.hbase.zkquorum";
    public static final String NUWA_IMPORT_HOODIE_INDEX_HBASE_ZKPORT = "nuwa.import.hoodie.index.index.hbase.zkport";
    public static final String NUWA_IMPORT_HOODIE_INDEX_HBASE_ZKNODE_PATH = "nuwa.import.hoodie.index.hbase.zknode.path";
    public static final String NUWA_IMPORT_HOODIE_INDEX_UPDATE_PARTITION_PATH = "nuwa.import.hoodie.index.update.partition.path";
    public static final String NUWA_IMPORT_HOODIE_INDEX_HBASE_GET_BATCH_SIZE = "nuwa.import.hoodie.index.hbase.get.batch.size";
    public static final String NUWA_IMPORT_HOODIE_INDEX_HBASE_QPS_FRACTION = "nuwa.import.hoodie.index.hbase.qps.fraction";
    public static final String NUWA_IMPORT_HOODIE_INDEX_HBASE_MAX_QPS_PER_REGION_SERVER = "nuwa.import.hoodie.index.hbase.max.qps.per.region.server";


    public static final String NUWA_IMPORT_HOODIE_UPSERT_BATCH_SIZE = "nuwa.import.hoodie.upsert.batch.size";


    public static final String NUWA_IMPORT_PARTITION_TYPE = "hoodie.datasource.write.partition.type";
    public static final String NUWA_IMPORT_DATE_FORMAT = "hoodie.datasource.write.partition.date.format";
    public static final String NUWA_IMPORT_PARTITION_STYLE = "hoodie.datasource.write.partition.style";
    public static final String HASH = "HASH";
    public static final String DATE = "DATE";
    public static final String DATE_FORMAT_Day = "yyyy-MM-dd";
    public static final String DATE_FORMAT_MONTH = "yyyy-MM";
    public static final String DATE_FORMAT_YEAR = "yyyy";
    public static final String MULTIPLE = "MULTIPLE";
    public static final String CONSTANT = "C";

}
