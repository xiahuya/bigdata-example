package com.clb.hoodie.demo

import com.clb.hoodie.util.{KerberosLogin, Util}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hive.HiveSyncTool
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.{Before, Test}


class HudiTest {

  @Before
  def init(): Unit = {
    val login = new KerberosLogin()
    login.kerberosLogin()
  }


  @Test
  def query(): Unit = {
    val basePath = "/tmp/huaxi/001/test"
    val spark = SparkSession.builder.appName("query insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*")

    tripsSnapshotDF.show(1000)
    tripsSnapshotDF.registerTempTable("hudi_test")
    //spark.sql("select * from hudi_test limit 100").show(100)
  }

  @Test
  def insert(): Unit = {
    val spark = SparkSession.builder.appName("hudi insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val insertData = spark.read.parquet("hdfs://master:8020/user/hive/warehouse/xh.db/hudi_mapping_test")
    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, classOf[OverwriteWithLatestAvroPayload].getName)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "hudi_rowkey_test")
      .mode(SaveMode.Overwrite)
      // 写入路径设置
      .save("/tmp/hudi/hudi_rowkey_test")
  }


  @Test
  def upsert(): Unit = {

    val spark = SparkSession.builder.appName("hudi upsert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val insertData = spark.read.parquet("/user/hive/warehouse/xh.db/hudi_test/202003021046190437.parquet")

    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "test")
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      // 写入路径设置
      .save("/tmp/hudi");
  }


  @Test
  def delete(): Unit = {
    val spark = SparkSession.builder.appName("delta insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val deleteData = spark.read.parquet("/user/hive/warehouse/xh.db/hudi_test/202003021046190437.parquet")
    deleteData.write.format("com.uber.hoodie")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "test")
      // 硬删除配置
      .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, "org.apache.hudi.EmptyHoodieRecordPayload")
  }


  @Test
  def insertPartition(): Unit = {
    val spark = SparkSession
      .builder
      .appName("hudi insert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[3]")
      .getOrCreate()
    // 读取文本文件转换为df
    val insertData = Util.readFromTxtByLineToDf(spark, "D:\\git\\study\\hudi-test\\src\\main\\resources\\test_insert_data.txt")
    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 设置分区列
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition")
      .mode(SaveMode.Append)
      .save("/tmp/hudi/xh")
  }


  @Test
  def queryPartition(): Unit = {
    val basePath = "/tmp/hudi_merge_on_read"
    val spark = SparkSession.builder.appName("hudi query").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*")

    tripsSnapshotDF.show()
  }

  @Test
  def upsertPartition(): Unit = {

    val spark = SparkSession.builder.appName("upsert partition").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "hdfs://master:8020/user/hive/warehouse/xh.db/presto_hudi_increnemt_test")

    upsertData.write.format("org.apache.hudi").option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "fk_id")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi/output/xh/presto_hudi_increment");
  }

  @Test
  def upsertPartitionChange(): Unit = {

    val spark = SparkSession.builder.appName("delta upsert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "D:\\git\\study\\hudi-test\\src\\main\\resources\\test_partition_update_data.txt")

    upsertData.write.format("org.apache.hudi").option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi");
  }


  @Test
  def hiveSync(): Unit = {
    val spark = SparkSession.builder.appName("delta hiveSync").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "D:\\git\\study\\hudi-test\\src\\main\\resources\\hive_sync.txt")

    upsertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "fk_id")
      // 设置要同步的hive库名
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "nuwa_hoodie_import_test")
      // 设置要同步的hive表名
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "nuwa_hudi_partition")
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置要同步的分区列名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "fk_id")
      // 设置jdbc 连接同步
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.0.111:10000")
      // hudi表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "nuwa_hudi_partition")
      // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi");
  }

  @Test
  def insertPartitionMergeOnRead(): Unit = {
    val spark = SparkSession.builder.appName("hudi insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    // 读取文本文件转换为df
    val insertData = Util.readFromTxtByLineToDf(spark, "D:\\git\\study\\hudi-test\\src\\main\\resources\\test_insert_data.txt")
    insertData.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 设置分区列
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition_merge_on_read")
      .mode(SaveMode.Overwrite)
      .save("/tmp/hudi_merge_on_read")
  }


  @Test
  def upsertPartitionMergeOnRead(): Unit = {

    val spark = SparkSession.builder.appName("upsert partition merge on read").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "D:\\git\\study\\hudi-test\\src\\main\\resources\\test_update_data.txt")

    upsertData.write.format("org.apache.hudi").option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 配置读时合并
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition_merge_on_read")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi_merge_on_read");
  }


  @Test
  def queryPartitionMergeOnRead(): Unit = {
    val basePath = "/tmp/hudi_merge_on_read"
    val spark = SparkSession.builder.appName("hudi query merge on read").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*")

    tripsSnapshotDF.show()
  }

  @Test
  def hiveSyncMergePartition(): Unit = {
    val args: Array[String] = Array(
      "--jdbc-url", "jdbc:hive2://192.168.0.111:10000",
      "--partition-value-extractor", "org.apache.hudi.hive.MultiPartKeysValueExtractor",
      "--user", "hive",
      "--pass", "hive",
      "--partitioned-by", "fk_id",
      "--base-path", "/nuwa/hudiImport/001/nuwa_hudi_partition",
      "--database", "nuwa_hoodie_import_test",
      "--table", "nuwa_hudi_partition_test111111111111")
    HiveSyncTool.main(args)
  }

  @Test
  def hiveSyncMergeNoPartition(): Unit = {
    val args: Array[String] = Array(
      "--jdbc-url", "jdbc:hive2://192.168.0.111:10000",
      "--partition-value-extractor", "org.apache.hudi.hive.NonPartitionedExtractor",
      "--user", "hive",
      "--pass", "hive",
      //      "--partitioned-by", "fk_id",
      "--base-path", "/nuwa/hudiImport/001/nuwa_hudi_no_partition",
      "--database", "nuwa_hoodie_import_test",
      "--table", "nuwa_hudi_no_partition")
    HiveSyncTool.main(args)
  }

  @Test
  def hiveSyncMergeOnRead(): Unit = {
    val spark = SparkSession.builder.appName("delta hiveSync").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val upsertData = Util.readFromTxtByLineToDf(spark, "D:\\git\\study\\hudi-test\\src\\main\\resources\\hive_sync.txt")

    upsertData.write.format("org.apache.hudi")
      // 配置读时合并
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "dt")
      // 设置要同步的hive库名
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "xh")
      // 设置要同步的hive表名
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "test_partition_merge_on_read")
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置要同步的分区列名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt")
      // 设置jdbc 连接同步
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.0.111:10000")
      // hudi表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "test_partition_merge_on_read")
      // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi_merge_on_read");
  }


  @Test
  def insertPartition2(): Unit = {
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder.appName("hudi insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    // 读取文本文件转换为df
    val insertData = spark.read.parquet("/user/hive/warehouse/xh.db/test_hudi_partition_change2/202003031140090629.parquet")
    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 设置分区列
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "oid")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TABLE_NAME, "test_hudi_partition_change2")
      .mode(SaveMode.Overwrite)
      .save("/tmp/hudi/test_partition_change2")
    val end = System.currentTimeMillis()
    println("耗费：" + (end - start) / 1000.0 + "S")
  }

  @Test
  def query2(): Unit = {
    val basePath = "/tmp/hudi/partition_test"
    val spark = SparkSession.builder.appName("query insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*/*")

    // tripsSnapshotDF.show()
    tripsSnapshotDF.registerTempTable("hudi_test")
    spark.sql("select count(*) from hudi_test").show(100000000)
  }

  @Test
  def upsertPartition2(): Unit = {
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder.appName("upsert partition").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[*]").getOrCreate()
    //.master("local[*]")
    val upsertData = spark.read.parquet("/user/hive/warehouse/xh.db/test_hudi_partition_change/202003030940450558.parquet")
    upsertData.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "oid")
      .option(HoodieWriteConfig.TABLE_NAME, "test_hudi_partition_change")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/tmp/hudi/test_partition_change")
    val end = System.currentTimeMillis()
    println("耗费：" + (end - start) / 1000.0 + "S")
  }

  @Test
  def test(): Unit = {
    println(HoodieIndex.IndexType.GLOBAL_BLOOM.name())
  }

}
