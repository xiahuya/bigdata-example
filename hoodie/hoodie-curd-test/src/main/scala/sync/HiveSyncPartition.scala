package sync

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Xiahu
  * @create 2020/5/28
  */
object HiveSyncPartition {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("delta hiveSync")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[3]")
      .getOrCreate()
    //    val upsertData = Util.readFromTxtByLineToDf(spark, "")
    val upsertData = spark.read.parquet("/datacenter/schema_test/*")

    upsertData.write.format("org.apache.hudi")
      // 设置主键列名
//      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
//      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
//      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "p_create_time")
//      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
//      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
//      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
//
//      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
//      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "hid0101_cache_xdcs_pacs_hj")
//      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "merge_test13")
//      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "deta_pre")
//      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.0.112:10000")
//      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
//
//
//      // 并行度参数设置
//      .option("hoodie.insert.shuffle.parallelism", "2")
//      .option("hoodie.upsert.shuffle.parallelism", "2")
//      .option(HoodieWriteConfig.TABLE_NAME, "nuwa_hudi_partition")
//      .mode(SaveMode.Append)
//      .save("/tmp/hudi");

    upsertData.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "p_create_time")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())


      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "hid0101_cache_xdcs_pacs_hj")
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "merge_test13")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "deta_pre")
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.0.112:10000")
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")


      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .option(HoodieWriteConfig.TABLE_NAME, "nuwa_hudi_partition")
      .save("/tmp/test2");
  }
}
