package sync

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Xiahu
  * @create 2020/5/28
  */
object HiveSyncNoPartition {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("delta hiveSync")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[3]")
      .getOrCreate()
    //    val upsertData = Util.readFromTxtByLineToDf(spark, "")
    val upsertData = spark.read.parquet("/user/hive/warehouse/nuwa_hoodie_import_test.db/nuwa_hudi_import_test_table_11/*")


    upsertData.write.format("org.apache.hudi")
      // 配置读时合并
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
      // 分区列设置
      // 设置要同步的hive库名sh
      //      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "nuwa_hoodie_import_test")
      // 设置要同步的hive表名
      //      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "nuwa_hudi_no_partition")
      // 设置数据集注册并同步到hive
      //      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置jdbc 连接同步
      //      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://master:10000/nuwa_hoodie_import_test")
      //      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[NonPartitionedExtractor].getName)
      // hudi表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, "nuwa_hudi_no_partition")
      // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
      //      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save("/nuwa/hudiImport/001/nuwa_hudi_no_partition/");
  }
}
