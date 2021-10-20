package com.clb.hoodie

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.{SaveMode, SparkSession}

object PartitionUpsert {
  def main(args: Array[String]): Unit = {
    val readpath = "/user/hive/warehouse/hudi_hbase.db/hudi_hbase_test_target_2/*"
    val dbTable = "xh.test"
    val db = dbTable.split("\\.")(0)
    val table = dbTable.split("\\.")(1)
    val spark = SparkSession
      .builder
      .appName("upsert partition")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    val upsertData = spark.read.parquet(readpath)
    upsertData.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 分区列设置
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "fk_id")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "8")
      .option("hoodie.upsert.shuffle.parallelism", "8")
      .option(HoodieWriteConfig.TABLE_NAME, table)
      .mode(SaveMode.Append)
      .save("/nuwa/hudi_hbase/test/")
  }
}
