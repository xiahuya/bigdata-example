package com.clb.hoodie

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object NoPartitionInsert {
  def main(args: Array[String]): Unit = {
    val readpath = "/user/hive/warehouse/hudi.db/hudi_schema_test/202011241436190429.parquet"
    val dbTable = "test.test"
    val db = dbTable.split("\\.")(0)
    val table = dbTable.split("\\.")(1)

    val spark = SparkSession.builder
      .appName("hudi insert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local")
      .getOrCreate()
    val insertData = spark.read.parquet(readpath)
    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY, table)
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "false")
//      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, "incremental")
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "1")
      .option("hoodie.upsert.shuffle.parallelism", "1")
      .option("hoodie.cleaner.commits.retained", "3")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, table)
      .mode(SaveMode.Append)
      // 写入路径设置
      .save("/tmp/xiahu/retained")
  }
}
