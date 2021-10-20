package com.clb.hoodie

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object NoPartitionUpsert {
  def main(args: Array[String]): Unit = {

    val readpath = "/tmp/flink/2020-07-20/14-05_14-10/xh/nuwa_test_1/*"
    val dbTable = "xh.nuwa_test_1"
    val db = dbTable.split("\\.")(0)
    val table = dbTable.split("\\.")(1)

    val spark = SparkSession
      .builder
      .appName("hudi upsert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    val insertData = spark.read.parquet(readpath)

    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, table)
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "1")
      .option("hoodie.upsert.shuffle.parallelism", "1")
      .mode(SaveMode.Append)
      // 写入路径设置
      .save("/nuwa/hudiImport/001/xh/nuwa_test_1")
  }
}
