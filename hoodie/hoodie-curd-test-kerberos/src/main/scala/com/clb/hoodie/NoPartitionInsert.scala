package com.clb.hoodie

import com.clb.hoodie.util.KerberosLogin
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object NoPartitionInsert {
  def main(args: Array[String]): Unit = {
    val login = new KerberosLogin()
    login.kerberosLogin()

    val readpath = ""
    val dbTable = ""
    val db = dbTable.split("\\.")(0)
    val table = dbTable.split("\\.")(1)

    val spark = SparkSession.builder
      .appName("hudi insert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val insertData = spark.read.parquet(readpath)
    insertData.write.format("org.apache.hudi")
      // 设置主键列名
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY, table)
      // 设置数据更新时间的列名
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "1")
      .option("hoodie.upsert.shuffle.parallelism", "1")
      // 表名称设置
      .option(HoodieWriteConfig.TABLE_NAME, table)
      .mode(SaveMode.Overwrite)
      // 写入路径设置
      .save("")
  }
}
