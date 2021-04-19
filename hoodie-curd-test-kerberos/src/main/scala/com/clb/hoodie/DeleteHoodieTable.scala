package com.clb.hoodie

import com.clb.hoodie.util.KerberosLogin
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.{EmptyHoodieRecordPayload, OverwriteWithLatestAvroPayload}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * @author Xiahu
  * @create 2020/7/16
  */
object DeleteHoodieTable {
  def main(args: Array[String]): Unit = {
    val login = new KerberosLogin()
    login.kerberosLogin()

    val readpath = "/tmp/flink/nvwa_partition_change_delete/2020-07-16/18-50_18-55/xh/test/p_dt=d_*"
    val dbTable = "xh.test"
    val db = dbTable.split("\\.")(0)
    val table = dbTable.split("\\.")(1)

    val spark = SparkSession.builder
      .appName("hudi insert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    val insertData = spark.read.parquet(readpath)
    insertData.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "p_dt")
      .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, classOf[EmptyHoodieRecordPayload].getName)
      .option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY, table)
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TABLE_NAME, table)
      .mode(SaveMode.Append)
      .save("/nuwa/hudiImport/001/xh/test3")
  }
}
