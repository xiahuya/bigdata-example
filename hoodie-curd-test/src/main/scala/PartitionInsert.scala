package com.clb.hoodie

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

object PartitionInsert {
  def main(args: Array[String]): Unit = {
    var count: Int = 0
//    while (count < 15) {
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
      val readpath = "/user/hive/warehouse/hudi.db/hudi_schema_test2/*"
      val dbTable = "hudi.schema_test"
      val db = dbTable.split("\\.")(0)
      val table = dbTable.split("\\.")(1)
      val spark = SparkSession.builder
        .appName("hudi insert")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .master("local[*]")
        .getOrCreate()

      val insertData = spark.read.parquet(readpath)
      insertData.write.format("org.apache.hudi")
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "p_create_time")
        .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
        .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
        .option("hoodie.insert.shuffle.parallelism", "8")
        .option("hoodie.upsert.shuffle.parallelism", "8")
        .option(HoodieWriteConfig.TABLE_NAME, table)
        /*.option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "hudi_sync")
        .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "schema_test")
        .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
        .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "deta_pre")
        .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.0.112:10000")*/
        .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
        .option("hoodie.cleaner.commits.retained", "15")
        /*.option("hoodie.parquet.max.file.size", "67108864")
        .option("hoodie.parquet.small.file.limit", "104857600")
        .option("hoodie.cleaner.commits.retained", "1")*/
        //.option("hoodie.compact.inline", "true")
        //.option("hoodie.logfile.to.parquet.compression.ratio", "0.35")
        .mode(SaveMode.Append)
        .save("/tmp/xiahu/retained")
      count += 1

      Thread.sleep(1000 * 60)
//    }
  }

}
