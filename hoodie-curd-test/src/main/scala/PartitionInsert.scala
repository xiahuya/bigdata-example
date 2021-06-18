package com.clb.hoodie

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

object PartitionInsert {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val readpath = "/user/hive/warehouse/hudi.db/hudi_schema_test2/*"
    val dbTable = "hudi.schema_test"
    val db = "xh"
    val table = "test"

    val spark = SparkSession.builder
      .appName("hudi insert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val basePath = "/tmp/flink/nuwa_mulit_partition_test/2020-10-20/%s/hid0101_cache_xdcs_pacs_hj/merge_test10/*"

    for (i <- 99 until 120) {
      if (i == 15 || i == 42 || i == 43 || i == 75 || i == 76 || i == 99 || i == 100) {
        print("err")
      } else {
        hudiInsert(table, basePath.format(i), spark)
        //Thread.sleep(30000)
      }
    }

  }

  def hudiInsert(table: String, sourceDataPath: String, spark: SparkSession): Unit = {
    /* val spark = SparkSession.builder
       .appName("hudi insert")
       .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       .master("local[*]")
       .getOrCreate()*/

    val insertData = spark.read.parquet(sourceDataPath)
    insertData.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "rowkey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      //      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "p_create_time")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "sex")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
      .option("hoodie.insert.shuffle.parallelism", "8")
      .option("hoodie.upsert.shuffle.parallelism", "8")
      .option(HoodieWriteConfig.TABLE_NAME, table)
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "xh")
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "test")
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "sex")
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.0.113:10000")
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .option("hoodie.cleaner.commits.retained", "15")
      /*.option("hoodie.parquet.max.file.size", "67108864")
      .option("hoodie.parquet.small.file.limit", "104857600")
      .option("hoodie.cleaner.commits.retained", "1")*/
      //.option("hoodie.compact.inline", "true")
      //.option("hoodie.logfile.to.parquet.compression.ratio", "0.35")
      .mode(SaveMode.Append)
      .save("/tmp/xiahu/retained")
  }

}
