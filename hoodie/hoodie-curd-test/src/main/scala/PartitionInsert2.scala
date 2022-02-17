import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieHBaseIndexConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object PartitionInsert2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sourceDataPath = "/tmp/hudi-0.5.2-test-data3.json"
    val table = "test"

    val spark = SparkSession.builder
      .appName("hudi insert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val basePath = "/user/xiahu/hudi/hoodie_xh_table_5"

    hudiInsert(table, sourceDataPath, basePath, spark)

  }

  def hudiInsert(table: String, sourceDataPath: String, targetPath: String, spark: SparkSession): Unit = {
    val insertData = spark.read.json(sourceDataPath)


    insertData.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "lastupdatedttm")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "fk_id")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.HBASE.name())
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
      .option("hoodie.insert.shuffle.parallelism", "8")
      .option("hoodie.upsert.shuffle.parallelism", "8")
      .option(HoodieWriteConfig.TABLE_NAME, table)
      .option(HoodieHBaseIndexConfig.HBASE_ZKQUORUM_PROP, "192.168.0.113")
      .option(HoodieHBaseIndexConfig.HBASE_ZKPORT_PROP, "2181")
      .option(HoodieHBaseIndexConfig.HBASE_GET_BATCH_SIZE_PROP, "1000")
      .option(HoodieHBaseIndexConfig.HBASE_ZK_ZNODEPARENT, "/hbase")
      .option(HoodieHBaseIndexConfig.HBASE_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieHBaseIndexConfig.HBASE_QPS_FRACTION_PROP, "0.5")
      .option(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER_PROP, "1000")
      .option(HoodieHBaseIndexConfig.HBASE_INDEX_QPS_ALLOCATOR_CLASS, "org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator")
      .option(HoodieHBaseIndexConfig.HBASE_TABLENAME_PROP, "hoodie_xh_table_5_index")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "xh")
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "xh_cow_5")
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "date_prt")
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://192.168.0.113:10000")
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .option("hoodie.cleaner.commits.retained", "15")
      .mode(SaveMode.Append)
      .save(targetPath)
  }

}
