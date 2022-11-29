import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode, SparkSession}

object Hoodie12Writer {


  def main(args: Array[String]): Unit = {
    val sourcePath = "/tmp/kafka_msg/"
    val targetPath = "/flink/his_v1/sqluser/test_struct"
    val dbTable = "hid0101_cache_his_sqluser.test_struct"
    val db = dbTable.split("\\.")(0)
    val table = dbTable.split("\\.")(1)


    val spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local")
      .getOrCreate()
    val insertData = spark.read.json(sourcePath)
    var data: DataFrameWriter[Row] = insertData.write.format("hudi")
      .option(HoodieWriteConfig.TBL_NAME.key(), dbTable)
      .option(DataSourceWriteOptions.OPERATION.key(), "upsert")
      .option(DataSourceWriteOptions.TABLE_NAME.key(), dbTable)
      .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "rowkey")
      .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "op_ts")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED.key(), "true")
      .option(DataSourceWriteOptions.HIVE_DATABASE.key(), db)
      .option(DataSourceWriteOptions.HIVE_TABLE.key(), table)
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS.key(), "org.apache.hudi.hive.NonPartitionedExtractor")
      .option(DataSourceWriteOptions.HIVE_URL.key(), "jdbc:hive2://node2:10000")
      .option(DataSourceWriteOptions.HIVE_USER.key(), "hive")
      .option("hoodie.embed.timeline.server", "false")

    data
      .mode(SaveMode.Append)
      .save(targetPath)
  }
}
