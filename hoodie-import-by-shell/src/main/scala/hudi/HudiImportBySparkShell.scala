package hudi

import java.lang

import com.clb.common.util.{ClientOutput, SSHClient}
import domain.{HiveSyncConfig, HoodieConfig}
import hudi.scala.util.HoodieHiveSyncUtil
import org.apache.hadoop.fs.{ContentSummary, Path}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import util.{AuthKerberos, ExceptionUtil, HoodieImportHelper}
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieIndexConfig._
import org.apache.hudi.config.HoodieWriteConfig._

/**
  * 写入hudi
  */
object HudiImportBySparkShell {
  private val log: Logger = LoggerFactory.getLogger(HudiImportBySparkShell.getClass)

  var hoodieConfig: HoodieConfig = null


  /**
    * 导入hudi
    *
    * @param spark                   spark session对象
    * @param configPath              配置文件路径
    * @param hudiTable               hudi表名称(库名.表名)
    * @param basePath                hudi表路径
    * @param inputPath               hdfs 文件路径
    * @param hudiPartitionColumnName 是否为分区表,是 : 分区列  不是:partition_false
    */
  def importDataToHudi(spark: SparkSession,
                       configPath: String,
                       hudiTable: String,
                       basePath: String,
                       inputPath: String,
                       hudiPartitionColumnName: String): String = {

    var result: String = "true"
    try {
      val table = hudiTable.split("\\.")
      val databaseName = table(0)
      val tableName = table(1)

      hoodieConfig = HoodieImportHelper.buildHoodieConfig(configPath)
      AuthKerberos.startKerneros(hoodieConfig)

      //根据inputPath + basePath 解析hudi table writePath
      val hoodieWritePath: String = HoodieImportHelper.parseHoodieWritePath(inputPath, basePath)
      //判断 hoodieImport 时使用overwrite or append
      val exists: lang.Boolean = HoodieImportHelper.isExist(new Path(hoodieWritePath))

      var flag = "false"
      if (!hudiPartitionColumnName.equals("partition_false")) {
        flag = "true"
      }


      //    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

      val insertData = spark.read.parquet(inputPath)

      var data = insertData.write.format("org.apache.hudi")
        //copy_on_writer or merge_on_read
        .option(TABLE_TYPE_OPT_KEY, hoodieConfig.getStorageType)
        .option(RECORDKEY_FIELD_OPT_KEY, hoodieConfig.getRowkey)
        // 设置数据更新时间的列名
        .option(PRECOMBINE_FIELD_OPT_KEY, hoodieConfig.getPrecombineField)
        //merge逻辑class
        .option(PAYLOAD_CLASS_OPT_KEY, DataSourceWriteOptions.DEFAULT_PAYLOAD_OPT_VAL)


      //1.判断当前ods表是否为分区表
      if ("true".equals(flag)) {
        //该表为分区表
        data
          .option(PARTITIONPATH_FIELD_OPT_KEY, hoodieConfig.getPartitionField) //fk_id
          //表数据发生变更时,分区是否发生变更
          .option(BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //true
          //设置全局索引
          .option(INDEX_TYPE_PROP, HoodieIndex.IndexType.BLOOM.name())
          .option(KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
      } else {
        data.option(KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
      }

      // 表名称设置
      data
        // 并行度参数设置
        .option("hoodie.insert.shuffle.parallelism", hoodieConfig.getInsertParallelism)
        .option("hoodie.upsert.shuffle.parallelism", hoodieConfig.getUpsertParallelism)
        .option(TABLE_NAME, hudiTable)
        .mode(if (exists) Append else Overwrite)
        // 写入路径设置
        .save(hoodieWritePath)
      result
    } catch {
      case e: Exception => {
        log.error(ExceptionUtil.getStackTrace(e))
        result = "false"
      }
        result
    }


    //val hiveSyncConfig: HiveSyncConfig = HoodieHiveSyncUtil.buildHiveSyncConfig(hoodieConfig, databaseName, tableName, hoodieWritePath, flag)

    //todo 公司环境不允许使用shell同步
    /*HoodieHiveSyncUtil.hiveSyncByShell(hoodieConfig, hiveSyncConfig)*/
    //HoodieHiveSyncUtil.hiveSyncByCode(hoodieConfig, hiveSyncConfig)
  }


  def main(args: Array[String]): Unit = {
    val confPath = "D:\\git\\apacheBigdata\\nuwa\\hudi-import-on-spakshell\\src\\main\\profiles\\dev\\hoodieImport.properties"
    //val confPath = "/home/xiahu/hoodieImport.properties"
    hoodieConfig = HoodieImportHelper.buildHoodieConfig(confPath)
    AuthKerberos.startKerneros(hoodieConfig)
    val hudiTable = "nuwa_hoodie_import_test.nuwa_hudi_partition"
    val basePath = "/nuwa/hudiImport/002/nuwa_hudi_partition/*"
    val inputPath = "/user/hive/warehouse/nuwa_hoodie_import_test.db/nuwa_hudi_import_test_table_1/*"
    val hudiPartitionColumnName = "fk_id"
    //构建SparkSession
    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    importDataToHudi(spark, confPath, hudiTable, basePath, inputPath, hudiPartitionColumnName)

  }
}
