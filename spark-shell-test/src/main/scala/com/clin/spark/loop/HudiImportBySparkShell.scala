package com.clin.spark.loop

import com.clin.spark.loop.config.HoodieConfig
import com.clin.spark.loop.util.{ExceptionUtil, HoodieImportHelper}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.EmptyHoodieRecordPayload
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.config.HoodieIndexConfig._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}

/**
  * @Author: hj
  * @Date: 2020/7/16 上午9:38
  * @Desc:
  */
object HudiImportBySparkShell {
  private val log: Logger = LoggerFactory.getLogger(HudiImportBySparkShell.getClass)

  var hoodieConfig: HoodieConfig = null


  /**
    * 导入hudi
    *
    * @param spark                   spark session对象
    * @param hudiTable               hudi表名称(库名.表名)
    * @param basePath                hudi表路径
    * @param inputPath               hdfs 文件路径
    * @param hudiPartitionColumnName 是否为分区表,是 : 分区列  不是:""
    */
  def importDataToHudi(spark: SparkSession,
                       hudiTable: String,
                       basePath: String,
                       inputPath: String,
                       isPartitionTable: Boolean,
                       hudiPartitionColumnName: String): (Boolean, Long) = {
    val startTime = System.currentTimeMillis()

    var result: Boolean = true
    try {
      val table = hudiTable.split("\\.")
      val databaseName = table(0)
      val tableName = table(1)

      val inputDirPath: String = inputPath.replace("/*", "")

      // 根据inputPath + basePath 解析hudi table writePath
      val hoodieWritePath: String = HoodieImportHelper.parseHoodieWritePath(inputPath, basePath)
      // 判断 hoodieImport 时使用overwrite or append
      val exists: Boolean = HoodieImportHelper.isExist(new Path(hoodieWritePath), hoodieConfig.getHadoopConfDir)


      //在进行hudi upsert 前,执行hudi 表数据硬删除操作
      var isExistDeleteData: Boolean = false
      if (isPartitionTable) {
        try {
          val fileSystem = new Path(inputDirPath).getFileSystem(HoodieImportHelper.getHdfsConfig(hoodieWritePath))
          isExistDeleteData = hudiHardDelete(spark, hoodieConfig, fileSystem, inputDirPath, tableName, hoodieWritePath, hudiPartitionColumnName)
        } catch {
          case e: Exception => {
            log.error(ExceptionUtil.getStackTrace(e))
            throw new RuntimeException("Hoodie HardDelete Error \n%s".format(ExceptionUtil.getStackTrace(e)))
          }
        }
      }

      var readDataPath: String = null
      if (isExistDeleteData) {
        //存在 分区变更数据,只加载 p_xxxx=xxx目录数据
        readDataPath = inputDirPath + "/" + hudiPartitionColumnName + "=*"
      } else {
        readDataPath = inputDirPath + "/*"
      }

      val insertData: DataFrame = spark.read.parquet(readDataPath)

      val data = insertData.write.format("org.apache.hudi")
        //copy_on_writer or merge_on_read
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, hoodieConfig.getStorageType)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, hoodieConfig.getRowkey)
        // 设置数据更新时间的列名
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, hoodieConfig.getPrecombineField)
        //merge逻辑class
        .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, DataSourceWriteOptions.DEFAULT_PAYLOAD_OPT_VAL)


      //1.判断当前ods表是否为分区表
      if (isPartitionTable) {
        //该表为分区表
        data
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, hudiPartitionColumnName) //fk_id
          //表数据发生变更时,分区是否发生变更
          .option(BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //true
          //设置全局索引
          .option(INDEX_TYPE_PROP, HoodieIndex.IndexType.BLOOM.name())
          .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[SimpleKeyGenerator].getName)
      } else {
        data.option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
      }

      // 表名称设置
      data
        // 并行度参数设置
        .option("hoodie.insert.shuffle.parallelism", hoodieConfig.getInsertParallelism)
        .option("hoodie.upsert.shuffle.parallelism", hoodieConfig.getUpsertParallelism)
        .option(HoodieWriteConfig.TABLE_NAME, hudiTable)
        .mode(if (exists) Append else Overwrite)
        .save(hoodieWritePath)
      (result, System.currentTimeMillis() - startTime)
    } catch {
      case e: Exception => {
        log.error(ExceptionUtil.getStackTrace(e))
        result = false
        (result, System.currentTimeMillis() - startTime)
      }
        (result, System.currentTimeMillis() - startTime)
    }


    // val hiveSyncConfig: HiveSyncConfig = HoodieHiveSyncUtil.buildHiveSyncConfig(hoodieConfig, databaseName, tableName, hoodieWritePath, flag)

    //todo 公司环境不允许使用shell同步
    /*HoodieHiveSyncUtil.hiveSyncByShell(hoodieConfig, hiveSyncConfig)*/
    //HoodieHiveSyncUtil.hiveSyncByCode(hoodieConfig, hiveSyncConfig)
  }


  //执行hudi 硬删除操作;
  val d_xxxPath: String = "%s/d_%s=*"
  val deletePartitionName = "d_%s"

  def hudiHardDelete(spark: SparkSession,
                     hoodieConfig: HoodieConfig,
                     fileSystem: FileSystem,
                     inputPath: String,
                     tableName: String,
                     writePath: String,
                     partitionColumn: String): Boolean = {
    var flag: Boolean = false

    //判断是否存在d_xxx= 目录
    val fileStatuses: Array[FileStatus] = fileSystem.listStatus(new Path(inputPath))
    val partitionName: String = deletePartitionName.format(partitionColumn)
    val fileIterator: Iterator[FileStatus] = fileStatuses.iterator
    while (fileIterator.hasNext) {
      val status: FileStatus = fileIterator.next()
      if (status.isDirectory) {
        val path: String = status.getPath.toString
        if (path.contains(partitionName)) {
          flag = true
        }
      }
    }

    if (flag) {
      val readpath = d_xxxPath.format(inputPath, partitionColumn)
      val insertData = spark.read.parquet(readpath)
      insertData.write.format("org.apache.hudi")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, hoodieConfig.getRowkey)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, hoodieConfig.getPrecombineField)
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, partitionColumn)
        .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, classOf[EmptyHoodieRecordPayload].getName)
        .option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY, tableName)
        .option("hoodie.insert.shuffle.parallelism", hoodieConfig.getInsertParallelism)
        .option("hoodie.upsert.shuffle.parallelism", hoodieConfig.getUpsertParallelism)
        .option(HoodieWriteConfig.TABLE_NAME, tableName)
        .mode(SaveMode.Append)
        .save(writePath)
    }
    flag
  }


  def main(args: Array[String]): Unit = {
    hoodieConfig = new HoodieConfig
    //    val confPath = "/home/huangjing/soft/git/experiment/spark-shell-test/src/main/resources/conf/spark-shell-test.properties"
    val confPath = "D:\\git\\apacheBigdata\\clb-bigdata-example\\spark-shell-test\\src\\main\\resources\\conf\\spark-shell-xiahu.properties"
    HoodieImportHelper.buildHoodieConfig(HudiImportBySparkShell.hoodieConfig, confPath)

    // val hudiTable = "test_db.test_table_11"
    // val basePath = "hdfs://192.168.0.111:8020/nuwa/hudi/test_db/test_table_target_11"
    // val inputPath = "hdfs://192.168.0.111:8020/user/hive/warehouse/test_db.db/test_table_11/.20200714090641/*"
    // val hudiPartitionColumnName = "fk_id"
    // val isPartition:Boolean = false


    val hudiTable = "test_db.test_table_1"
    val basePath = "/nuwa/hudi/test_db/test_table_target_1"
    val inputPath = "/user/hive/warehouse/test_db.db/test_table_1/.20200714090652/*"
    val hudiPartitionColumnName = "fk_id"
    val isPartition: Boolean = true


    //构建SparkSession
    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    // importDataToHudi(spark, confPath, hudiTable, basePath, inputPath,false, hudiPartitionColumnName)
    HudiImportBySparkShell.importDataToHudi(spark, hudiTable, basePath, inputPath, isPartition, hudiPartitionColumnName)

  }
}
