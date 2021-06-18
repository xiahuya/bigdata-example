package cn.hoodie.compact.util

import java.io.{File, FileReader}
import java.util
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.hive.HiveSyncConfig

object HoodieWriteUtil {
  def buildPamater(): java.util.HashMap[String, String] = {
    val parameters = new java.util.HashMap[String, String]()
    parameters.put("hoodie.datasource.compaction.async.enable","true")
    parameters.put("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    parameters.put("hoodie.datasource.write.insert.drop.duplicates", "false")
    parameters.put("hoodie.datasource.write.partitionpath.field", "p_create_time")
    parameters.put("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload")
    /*
    parameters.put("hoodie.datasource.hive_sync.database", "default")
    parameters.put("hoodie.insert.shuffle.parallelism", "8")
    parameters.put("path", "/tmp/xiahu/retained")
    parameters.put("hoodie.datasource.write.precombine.field", "lastupdatedttm")
    parameters.put("hoodie.datasource.hive_sync.partition_fields", "")

    parameters.put("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
    parameters.put("hoodie.datasource.write.streaming.retry.interval.ms", "2000")
    parameters.put("hoodie.datasource.hive_sync.table", "unknown")
    parameters.put("hoodie.index.type", "GLOBAL_BLOOM")
    parameters.put("hoodie.datasource.write.streaming.ignore.failed.batch", "true")
    parameters.put("hoodie.datasource.write.operation", "upsert")
    parameters.put("hoodie.datasource.hive_sync.enable", "false")
    parameters.put("hoodie.datasource.write.recordkey.field", "rowkey")
    parameters.put("hoodie.table.name", "test")
    parameters.put("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://localhost:10000")

    parameters.put("hoodie.datasource.write.hive_style_partitioning", "false")
    parameters.put("hoodie.bloom.index.update.partition.path", "true")
    parameters.put("hoodie.datasource.hive_sync.username", "hive")
    parameters.put("hoodie.datasource.write.streaming.retry.count", "3")
    parameters.put("hoodie.datasource.hive_sync.password", "hive")
    parameters.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
    parameters.put("hoodie.upsert.shuffle.parallelism", "8")
    parameters.put("hoodie.cleaner.commits.retained", "15")
    parameters.put("hoodie.datasource.write.commitmeta.key.prefix", "_")*/
    parameters
  }

  def buildSchema(): String = {
    val fieldName = Array("id", "fk_id", " qfxh", " jdpj", " nioroa", " gwvz", " joqtf", " isdeleted", " lastupdatedttm", " rowkey")
    val fieldType = Array("varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar")
    SchemaUtil.schemaParse(fieldName, fieldType, "xh.hoodieTableSink").toString
  }

  def buildSyncConfig(basePath: Path, confPropPath: String, tableName: String, isPartitionTable: Boolean): HiveSyncConfig = {
    val prop = new Properties()
    try {
      prop.load(new FileReader(new File(confPropPath)))
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig()
    hiveSyncConfig.basePath = basePath.toString
    hiveSyncConfig.databaseName = prop.get(HIVE_DATABASE_OPT_KEY).toString
    hiveSyncConfig.tableName = tableName
    hiveSyncConfig.hiveUser = prop.get(HIVE_USER_OPT_KEY).toString
    hiveSyncConfig.hivePass = prop.get(HIVE_PASS_OPT_KEY).toString
    hiveSyncConfig.jdbcUrl = prop.get(HIVE_URL_OPT_KEY).toString
    var array: Array[String] = prop.get(HIVE_PARTITION_FIELDS_OPT_KEY).toString.split(",")
    val partitionField = new util.ArrayList[String]()
    array.foreach(partitionField.add(_))
    hiveSyncConfig.partitionFields = partitionField
    if (isPartitionTable) {
      hiveSyncConfig.partitionValueExtractorClass = "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    } else {
      hiveSyncConfig.partitionValueExtractorClass = "org.apache.hudi.hive.NonPartitionedExtractor"
    }
    hiveSyncConfig
  }
}
