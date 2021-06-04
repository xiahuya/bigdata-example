package cn.hoodie.compact


import java.util

import cn.hoodie.compact.util.SchemaUtil
import org.apache.hudi.client.{AbstractHoodieWriteClient, SparkRDDWriteClient}
import org.apache.hudi.{DataSourceUtils, common}
import org.apache.hudi.common.model.HoodieRecordPayload
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions.mapAsJavaMap


object HoodieCompaction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val jsc = new SparkContext(conf)


    val hoodieWriteClient: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]] = Option.empty
    val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc, buildSchema(), "/tmp/hoodieTableSink",
      "hoodieTableSink", buildPamater()
    )).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

    //写
    val map = new java.util.HashMap[String, String]()
    val instantTime = "20210603192022"
    //生成cpmpact计划(创建:20210603191720.compaction.requested)
    client.scheduleCompaction(common.util.Option.of(map), instantTime)

    //执行compact(正式合并逻辑)
    client.simpleCompact();
    System.exit(0)

    jsc.stop
  }

  def buildPamater(): java.util.HashMap[String, String] = {
    val parameters = new java.util.HashMap[String, String]()
    parameters.put("hoodie.datasource.write.insert.drop.duplicates", "false")
    parameters.put("hoodie.datasource.hive_sync.database", "default")
    parameters.put("hoodie.insert.shuffle.parallelism", "8")
    parameters.put("path", "/tmp/xiahu/retained")
    parameters.put("hoodie.datasource.write.precombine.field", "lastupdatedttm")
    parameters.put("hoodie.datasource.hive_sync.partition_fields", "")
    parameters.put("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload")
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
    parameters.put("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    parameters.put("hoodie.datasource.write.hive_style_partitioning", "false")
    parameters.put("hoodie.bloom.index.update.partition.path", "true")
    parameters.put("hoodie.datasource.hive_sync.username", "hive")
    parameters.put("hoodie.datasource.write.streaming.retry.count", "3")
    parameters.put("hoodie.datasource.hive_sync.password", "hive")
    parameters.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
    parameters.put("hoodie.upsert.shuffle.parallelism", "8")
    parameters.put("hoodie.cleaner.commits.retained", "15")
    parameters.put("hoodie.datasource.write.partitionpath.field", "p_create_time")
    parameters.put("hoodie.datasource.write.commitmeta.key.prefix", "_")
    parameters
  }

  def buildSchema(): String = {
    val fieldName = Array("id", "fk_id", " qfxh", " jdpj", " nioroa", " gwvz", " joqtf", " isdeleted", " lastupdatedttm", " rowkey")
    val fieldType = Array("varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar")
    SchemaUtil.schemaParse(fieldName, fieldType, "xh.hoodieTableSink").toString
  }

}
