package cn.hoodie.compact


import java.util

import cn.hoodie.compact.util.{HoodieWriteUtil, SchemaUtil}
import org.apache.hudi.client.{AbstractHoodieWriteClient, SparkRDDWriteClient}
import org.apache.hudi.{DataSourceUtils, common}
import org.apache.hudi.common.model.HoodieRecordPayload
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 基于hoodie 0.9.0 版本 hoodie mor 表 压缩合并操作
  * 20210618112844 /tmp/hoodie_callback/test test
  */
object HoodieCompaction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("Word Count")
    val jsc = new SparkContext(conf)

    val instantTime = args(0)
    val basePath = args(1)
    val tableName = args(2)


    val hoodieWriteClient: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]] = Option.empty
    val client = hoodieWriteClient.getOrElse(
      DataSourceUtils.createHoodieClient(jsc,
        HoodieWriteUtil.buildSchema(), // 必须构造schema
        basePath,
        tableName,
        HoodieWriteUtil.buildPamater()
      )).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

    //写
    val map = new java.util.HashMap[String, String]()
    //生成cpmpact计划(创建:20210603191720.compaction.requested)
    // 调用存在序列化问题,无法使用
    // client.scheduleCompaction(common.util.Option.of(map), instantTime)

    //执行compact(正式合并逻辑)
    client.simpleCompact();




    jsc.stop
    System.exit(0)


  }


}
