package cn.hoodie.compact

import cn.hoodie.compact.util.HoodieWriteUtil
import org.apache.hudi.DataSourceUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.HoodieRecordPayload
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于hoodie 0.9.0 版本 hoodie clean 操作
  * 20210617162438 /tmp/hoodie_callback/test test
  */
object HoodieClean {
  def main(args: Array[String]): Unit = {
   /* val conf = new SparkConf().setMaster("local[3]").setAppName("Word Count")
    val jsc = new SparkContext(conf)
    val commitTime = args(0)
    val basePath = args(1)
    val tableName = args(2)


    val hoodieWriteClient: Option[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]] = Option.empty
    val client = hoodieWriteClient.getOrElse(DataSourceUtils.createHoodieClient(jsc, null, basePath,
      tableName, HoodieWriteUtil.buildPamater()
    )).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
    //client.simpleClean(commitTime, true)
    System.exit(0)*/
  }
}
