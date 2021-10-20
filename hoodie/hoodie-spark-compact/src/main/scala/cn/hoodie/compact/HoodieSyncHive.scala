package cn.hoodie.compact

import cn.hoodie.compact.util.HoodieWriteUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncTool}


object HoodieSyncHive {

  /*def main(args: Array[String]): Unit = {
    val confPropPath = "D:\\code\\github\\bigdata-example\\hoodie-spark-compact\\src\\main\\resources\\hoodie-synchive.properties"
    val basePath = "/tmp/xiahu/xh_cow"
    val tableName = "xh_cow"
    val isPartitionTable = true
    syncHive(new Path(basePath), confPropPath, tableName, isPartitionTable)
  }


  def syncHive(basePath: Path, confPropPath: String, tableName: String, isPartitionTable: Boolean): Unit = {
    val hiveSyncConfig: HiveSyncConfig = HoodieWriteUtil.buildSyncConfig(basePath, confPropPath, tableName, isPartitionTable)
    val hiveConf = new HiveConf()
    val fs = FileSystem.get(new Configuration())
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable()
  }*/
}

