package com.clb.hoodie

import com.clb.hoodie.util.KerberosLogin
import org.apache.spark.sql.SparkSession

object Query {

  def main(args: Array[String]): Unit = {
    val login = new KerberosLogin()
    login.kerberosLogin()

    val basePath = "/nuwa-import/xh/test"
//    val basePath = "/nuwa/hudiImport/001/xh/test3"
    val spark = SparkSession.builder
      .appName("query")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[3]")
      .getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath + "/*")

    tripsSnapshotDF.show(100)
    tripsSnapshotDF.registerTempTable("hudi_test")
    spark.sql("select count(*) from hudi_test").show(100000000)
  }
}
