package com.clb.hoodie

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

object Query {

  def main(args: Array[String]): Unit = {

    //val basePath = "".trim
    val basePath = "/tmp/xiahu/xh_mor/*/*"
    val spark = SparkSession.builder
      .appName("query")

      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[3]")
      .getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath) // /*/*

    tripsSnapshotDF.show(100000)
    tripsSnapshotDF.registerTempTable("hudi_test") //1454
    //   spark.sql("select count(*) from hudi_test").show(100000000) //124815
    //spark.sql("select rowkey from hudi_test ORDER BY rowkey ASC").show(100000000) //124815
    //spark.sql("select * from hudi_test where rowkey = '5521'").show(100000000)//124815
  }
}
