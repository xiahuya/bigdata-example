package com.clb.hoodie

import org.apache.spark.sql.SparkSession

object Query {

  def main(args: Array[String]): Unit = {

    //val basePath = "".trim
    val basePath = "/datalake/hid0101_cache_xdcs_pacs_hj_test/hudi_schema_test/"
    val spark = SparkSession.builder
      .appName("query")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[3]")
      .getOrCreate()
    val tripsSnapshotDF = spark.
      read.
      format("org.apache.hudi").
      load(basePath) // /*/*

    tripsSnapshotDF.show(100000)
    tripsSnapshotDF.registerTempTable("hudi_test") //1454
    spark.sql("select count(*) from hudi_test").show(100000000) //124815
    //spark.sql("select rowkey from hudi_test ORDER BY rowkey ASC").show(100000000) //124815
    //spark.sql("select * from hudi_test where rowkey = '5521'").show(100000000)//124815
  }
}
