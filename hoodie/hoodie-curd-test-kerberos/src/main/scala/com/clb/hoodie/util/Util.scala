package com.clb.hoodie.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source

/**
  *
  * @author Xiahu
  * @create 2020/7/17
  */
class Util {

}

object Util{
  /**
    * 根据路径读取文本文件
    *
    * @param filePath
    * @return
    */
  def readFromTxtByLine(filePath: String): (String, Array[String]) = {
    var head: String = ""
    val source = Source.fromFile(filePath, "UTF-8")
    val lines = source.getLines().toArray
    val context = new Array[String](lines.length - 1)
    source.close()
    //println(lines.size)
    for (i <- 0 until lines.length) {
      if (i == 0) {
        head = lines(i)
      } else {
        context(i - 1) = lines(i)
      }
    }

    (head, context)
  }

  /**
    * 文本文件转换为df
    *
    * @param spark
    * @param path
    * @return
    */
  def readFromTxtByLineToDf(spark: SparkSession, path: String): DataFrame = {
    val lines = readFromTxtByLine(path)

    val fields = lines._1.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rdd = spark.sparkContext.parallelize(lines._2)
    val rowrdd = rdd.map(str => {
      val strs = str.split(",")
      Row.fromSeq(strs)
    })

    val textDF = spark.createDataFrame(rowrdd, schema)
    textDF
  }
}
