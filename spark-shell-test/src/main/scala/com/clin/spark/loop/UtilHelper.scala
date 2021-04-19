package com.clin.spark.loop

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.hudi.client.HoodieWriteClient
import org.apache.spark.SparkConf


object UtilHelper {
  def loadProperties(path: String): Map[String, String] = {
    val props = new Properties()
    val file = new File(path)
    val inputStream = new FileInputStream(file)
    props.load(inputStream)
    inputStream.close()

    var allConfig = Map[String, String]()
    val iter = props.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey.toString
      val value = if (null == entry.getValue) "" else entry.getValue.toString
      allConfig += (key -> value)
    }
    allConfig
  }

  def getProperties(path: String): Properties = {
    val props = new Properties()
    val file = new File(path)
    val inputStream = new FileInputStream(file)
    props.load(inputStream)
    inputStream.close()
    props
  }

  def buildSparkConf(sparkConfigPath: String): SparkConf = {
    val conf = new SparkConf()
    loadProperties(sparkConfigPath).foreach(one => {
      conf.set(one._1, one._2)
    })

    HoodieWriteClient.registerClasses(conf)

  }


  def buildSparkConf(mapProperties :  Map[String, String]): SparkConf = {
    val conf = new SparkConf()
    mapProperties.foreach(one => {
      conf.set(one._1, one._2)
    })

    HoodieWriteClient.registerClasses(conf)

  }
}
