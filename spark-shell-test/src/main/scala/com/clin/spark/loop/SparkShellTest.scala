package com.clin.spark.loop

import java.io._
import java.net.{Socket, URI}
import java.util.UUID

import com.clin.spark.loop.config.HoodieConfig
import com.clin.spark.loop.util.HoodieImportHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scala.tools.nsc.GenericRunnerSettings

/**
  * @Author: hj
  * @Date: 2020/6/30 下午10:48
  * @Desc:
  */
object SparkShellTest {

  protected lazy implicit val logger = LoggerFactory.getLogger(getClass)

  private var conf = new SparkConf()


  def createSparkSession(): SparkSession = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")

    var sparkContext: SparkContext = null
    var sparkSession: SparkSession = null

    if (conf.get("spark.master").contains("yarn")) {
      val path: String = conf.get("spark.yarn.dist.files").replace("file://", "")
      logger.info("加载配置文件：" + path)
      HudiImportBySparkShell.hoodieConfig = new HoodieConfig()
      HoodieImportHelper.buildHoodieConfig(HudiImportBySparkShell.hoodieConfig, path)
      UtilHelper.loadProperties(path).foreach(one => {
        conf.set(one._1, one._2)
      })
    }

    /*conf.set("spark.repl.class.outputDir", createTempDir().getAbsolutePath())
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")*/
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }

    val builder = SparkSession.builder.config(conf)
    sparkSession = builder.getOrCreate()
    sparkContext = sparkSession.sparkContext
    sparkSession
  }

  def getLocalUserJarsForShell(conf: SparkConf): Seq[String] = {
    val localJars = conf.getOption("spark.repl.local.jars")
    localJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
  }

  private def scalaOptionError(msg: String): Unit = {
    logger.error(msg)
  }

  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    dir
  }

  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case e: SecurityException => dir = null;
      }
    }

    dir.getCanonicalFile
  }

  def loginHdfsKerberos(mapProperty: Map[String, String]): Unit = {
    val kerberosIsOpen = mapProperty.get("spark.kerberos.is.open").get.toBoolean
    if (kerberosIsOpen) {
      val hadoop_conf_dir = mapProperty.get("hadoop.conf.dir").get
      val hadoopConfig = new Configuration()
      hadoopConfig.addResource(hadoop_conf_dir + File.separator + "core-site.xml")
      hadoopConfig.addResource(hadoop_conf_dir + File.separator + "hdfs-site.xml")
      System.setProperty("java.security.krb5.conf", mapProperty.get("java.security.krb5.conf").get)
      try {
        UserGroupInformation.setConfiguration(hadoopConfig)
        UserGroupInformation.loginUserFromKeytab(mapProperty.get("kerberos.user.name").get, mapProperty.get("java.security.auth.login.config").get)
      } catch {
        case e: IOException =>
          logger.error(String.format("kerberos 认证异常: %s", e.getMessage))
      }
    }
  }


  def main(args: Array[String]): Unit = {
    val propertiesMap = UtilHelper.loadProperties(args(0))


    val outputDir = createTempDir()

    conf = UtilHelper.buildSparkConf(propertiesMap)
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    loginHdfsKerberos(propertiesMap)

    val socket = new Socket(propertiesMap.get("spark.socket.ip").get, propertiesMap.get("spark.socket.port").get.toInt)
    val input = socket.getInputStream
    val output = socket.getOutputStream
    val br = new BufferedReader(new InputStreamReader(input))
    val pw = new PrintWriter(new OutputStreamWriter(output))

    class ILoop(in: Option[BufferedReader], out: PrintWriter) extends SparkILoop(in, out) {
      val customInitializationCommands: Seq[String] = Seq(
        """
        @transient val spark = if (org.apache.spark.repl.Main.sparkSession != null) {
            org.apache.spark.repl.Main.sparkSession
          } else {
            com.clin.spark.loop.SparkShellTest.createSparkSession()
          }
        @transient val sc = {
          val _sc = spark.sparkContext
          if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
            val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
            if (proxyUrl != null) {
              println(
                s"Spark Context Web UI is available at ${proxyUrl}/proxy/${_sc.applicationId}")
            } else {
              println(s"Spark Context Web UI is available at Spark Master Public URL")
            }
          } else {
            _sc.uiWebUrl.foreach {
              webUrl => println(s"Spark context Web UI available at ${webUrl}")
            }
          }
          println("Spark context available as 'sc' " +
            s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
          println("Spark session available as 'spark'.")
          _sc
        }
        """,
        "import org.apache.spark.SparkContext._",
        "import spark.implicits._",
        "import spark.sql",
        "import org.apache.spark.sql.functions._",
        "import com.clin.spark.loop.config.HoodieConfig",
        "import org.apache.hadoop.fs.{ContentSummary, Path}",
        "import org.apache.hudi.DataSourceWriteOptions",
        "import org.apache.hudi.index.HoodieIndex",
        "import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}",
        "import org.apache.spark.sql.{SaveMode, SparkSession}",
        "import org.slf4j.{Logger, LoggerFactory}",
        "import org.apache.spark.sql.SaveMode._",
        "import org.apache.hudi.DataSourceWriteOptions._",
        "import org.apache.hudi.config.HoodieIndexConfig._",
        "import org.apache.hudi.config.HoodieWriteConfig._",
        "import com.clin.spark.loop.HudiImportBySparkShell"

      )


      override def initializeSpark(): Unit = {
        intp.beQuietDuring {
          savingReplayStack {
            customInitializationCommands.foreach(processLine)
          }
        }
      }
    }


    val interp = new ILoop(Option(br), pw)

    val jars = getLocalUserJarsForShell(conf)
      // Remove file:///, file:// or file:/ scheme if exists for each jar
      .map { x => if (x.startsWith("file:")) new File(new URI(x)).getPath else x }
      .mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", jars
    ) ++ args.toList

    val settings = new GenericRunnerSettings(scalaOptionError)

    settings.processArguments(interpArguments, true)
    interp.settings = settings

    /*

    //interp.interpret("sc")
    */
    /*interp.createInterpreter()
    interp.initializeSpark()*/

    interp.process(settings)
    // Option(sparkContext).foreach(_.stop)

  }
}
