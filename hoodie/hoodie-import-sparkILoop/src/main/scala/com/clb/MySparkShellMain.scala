//package com.clb


//import java.io.{BufferedReader, File, InputStream, InputStreamReader, OutputStream, OutputStreamWriter}
//import java.net.{Socket, URI}
//import java.util.Locale
//import com.clb.spark.MySparkILoop
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
//import org.apache.spark.util.Utils
//
//import scala.tools.nsc.interpreter.JPrintWriter


/**
  *
  * @author Xiahu
  * @create 2020/7/15
  */

package org.apache.spark {

  import java.io.{BufferedReader, File, InputStream, InputStreamReader, OutputStream, OutputStreamWriter}
  import java.net.{Socket, URI}
  import java.util.Locale

  import com.clb.spark.MySparkILoop
  import org.apache.spark.internal.Logging
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.util.Utils
  import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

  import scala.tools.nsc.GenericRunnerSettings
  import scala.tools.nsc.interpreter.JPrintWriter


  object MySparkShellMain extends Logging {
    var interp: MySparkILoop = _

    private var hasErrors = false
    private var isShellSession = false
    var jPrintWriter: JPrintWriter = null
    var sparkContext: SparkContext = _
    var sparkSession: SparkSession = _
    val conf = new SparkConf()

    val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
    val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")


    def main(args: Array[String]): Unit = {
      val socket = new Socket("192.168.0.113", 8888)
      val inputStream: InputStream = socket.getInputStream
      val outputStream: OutputStream = socket.getOutputStream

      val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
      val maybeReader: Option[BufferedReader] = Option[BufferedReader](bufferedReader)
      val jPrintWriter = new JPrintWriter(new OutputStreamWriter(outputStream))

      val mySparkILoop = new MySparkILoop(maybeReader, jPrintWriter)


      interp = mySparkILoop
      val jars = Utils.getLocalUserJarsForShell(conf)
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

//      if (!hasErrors) {
//        interp.process(settings) // Repl starts and goes in loop of R.E.P.L
//        Option(sparkContext).foreach(_.stop)
//      }
      interp.process(settings)

    }

    private def scalaOptionError(msg: String): Unit = {
      hasErrors = true
      Console.err.println(msg)
    }

    def createSparkSession(): SparkSession = {
      try {
        val execUri = System.getenv("SPARK_EXECUTOR_URI")
        conf.setIfMissing("spark.app.name", "Spark shell")
        // SparkContext will detect this configuration and register it with the RpcEnv's
        // file server, setting spark.repl.class.uri to the actual URI for executors to
        // use. This is sort of ugly but since executors are started as part of SparkContext
        // initialization in certain cases, there's an initialization order issue that prevents
        // this from being set after SparkContext is instantiated.
        conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
        if (execUri != null) {
          conf.set("spark.executor.uri", execUri)
        }
        if (System.getenv("SPARK_HOME") != null) {
          conf.setSparkHome(System.getenv("SPARK_HOME"))
        }

        val builder = SparkSession.builder.config(conf)
        if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase(Locale.ROOT) == "hive") {
          if (SparkSession.hiveClassesArePresent) {
            // In the case that the property is not set at all, builder's config
            // does not have this value set to 'hive' yet. The original default
            // behavior is that when there are hive classes, we use hive catalog.
            sparkSession = builder.enableHiveSupport().getOrCreate()
            logInfo("Created Spark session with Hive support")
          } else {
            // Need to change it back to 'in-memory' if no hive classes are found
            // in the case that the property is set to hive in spark-defaults.conf
            builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
            sparkSession = builder.getOrCreate()
            logInfo("Created Spark session")
          }
        } else {
          // In the case that the property is set but not to 'hive', the internal
          // default is 'in-memory'. So the sparkSession will use in-memory catalog.
          sparkSession = builder.getOrCreate()
          logInfo("Created Spark session")
        }
        sparkContext = sparkSession.sparkContext
        sparkSession
      } catch {
        case e: Exception if isShellSession =>
          logError("Failed to initialize Spark session.", e)
          sys.exit(1)
      }
    }
  }

}

