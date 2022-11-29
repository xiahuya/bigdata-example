import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieHBaseIndexConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkReadTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sourceDataPath = "/tmp/kafka_msg.json"


    val spark = SparkSession.builder
      .appName("hudi insert")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    var source: DataFrame = spark.read.json(sourceDataPath)
    source.show(1)
    source.printSchema()


  }



}
