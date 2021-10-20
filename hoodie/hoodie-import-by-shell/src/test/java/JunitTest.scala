import domain.{HiveSyncConfig, HoodieConfig}
import hudi.HudiImportBySparkShell
import hudi.scala.util.HoodieHiveSyncUtil
import org.junit.{Before, Test}

/**
  *
  * @author Xiahu
  * @create 2020/7/8
  */
class JunitTest {
  var config: HoodieConfig = null
  var hiveSyncConfig: HiveSyncConfig = null

  @Before
  def init(): Unit = {
    config = new HoodieConfig
    config.setInsertParallelism("2")
    config.setUpsertParallelism("2")
    config.setStorageType("COPY ON WRITE")
    config.setRowkey("rowkey")
    config.setPartitionField("lastupdatedttm")
    config.setPartitionField("fk_id")
    //    config.setJdbcUrl("jdbc:hive2://master:10000/xh;principal=hive/master@HADOOP.COM")
    config.setJdbcUrl("jdbc:hive2://master:10000")
    config.setUsername("hive")
    config.setPassword("hive")
    config.setHadoopConfDir("E:\\conf\\nvwa\\local\\hadoop")
    config.setKerberosEnable("true")
    config.setKeytab("E:\\conf\\nvwa\\local\\hdfs.keytab")
    config.setKrb5Path("E:\\conf\\nvwa\\local\\krb5.conf")
    config.setUserPrial("hdfs/master@HADOOP.COM")
    config.setSshHost("192.168.0.28")
    config.setSshPort("22")
    config.setSshUser("root")
    config.setSshPass("P@ssw0rd123")
    config.setSshShellPath("/opt/hudi/hudi-hive-sync/run_sync_tool.sh")
    hiveSyncConfig = HoodieHiveSyncUtil.buildHiveSyncConfig(
      config,
      "xh",
      "nuwa_hudi_partition",
      "/nuwa/hudiImport/001/nuwa_hudi_partition",
      "true")
  }

  @Test
  def hiveSyncByShell(): Unit = {
    HoodieHiveSyncUtil.hiveSyncByShell(config, hiveSyncConfig)
  }

  @Test
  def hiveSyncByCode(): Unit = {
    HoodieHiveSyncUtil.hiveSyncByCode(config, hiveSyncConfig)
  }

}
