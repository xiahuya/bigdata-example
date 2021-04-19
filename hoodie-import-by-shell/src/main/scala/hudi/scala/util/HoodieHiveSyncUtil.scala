package hudi.scala.util

import com.clb.common.util.{ClientOutput, SSHClient}
import domain.{HiveSyncConfig, HoodieConfig}
import hudi.HudiImportBySparkShell
import hudi.HudiImportBySparkShell.log
import org.apache.hudi.hive.HiveSyncTool
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * @author Xiahu
  * @create 2020/7/9
  */
object HoodieHiveSyncUtil {
  private val log: Logger = LoggerFactory.getLogger(HoodieHiveSyncUtil.getClass)

  /**
    * 封装 用于hiveSync的对象
    *
    * @param hoodieConfig
    * @param databaseName
    * @param tableName
    * @param hoodieWritePath
    * @param flag
    * @return
    */
  def buildHiveSyncConfig(hoodieConfig: HoodieConfig, databaseName: String, tableName: String, hoodieWritePath: String, flag: String): HiveSyncConfig = {
    //封装Config对象
    val configParam = new HiveSyncConfig()
    configParam.setDatabase(databaseName)
    configParam.setTable(tableName)
    configParam.setJdbcurl(hoodieConfig.getJdbcUrl)
    configParam.setSourcePath(hoodieWritePath)
    configParam.setUsername(hoodieConfig.getUsername)
    configParam.setPassword(hoodieConfig.getPassword)
    configParam.setPartition_value_extractor(flag)
    configParam.setPartition_by(flag)
    configParam
  }

  /**
    * ssh远程连接服务器,使用shell脚本同步
    *
    * @param hoodieConfig    HoodieConfig配置对象
    * @param databaseName    数据库名称
    * @param tableName       表名称
    * @param hoodieWritePath hudi写入路径
    * @param flag            是否为分区表
    */
  def hiveSyncByShell(hoodieConfig: HoodieConfig, hiveSyncConfig: HiveSyncConfig): Unit = {
    val command: String = buildHiveSyncShell(hoodieConfig, hiveSyncConfig)
    runCommand(hoodieConfig, command)
  }


  /**
    * 代码内同步数据
    *
    * @param hoodieConfig
    * @param configParam
    */
  def hiveSyncByCode(hoodieConfig: HoodieConfig, configParam: HiveSyncConfig): Unit = {
    var args: Array[String] = Array(
      "--jdbc-url", hoodieConfig.getJdbcUrl,
      "--partition-value-extractor", configParam.getPartition_value_extractor,
      "--user", configParam.getUsername,
      "--pass", configParam.getPassword,
      "--base-path", configParam.getSourcePath,
      "--database", configParam.getDatabase,
      "--table", configParam.getTable)

    var newArgs: Array[String] = args
    if (!"null".equalsIgnoreCase(configParam.getPartition_by)) {
      if (configParam.getSourcePath.startsWith("/datalake")) {
        args = args :+ ("--partitioned-by")
        newArgs = args :+ ("date_prt")
      } else if (configParam.getSourcePath.startsWith("/datacenter")) {
        args = args :+ ("--partitioned-by")
        newArgs = args :+ ("medorgcode,date_prt ")
      } else {
        if (configParam.getDatabase.equalsIgnoreCase("dc_db")) {
          args = args :+ ("--partitioned-by")
          newArgs = args :+ ("medorgcode,date_prt ")
        } else {
          //todo partitioned-by 需要改正
          args = args :+ ("--partitioned-by")
          newArgs = args :+ ("fk_id")
        }
      }
    }
    log.info(newArgs.mkString(" "))
    HiveSyncTool.main(newArgs)
  }


  /**
    * 拼接 shell command
    *
    * @param hoodieConfig
    * @param configParam
    * @return
    */
  def buildHiveSyncShell(hoodieConfig: HoodieConfig, configParam: HiveSyncConfig): String = {
    val stringBuilder = new StringBuilder("sh %s ")
    stringBuilder.append("--base-path %s ")
    stringBuilder.append("--database %s ")
    stringBuilder.append("--table %s ")
    stringBuilder.append("--jdbc-url '%s' ")
    stringBuilder.append("--partition-value-extractor %s ")
    stringBuilder.append("--user %s ")
    stringBuilder.append("--pass %s ")

    if (!"null".equalsIgnoreCase(configParam.getPartition_by)) {
      if (configParam.getSourcePath.startsWith("/datalake")) {
        stringBuilder.append("--partitioned-by date_prt ")
      } else if (configParam.getSourcePath.startsWith("/datacenter")) {
        stringBuilder.append("--partitioned-by medorgcode,date_prt ")
      } else {
        if (configParam.getDatabase.equalsIgnoreCase("dc_db")) {
          stringBuilder.append("--partitioned-by medorgcode,date_prt ")
        } else {
          //todo partitioned-by 需要改正
          stringBuilder.append("--partitioned-by fk_id ")
        }
      }
    }

    stringBuilder.toString().format(
      hoodieConfig.getSshShellPath,
      configParam.getSourcePath,
      configParam.getDatabase,
      configParam.getTable,
      configParam.getJdbcurl,
      configParam.getPartition_value_extractor,
      configParam.getUsername,
      configParam.getPassword)

  }


  /**
    * 远程调用shell,执行hiveSync
    *
    * @param hoodieConfig
    * @param command
    */
  def runCommand(hoodieConfig: HoodieConfig, command: String): Unit = {
    val host: String = hoodieConfig.getSshHost
    val port: Int = hoodieConfig.getSshPort.toInt
    val user: String = hoodieConfig.getSshUser
    val pass: String = hoodieConfig.getSshPass
    val client = new SSHClient(host, port, user, pass)
    val ssh: ClientOutput = client.execCommand(command)
    log.info(command)
    if (ssh.getExitCode == 0) {
      log.info("hudi hive sync success!")
      log.debug(ssh.getText)
    } else {
      log.error(String.format("hudi hive sync error! [%s]", ssh.getText))
      throw new Exception(String.format("hudi hive sync error![%s]", ssh.getText))
    }

  }
}
