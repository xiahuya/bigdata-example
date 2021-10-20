package util;

import domain.HiveSyncConfig;
import domain.HoodieConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/7/7
 */
public class HoodieImportHelper {
    private static final Logger log = LoggerFactory.getLogger(HoodieImportHelper.class);


    /**
     * 构建 Config对象
     *
     * @param configPath
     * @return
     */
    public static HoodieConfig buildHoodieConfig(String configPath) {
        HoodieConfig hoodieConfig = new HoodieConfig();
        Properties properties = loadProp(configPath);
        hoodieConfig.setInsertParallelism(properties.getProperty(HoodieImportConstant.HOODIE_INSERT_SHUFFLE_PARALLELISM));
        hoodieConfig.setUpsertParallelism(properties.getProperty(HoodieImportConstant.HOODIE_UNSERT_SHUFFLE_PARALLELISM));
        hoodieConfig.setRowkey(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD));
        hoodieConfig.setPrecombineField(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD));
        hoodieConfig.setPartitionField(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_WRITE_PARTITIONPATH_FIELD));
        hoodieConfig.setStorageType(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_WRITE_STORAGE_TYPE));

        hoodieConfig.setKerberosEnable(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_KERBER_ENABLE));
        hoodieConfig.setHadoopConfDir(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_HADOOP_CONF_DIR));
        hoodieConfig.setKeytab(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_KERBER_KEYTAB));
        hoodieConfig.setKrb5Path(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_KERBER_KRB5_PATH));
        hoodieConfig.setUserPrial(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_KERBER_USER_PRINCIPAL));

        hoodieConfig.setJdbcUrl(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_HIVE_SYNC_JDBCURL));
        hoodieConfig.setUsername(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_HIVE_SYNC_USERNAME));
        hoodieConfig.setPassword(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_HIVE_SYNC_PASSWORD));
        hoodieConfig.setSshHost(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_HOST));
        hoodieConfig.setSshPort(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_PORT));
        hoodieConfig.setSshUser(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_USER));
        hoodieConfig.setSshPass(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_PASS));
        hoodieConfig.setSshShellPath(properties.getProperty(HoodieImportConstant.HOODIE_IMPORT_SSH_HUDI_HIVE_SYNC_SHELL_PATH));

        return hoodieConfig;
    }



    /**
     * 查看hudiWritePath 下是否存在data,判断SaveMode 使用overwrite or Append
     *
     * @return
     */
    public static Boolean isExist(Path hoodieWritePath) {
        Boolean flag = false;
        Configuration config = AuthKerberos.getConfig();
        if (config == null) {
            config = new Configuration();
        }
        try {
            FileSystem fileSystem = hoodieWritePath.getFileSystem(config);
            flag = fileSystem.exists(hoodieWritePath);
        } catch (IOException e) {
            log.error(String.format("读取HDFS 目录异常 \n%s", ExceptionUtil.getStackTrace(e)));
        }
        return flag;
    }


    //todo 后面结合真实场景数据解析
    public static String parseHoodieWritePath(String inputPath, String basePath) {
        return basePath;
    }


    /**
     * 加载配置文件信息
     *
     * @param configPath
     * @return
     * @throws IOException
     */
    private static Properties loadProp(String configPath) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(new File(configPath)));
        } catch (IOException e) {
            log.error(String.format("加载配置文件异常 ----> %s", e.getMessage()));
        }
        return prop;
    }
}
