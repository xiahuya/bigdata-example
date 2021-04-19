package com.clin.spark.loop.util;

import com.clin.spark.loop.config.HoodieConfig;
import com.clin.spark.loop.config.HoodieImportConstant;
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
 * @Author: hj
 * @Date: 2020/7/16 上午9:47
 * @Desc:
 */
public class HoodieImportHelper {
    private static final Logger log = LoggerFactory.getLogger(HoodieImportHelper.class);


    /**
     * 构建 Config对象
     *
     * @param configPath
     * @return
     */
    public static HoodieConfig buildHoodieConfig(HoodieConfig hoodieConfig,String configPath) {
        Properties properties = loadProp(configPath);
        hoodieConfig.setInsertParallelism(properties.getProperty(HoodieImportConstant.HOODIE_INSERT_SHUFFLE_PARALLELISM));
        hoodieConfig.setUpsertParallelism(properties.getProperty(HoodieImportConstant.HOODIE_UNSERT_SHUFFLE_PARALLELISM));
        hoodieConfig.setRowkey(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD));
        hoodieConfig.setPrecombineField(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD));
        hoodieConfig.setStorageType(properties.getProperty(HoodieImportConstant.HOODIE_DATASOURCE_WRITE_STORAGE_TYPE));

        hoodieConfig.setHadoopConfDir(properties.getProperty(HoodieImportConstant.HADOOP_CONF_DIR));

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
    public static Boolean isExist(Path hoodieWritePath,String hadoopPath) {
        Boolean flag = false;
        Configuration config = new Configuration();
        config.addResource(hadoopPath + File.separator + "core-site.xml");
        config.addResource(hadoopPath + File.separator + "hdfs-site.xml");

        try {
            FileSystem fileSystem = hoodieWritePath.getFileSystem(config);
            flag = fileSystem.exists(hoodieWritePath);
        } catch (IOException e) {
            log.error(String.format("读取HDFS 目录异常 \n%s", ExceptionUtil.getStackTrace(e)));
        }
        return flag;
    }

    public static Configuration getHdfsConfig(String hadoopPath) {
        Configuration config = new Configuration();
        config.addResource(hadoopPath + File.separator + "core-site.xml");
        config.addResource(hadoopPath + File.separator + "hdfs-site.xml");
        return config;
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
