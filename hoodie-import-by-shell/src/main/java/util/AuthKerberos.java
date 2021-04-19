package util;


import domain.HoodieConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author Xiahu
 * @create 2020/7/7
 */

/**
 * 用于认证kerberos
 */
public class AuthKerberos {
    private static final Logger logger = LoggerFactory.getLogger(AuthKerberos.class);
    private static Configuration configuration;

    public static Configuration getConfig() {
        return configuration;
    }


    public static void startKerneros(HoodieConfig hoodieConfig) {
        if (hoodieConfig.getKerberosEnable().equals("true")) {
            loginHdfs(hoodieConfig);
        } else {
            logger.info(String.format("HoodieImport 程序不需要进行kerberos认证"));
        }
    }

    private static void loginHdfs(HoodieConfig hoodieConfig) {
        buildConfiguration(hoodieConfig);
        if (null != configuration) {
            if (StringUtils.isNotEmpty(hoodieConfig.getKeytab()) && StringUtils.isNotEmpty(hoodieConfig.getKrb5Path())) {
                System.setProperty("java.security.krb5.conf", hoodieConfig.getKrb5Path());
                try {
                    UserGroupInformation.setConfiguration(configuration);
                    UserGroupInformation.loginUserFromKeytab(hoodieConfig.getUserPrial(), hoodieConfig.getKeytab());
                } catch (IOException e) {
                    logger.error(String.format("kerberos 认证异常: %s %s \n", e.getMessage(), ExceptionUtil.getStackTrace(e)));
                }
                logger.info(String.format("Kerberos HDFS 认证成功"));
            } else {
                logger.error(String.format("HDFS 认证kerberos 文件不存在"));
                throw new RuntimeException(String.format("HDFS 认证kerberos 文件不存在"));
            }
        } else {
            logger.error("Hadoop Configuration 尚未初始化成功,请检查配置");
            throw new RuntimeException(String.format("Hadoop Configuration 未加载成功"));
        }

    }

    private static void buildConfiguration(HoodieConfig hoodieConfig) {
        String hadoopConfDir = hoodieConfig.getHadoopConfDir();
        File file = new File(hadoopConfDir);
        if (!file.exists()) {
            logger.error(String.format("路径[%s]不存在,请检查配置", hoodieConfig.getHadoopConfDir()));
            throw new RuntimeException(String.format("路径[%s]不存在,请检查配置", hoodieConfig.getHadoopConfDir()));
        }

        configuration = new Configuration();
        String coreXml = hoodieConfig.getHadoopConfDir() + File.separator + "core-site-28.xml";
        String hdfsXml = hoodieConfig.getHadoopConfDir() + File.separator + "hdfs-site-28.xml";
        configuration.addResource(new Path(coreXml));
        configuration.addResource(new Path(hdfsXml));
        logger.info(String.format("Hadoop Configuration 加载完毕"));
    }
}
