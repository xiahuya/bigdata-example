package cn.xhjava.flink.submit;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;

/**
 * @author Xiahu
 * @create 2021/5/7
 */
public class ConfigurationLoader {


    public static Configuration flinkConfiguration(String flinkConfigPath) {
        Configuration flinkConfiguration = StringUtils.isEmpty(flinkConfigPath) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkConfigPath);
        /*if (StringUtils.isNotBlank(queue)) {
            flinkConfiguration.setString(YarnConfigOptions.APPLICATION_QUEUE, queue);
        }
        if (StringUtils.isNotBlank(jobid)) {
            flinkConfiguration.setString(YarnConfigOptions.APPLICATION_NAME, jobid);
        }
        if(StringUtils.isNotBlank(yarnconf)){
            flinkConfiguration.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnconf);
        }
        if(CLASS_PATH_PLUGIN_LOAD_MODE.equalsIgnoreCase(pluginLoadMode)){
            flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "child-first");
        }else{
            flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        }
        flinkConfiguration.setString(ConfigConstant.FLINK_PLUGIN_LOAD_MODE_KEY, pluginLoadMode);*/
        return flinkConfiguration;
    }

    public static org.apache.hadoop.conf.Configuration hadoopConfiguration(String hadoopConfigPath){
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        String coreXml = hadoopConfigPath + File.separator + "core-site.xml";
        String hdfsXml = hadoopConfigPath + File.separator + "hdfs-site.xml";
        configuration.addResource(new Path(coreXml));
        configuration.addResource(new Path(hdfsXml));
        return configuration;
    }

    public static YarnConfiguration yarnConfiguration(String yarnConfigPath){
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        String yarnXml = yarnConfigPath + File.separator + "yarn-site.xml";
        yarnConfiguration.addResource(new Path(yarnXml));
        return yarnConfiguration;
    }
}
