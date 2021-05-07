
package cn.xhjava.flink.submit.util;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class ClusterClientFactory {

    private final static Logger LOG = LoggerFactory.getLogger(ClusterClientFactory.class);

    private YarnClient client;
    private org.apache.hadoop.conf.Configuration configuration = null;
    private YarnConfiguration yarnConfiguration;

    private String hadoopConfigPath;

    private String krb5conf;

    private String user;

    private String keytab;

    private Boolean kerberosEnable = false;

    private Configuration flinkConfig = new Configuration();

    public ClusterClientFactory(String hadoopConfigPath) {
        this.hadoopConfigPath = hadoopConfigPath;
    }

    private void buildNuwaYarnClient() {
        if (kerberosEnable) {
            try {
                getHadoopConf(hadoopConfigPath);
                System.setProperty("java.security.krb5.conf", krb5conf);
                UserGroupInformation.setConfiguration(configuration);
                UserGroupInformation.loginUserFromKeytab(user, keytab);
                client = YarnClient.createYarnClient();
                client.init(configuration);
                client.start();
            } catch (IOException e) {
                LOG.error(String.format("kerberos 认证异常: %s", e.getMessage()));
            }
        } else {
            getHadoopConf(hadoopConfigPath);
            try {
                client = YarnClient.createYarnClient();
                client.init(configuration);
                client.start();
            } catch (Exception e) {
                LOG.error(String.format("Get Yarn Client Info field ! \n%s", ExceptionUtil.getStackTrace(e)));
            }
        }
    }


    private void getHadoopConf(String hadoopConfigPath) {
        String coreXml = hadoopConfigPath + File.separator + "core-site.xml";
        String hdfsXml = hadoopConfigPath + File.separator + "hdfs-site.xml";
        String yarnXml = hadoopConfigPath + File.separator + "yarn-site.xml";
        configuration = new org.apache.hadoop.conf.Configuration();
        configuration.addResource(new Path(coreXml));
        configuration.addResource(new Path(yarnXml));
        configuration.addResource(new Path(hdfsXml));
        getYarnConf(hadoopConfigPath);
    }

    private void getYarnConf(String hadoopConfigPath) {
        yarnConfiguration = new YarnConfiguration();
        String coreXml = hadoopConfigPath + File.separator + "core-site.xml";
        String hdfsXml = hadoopConfigPath + File.separator + "hdfs-site.xml";
        String yarnXml = hadoopConfigPath + File.separator + "yarn-site.xml";
        yarnConfiguration.addResource(new Path(coreXml));
        yarnConfiguration.addResource(new Path(yarnXml));
        yarnConfiguration.addResource(new Path(hdfsXml));
    }


    public ClusterClient createYarnClient(String appId) {
        ApplicationId applicationId;
        try {
            this.buildNuwaYarnClient();
            if (StringUtils.isEmpty(appId)) {
                applicationId = getAppIdFromYarn(client);
                if (applicationId == null || StringUtils.isEmpty(applicationId.toString())) {
                    throw new RuntimeException("No flink session found on yarn cluster.");
                }
            } else {
                applicationId = ConverterUtils.toApplicationId(appId);
            }

            try (YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                    flinkConfig,
                    yarnConfiguration,
                    client,
                    YarnClientYarnClusterInformationRetriever.create(client),
                    true)) {
                return yarnClusterDescriptor.retrieve(applicationId).getClusterClient();
            }
        } catch (ClusterRetrieveException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return null;
    }


    private ApplicationId getAppIdFromYarn(YarnClient yarnClient) throws Exception {
        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        ApplicationId applicationId = null;
        int maxMemory = -1;
        int maxCores = -1;
        for (ApplicationReport report : reportList) {
            if (!report.getName().startsWith("Flink session")) {
                continue;
            }

            if (!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }

            if (!report.getQueue().equals("root.users.root")) {
                continue;
            }

            int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
            int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();

            boolean isOverMaxResource = thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores;
            if (isOverMaxResource) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }
        }

        return applicationId;
    }
}
