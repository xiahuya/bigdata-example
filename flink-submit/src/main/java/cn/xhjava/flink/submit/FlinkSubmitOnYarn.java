package cn.xhjava.flink.submit;

import cn.xhjava.flink.submit.util.ClusterClientFactory;
import cn.xhjava.flink.submit.util.Options;
import cn.xhjava.flink.submit.util.SysUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2021/5/7
 */
public class FlinkSubmitOnYarn {
    public static final String MAIN_CLASS = "stream.async.join.hbase.MutilStreamJoin_03";
    private static Options launcherOptions = new Options();

    static {
        launcherOptions.setParallelism("1");
        launcherOptions.setS(null);
    }

    public static void main(String[] args) throws Exception {
        ClusterClientFactory clusterClientFactory = new ClusterClientFactory("E:\\conf\\maser");
        ClusterClient clusterClient = clusterClientFactory.createYarnClient(null);
        String webInterfaceURL = clusterClient.getWebInterfaceURL();
        ClientUtils.submitJob(clusterClient, buildJobGraph(launcherOptions, args));
        System.out.println(webInterfaceURL);
    }


    public static JobGraph buildJobGraph(Options launcherOptions, String[] remoteArgs) throws Exception {
        File jarFile = new File("E:\\conf\\jar\\flink-realtime-jar-with-dependencies.jar");
        List<URL> urlList = new ArrayList<>();
        urlList.addAll(SysUtil.findJarsInDir(new File("E:\\conf\\jar")));


        SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
        if (StringUtils.isNotEmpty(launcherOptions.getS())) {
            savepointRestoreSettings = SavepointRestoreSettings.forPath(launcherOptions.getS());
        }
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(jarFile)
                .setUserClassPaths(urlList)
                .setEntryPointClassName(MAIN_CLASS)
                .setConfiguration(ConfigurationLoader.flinkConfiguration("D:\\Tool\\flink-1.12.2\\conf"))
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .setArguments(remoteArgs)
                .build();
        return PackagedProgramUtils.createJobGraph(
                program,
                ConfigurationLoader.flinkConfiguration("D:\\Tool\\flink-1.12.2\\conf"),
                Integer.parseInt(launcherOptions.getParallelism()),
                false);
    }
}
