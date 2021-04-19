package com.clb.hoodie.socket;

import com.clb.hoodie.core.SocketThread;
import com.clb.hoodie.domain.HoodieImportParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2020/7/9
 * java -cp hoodieTrigger-jar-with-dependencies.jar com.clb.hoodie.socket.SparkShellServer
 */
public class SparkShellServer {
    private static final Logger log = LoggerFactory.getLogger(SparkShellServer.class);
    //private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static LinkedList<HoodieImportParameter> list = null;
    private Properties properties;
    private ExecutorService pool = null;
    private HashMap<String, HoodieImportParameter> runningJobMap = null;

    public SparkShellServer(LinkedList<HoodieImportParameter> list, Properties properties) {
        this.runningJobMap = new HashMap<>();
        this.list = list;
        this.properties = properties;
        pool = Executors.newCachedThreadPool();
    }


    public void executSparkShellCommand() throws IOException {
        // A:创建接收端的Socket对象
        ServerSocket ss = new ServerSocket(8081);
        // B:监听客户端连接.返回一个对应的Socket对象
        log.info("Spark Shell Server Start,Port:[8081]");
        while (true) {
            Socket socker = ss.accept();
            pool.execute(new SocketThread(list, socker, properties,runningJobMap));
        }
//        pool.shutdown();
    }


    public static void main(String[] args) throws Exception {
        /*SparkShellServer sparkShellServer = new SparkShellServer();
        sparkShellServer.executSparkShellCommand();
        Thread.sleep(10000);
        String importStr = "import hudi.HudiImportBySparkShell \n";
        String command = "HudiImportBySparkShell.importDataToHudi(spark,\"/home/xiahu/hoodieImport.properties\", \"nuwa_hoodie_import_test.nuwa_hudi_partition\", \"/nuwa/hudi_test/nuwa_hudi_import_test_table_1\", \"/user/hive/warehouse/nuwa_hoodie_import_test.db/nuwa_hudi_import_test_table_1/*\", \"fk_id\") \n";
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(s.getOutputStream());

        // C:获取输入流，读取数据显示在控制台
        while (System.currentTimeMillis() > 1) {
            log.info("获取......");
            InputStream is = s.getInputStream();
            byte[] bys = new byte[1024];
            int len = is.read(bys);
            String str = new String(bys, 0, len);
            log.info("Server: \n" + str.trim());

            if (str.contains("scala> ")) {
                outputStreamWriter.write(command);
                outputStreamWriter.flush();
                log.info(command);
            }
            log.info("循环......");
        }*/

       /*outputStreamWriter.write(command);
        outputStreamWriter.flush();
        log.info(command);


        InputStream is = s.getInputStream();
        byte[] bys = new byte[1024];
        int len = is.read(bys);
        String str = new String(bys, 0, len);
        log.info("Server: " + str.trim());*/
        // 释放资源
        log.info("Spark Shell Server stop");

    }
}
