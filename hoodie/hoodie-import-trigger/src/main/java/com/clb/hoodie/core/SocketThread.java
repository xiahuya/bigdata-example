package com.clb.hoodie.core;

import com.clb.hoodie.domain.HoodieImportParameter;
import com.clb.hoodie.log.LogFileName;
import com.clb.hoodie.log.LoggerUtil;
import com.clb.hoodie.util.HoodieImportUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/7/9
 */
public class SocketThread implements Runnable {
    /*private static final Logger log = LoggerFactory.getLogger(SocketThread.class);*/
    private static final Logger log = LoggerUtil.logger(LogFileName.SparkConsumer);
    private static LinkedList<HoodieImportParameter> list = null;
    private Socket socket = null;
    private static Properties properties;
    private String client;
    private String port;
    private String lastCommand;
    private String currentCommand;
    private static HashMap<String, HoodieImportParameter> runningJobMap = null;
    private HoodieImportParameter importParameter;
    //是否是第一次进来
    private boolean isFirst = true;
    private String lastClientResult = null;
    private String clientResult = null;
    private long id = -1l;

    public SocketThread(LinkedList<HoodieImportParameter> list, Socket socket, Properties properties, HashMap<String, HoodieImportParameter> runningJobMap) {
        this.list = list;
        this.socket = socket;
        this.properties = properties;
        this.runningJobMap = runningJobMap;
        client = socket.getInetAddress().toString();
        port = String.valueOf(socket.getPort());
        log.info(String.format("Client [%s:%s] Connection Succecc!", client, port));
        id = Thread.currentThread().getId();
    }

    @Override
    public void run() {
        try {
            getHoodieImportJob();
        } catch (SocketException socket) {
            log.info(String.format("Client [%s:%s] 断开连接!", client, port));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getHoodieImportJob() throws IOException {
        InputStream is = null;
        InputStreamReader isr = null;
        BufferedInputStream bis = null;
        BufferedReader br = null;
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(socket.getOutputStream());
        try {
            //与客户端建立通信，获取输入流，读取取客户端提供的信息
            is = socket.getInputStream();
            isr = new InputStreamReader(is, "UTF-8");
            br = new BufferedReader(isr);

            while (true) {
                if (list.size() > 0) {
                    //是否是第一次与服务端连接
                    if (isFirst) {
                        //客户端刚刚启动时才进来
                        while ((clientResult = br.readLine()) != null) {
                            if (clientResult.contains("information.") || clientResult.contains("scala> ")) { //客户端启动时打印的参数
                                firstIntoExecutor(outputStreamWriter, socket);
                                break;
                            }
                        }
                    } else {
                        //  假如此次循环是第二次与客户端通信;
                        //  假设list集合内一直存在数据,那么会一直在此逻辑中循环;
                        //  假如list 集合中的元素为0,会跳出该逻辑,并执行下逻辑;
                        if (StringUtils.isEmpty(lastClientResult)) {
                            while ((clientResult = br.readLine()) != null) {
                                if (clientResult.contains("String = true") || clientResult.contains("String = false")) {
                                    //根据返回值,打印日志,上次任务是否执行成功
                                    log.info(HoodieImportUtil.printExecutorResult(clientResult, lastCommand));


                                    //不管成功与否,移除上一次提交的hudi import表
                                    synchronized (list.getClass()) {
                                        if (runningJobMap.size() > 0) {
                                            synchronized (list.getClass()) {

                                                //根据返回值判断,如果runningJobMap集合中有此表,存在返回值表示结束上一流程,开始新的流程
                                                if (runningJobMap.containsKey(mergeStr(importParameter.getDatabse(), importParameter.getTable()))) {
                                                    //移除此表
                                                    runningJobMap.remove(mergeStr(importParameter.getDatabse(), importParameter.getTable()));
                                                    log.info(String.format("表[%s ] 打开", mergeStr(importParameter.getDatabse(), importParameter.getTable())));
                                                }
                                            }
                                        }
                                    }


                                    boolean isBreak = getCommandAndRequetClient(clientResult, outputStreamWriter);
                                    if (isBreak) {
                                        break;
                                    }
                                }

                            }
                        } else {
                            if (lastClientResult.contains("String = true") || lastClientResult.contains("String = false")) {
                                log.info(HoodieImportUtil.printExecutorResult(lastClientResult, lastCommand));


                                //不管成功与否,移除上一次提交的hudi import表
                                synchronized (list.getClass()) {
                                    if (runningJobMap.size() > 0) {
                                        synchronized (list.getClass()) {
                                            //根据返回值判断,如果runningJobMap集合中有此表,存在返回值表示结束上一流程,开始新的流程
                                            if (runningJobMap.containsKey(mergeStr(importParameter.getDatabse(), importParameter.getTable()))) {
                                                //移除此表
                                                runningJobMap.remove(mergeStr(importParameter.getDatabse(), importParameter.getTable()));
                                                log.info(String.format("表[%s ] 打开", mergeStr(importParameter.getDatabse(), importParameter.getTable())));
                                            }
                                        }
                                    }
                                }

                                getCommandAndRequetClient2(lastClientResult, outputStreamWriter);
                            }
                        }
                    }
                } else {
                    log.info("list size is " + list.size());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SocketException socket) {
            log.info(String.format("Client [%s:%s] 断开连接!", client, port));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //关闭资源即相关socket
            try {
                //关闭输入流
                socket.shutdownInput();
                if (outputStreamWriter != null) {
                    outputStreamWriter.close();
                }
                if (br != null)
                    br.close();
                if (isr != null)
                    isr.close();
                if (is != null)
                    is.close();
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    /**
     * Clinet 与 server 第一次连接响应时执行
     * 1. 从list 集合中获取对象
     * 2. 判断该表是否正在进行 import,如果是,等待;如果不是,直接执行;
     *
     * @param outputStreamWriter
     */
    public void firstIntoExecutor(OutputStreamWriter outputStreamWriter, Socket socket) throws IOException {
        String getObjectLog = "%s:%s =>正在从内存List 集合中获取[ HoodieImportParameter ] 对象";
        String ObjectLog = "%s:%s => 成功从内存List 集合中获取[ HoodieImportParameter ] 对象";
        String checkObjectLog = "%s:%s => 开始检查表[ %s ]是否正在进行hudi import ";
        String checkObjectLogLate = "%s:%s => 表[ %s ] 检查完毕,准备进行hudi import ";
        String tableRunning = "%s:%s => 表[ %s ] 正在进行hudi import,稍等......... ";


        //从static list中获取数据
        log.info(String.format(getObjectLog, socket.getInetAddress(), socket.getPort()));
        synchronized (list.getClass()) {
            if (list.size() > 0) {
                importParameter = list.removeFirst();
            } else {
                importParameter = null;
            }
        }
        log.info(String.format(ObjectLog, socket.getInetAddress(), socket.getPort()));

        //查看runningJobMap 中是否存在该对象
        log.info(String.format(checkObjectLog, socket.getInetAddress(), socket.getPort(), mergeStr(importParameter.getDatabse(), importParameter.getTable())));
        if (null != importParameter) {
            while (runningJobMap.containsKey(mergeStr(importParameter.getDatabse(), importParameter.getTable()))) {
                //自旋
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info(String.format(tableRunning, socket.getInetAddress(), socket.getPort(), mergeStr(importParameter.getDatabse(), importParameter.getTable())));
            }
            log.info(String.format(checkObjectLogLate, socket.getInetAddress(), socket.getPort(), mergeStr(importParameter.getDatabse(), importParameter.getTable())));

            currentCommand = HoodieImportUtil.buildSparkShellCommand(importParameter, properties.getProperty("hoodie.import.by.shell.config.path"));
            synchronized (list.getClass()) {
                //锁表
                runningJobMap.put(mergeStr(importParameter.getDatabse(), importParameter.getTable()), importParameter);
                log.info(String.format("表[ %s ] 锁住", mergeStr(importParameter.getDatabse(), importParameter.getTable())));

                outputStreamWriter.write(currentCommand);
                log.info(currentCommand);
                outputStreamWriter.flush();
            }
            lastCommand = currentCommand;
        } else {
            log.info("未开始任务数: " + list.size());
        }
        isFirst = false;

    }


    /**
     * @param clientResult
     * @param outputStreamWriter
     * @return
     * @throws IOException
     */
    public boolean getCommandAndRequetClient(String clientResult, OutputStreamWriter outputStreamWriter) throws
            IOException {
        boolean isBreak = false;
        log.info(String.format("第二次  Get [ HoodieImportParameter ]"));
        synchronized (list.getClass()) {
            if (list.size() > 0) {
                importParameter = list.removeFirst();
            } else {
                importParameter = null;
            }
        }
        log.info(String.format("第二次   Get [ HoodieImportParameter ] Success"));
        //执行方法前查看runningJobMap中是否存在正在运行的表
        if (null != importParameter) {
            while (runningJobMap.containsKey(mergeStr(importParameter.getDatabse(), importParameter.getTable()))) {
                //自旋
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info(String.format("Table[%s.%s] is Running HudiImport,wait....getCommandAndRequetClient", importParameter.getDatabse(), importParameter.getTable()));
            }

            currentCommand = HoodieImportUtil.buildSparkShellCommand(importParameter, properties.getProperty("hoodie.import.by.shell.config.path"));
            synchronized (list.getClass()) {
                runningJobMap.put(mergeStr(importParameter.getDatabse(), importParameter.getTable()), importParameter);
                log.info(String.format("表[%s ] 锁住", mergeStr(importParameter.getDatabse(), importParameter.getTable())));

                outputStreamWriter.write(currentCommand);
                log.info(currentCommand);
                //在执行 hudiImport时,将此表标志正在运行,在此流程结束前,不允许此表再次执行hudiImport
                outputStreamWriter.flush();
                lastCommand = currentCommand;
            }
        } else {
            log.info("未开始任务数: " + list.size());
            //因为list 集合内已经没有数据了,将之前的返回值赋值,给与下一个逻辑处理
            lastClientResult = clientResult;
            isBreak = true;
        }
        return isBreak;
    }

    /**
     * list 集合内的元素被消费的没有后,突然又来了一批数据,走该方法
     *
     * @param clientResult
     * @param outputStreamWriter
     * @throws IOException
     */
    public void getCommandAndRequetClient2(String clientResult, OutputStreamWriter outputStreamWriter) throws
            IOException {
        log.info(String.format("第三次 to Get [ HoodieImportParameter ]"));
        synchronized (list.getClass()) {
            if (list.size() > 0) {
                importParameter = list.removeFirst();
            } else {
                importParameter = null;
            }
        }
        log.info(String.format("第三次 [ HoodieImportParameter ] Success"));
        //执行方法前查看runningJobMap中是否存在正在运行的表
        if (null != importParameter) {
            while (runningJobMap.containsKey(mergeStr(importParameter.getDatabse(), importParameter.getTable()))) {
                //自旋
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info(String.format("Table[%s.%s] is Running HudiImport,wait....getCommandAndRequetClient2", importParameter.getDatabse(), importParameter.getTable()));
            }

            currentCommand = HoodieImportUtil.buildSparkShellCommand(importParameter, properties.getProperty("hoodie.import.by.shell.config.path"));
            synchronized (list.getClass()) {
                runningJobMap.put(mergeStr(importParameter.getDatabse(), importParameter.getTable()), importParameter);
                log.info(String.format("表[%s ] 锁住", mergeStr(importParameter.getDatabse(), importParameter.getTable())));

                outputStreamWriter.write(currentCommand);
                //在执行 hudiImport时,将此表标志正在运行,在此流程结束前,不允许此表再次执行hudiImport

                log.info(currentCommand);
                outputStreamWriter.flush();
                lastCommand = currentCommand;
            }
            lastClientResult = null;
        } else {
            log.info("未开始任务数: " + list.size());
            lastClientResult = null;
        }
    }

    private String mergeStr(String db, String table) {
        return db + "." + table;
    }
}
