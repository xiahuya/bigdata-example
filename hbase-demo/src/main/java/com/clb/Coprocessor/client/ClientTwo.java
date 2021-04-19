/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: ClientTwo
 * Author:   17650
 * Date:     2019/6/25 14:58
 * Description: Table.coprocessorService(Class, byte[], byte[],Batch.Call) 方式客户端代码实现
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */

package com.clb.Coprocessor.client;


import com.clb.Coprocessor.MyStatisticsInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * 〈一句话功能简述〉<br> 
 * 〈Table.coprocessorService(Class, byte[], byte[],Batch.Call) 方式客户端代码实现〉
 *
 * @author 17650
 * @create 2019/6/25
 * @since 1.0.0
 */

public class ClientTwo {

    /**
     * 通过Table.coprocessorService(Class, byte[], byte[],Batch.Call) 请求多region服务
     *
     * 在这个API参数列表中，
     　　　*　Class代表所需要请求的服务，当前是我们定义在proto中的myStatisticsService服务。
     　　　*　byte[], byte[]参数指明了startRowKey和endRowKey，当都为null的时候即进行全表的全region的数据计算。
     　　　*　Batch.Call：需要自定义，API会根据如上参数信息并行的连接各个region，来执行这个参数中定义的call方法来执行接口方法的调用查询　
     * 此API会将各个region上的信息放入Map<regionname,result>的结构中，搜集所有的结果后返回给调用者，调用者在循环进行归并计算。
     *
     * @param config
     * @param tableName
     * @param startRowkey
     * @param endRowkey
     * @param type
     * @param famillyName
     * @param columnName
     * @return
     * @throws Throwable
     */
    public static long multipleRegionsStatistics(Configuration config, String tableName, String startRowkey, String endRowkey, final String type, final String famillyName, final String columnName)throws Throwable {
        long result = 0;
        Table table = null;
        Connection connection = null;

        // 返回值接收，Map<region名称,计算结果>
        Map<byte[], MyStatisticsInterface.getStatisticsResponse> results = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));

            // 第四个参数是接口类 Batch.Call。它定义了如何调用协处理器，用户通过重载该接口的 call() 方法来实现客户端的逻辑。在 call() 方法内，可以调用 RPC，并对返回值进行任意处理。
            Batch.Call<MyStatisticsInterface.myStatisticsService, MyStatisticsInterface.getStatisticsResponse> callable = new Batch.Call<MyStatisticsInterface.myStatisticsService, MyStatisticsInterface.getStatisticsResponse>() {

                ServerRpcController controller = new ServerRpcController();
                // 定义返回
                BlockingRpcCallback<MyStatisticsInterface.getStatisticsResponse> rpcCallback = new BlockingRpcCallback<MyStatisticsInterface.getStatisticsResponse>();

                // 下面重载 call 方法，API会连接到region后会运行call方法来执行服务的请求
                public MyStatisticsInterface.getStatisticsResponse call(MyStatisticsInterface.myStatisticsService instance) throws IOException {
                    // Server 端会进行慢速的遍历 region 的方法进行统计
                    MyStatisticsInterface.getStatisticsRequest.Builder request = MyStatisticsInterface.getStatisticsRequest
                            .newBuilder();
                    request.setType(type);
                    if (null != famillyName) {
                        request.setFamillyName(famillyName);
                    }

                    if (null != columnName) {
                        request.setColumnName(columnName);
                    }
                    // RPC 接口方法调用
                    instance.getStatisticsResult(controller, request.build(),rpcCallback);
                    // 直接返回结果，即该 Region 的 计算结果
                    return rpcCallback.get();
                }

            };

            /**
             * 通过Table.coprocessorService(Class, byte[], byte[],Batch.Call) 请求多region服务
             *
             * 在这个API参数列表中，
             　　　*　Class代表所需要请求的服务，当前是我们定义在proto中的myStatisticsService服务。
             　　　*　byte[], byte[]参数指明了startRowKey和endRowKey，当都为null的时候即进行全表的全region的数据计算。
             　　　*　Batch.Call：需要自定义，API会根据如上参数信息并行的连接各个region，来执行这个参数中定义的call方法来执行接口方法的调用查询　
             * 此API会将各个region上的信息放入Map<regionname,result>的结构中，搜集所有的结果后返回给调用者，调用者在循环进行归并计算。
             */
            results = table.coprocessorService(MyStatisticsInterface.myStatisticsService.class,
                    Bytes.toBytes(startRowkey), Bytes.toBytes(endRowkey),
                    callable);

            // 取得结果值后循环将结果合并，即得到最终的结果
            Collection<MyStatisticsInterface.getStatisticsResponse> resultsc = results.values();
            for (MyStatisticsInterface.getStatisticsResponse r : resultsc) {
                result += r.getResult();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != table) {
                table.close();
            }

            if (null != connection) {
                connection.close();
            }
        }
        return result;
    }
}