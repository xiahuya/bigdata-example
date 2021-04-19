package com.clb.Coprocessor.client;


import com.clb.Coprocessor.MyStatisticsInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;


public class ClientOne {
    /**
     * 其原理为通过参数的rowKey值进行region的定位，通过向此region请求服务进行计算，计算的数据范围仅限此region。
     */


    /**
     * 通过CoprocessorRpcChannel coprocessorService(byte[] row); 请求单region服务
     * <p>
     * 客户端通过rowKey的指定，指向rowKey所在的region进行服务请求,所以从数据上来说只有这个region所包含的数据范围
     * 另外由于只向单个region请求服务，所以在客户端也没有必要在做归并操作。
     *
     * @param config
     * @param tableName
     * @param rowkey
     * @param type
     * @param famillyName
     * @param columnName
     * @return
     * @throws IOException
     */
    public static long singleRegionStatistics(Configuration config, String tableName, String rowkey, String type, String famillyName, String columnName) throws IOException {
        long result = 0;
        Table table = null;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));

            // 每一个region都加载了Endpoint协处理器，换句话说每一个region都能提供rpc的service服务，首先确定调用的范围
            // 这里只通过一个rowkey来确定，不管在此表中此rowkey是否存在，只要某个region的范围包含了这个rowkey，则这个region就为客户端提供服务
            CoprocessorRpcChannel channel = table.coprocessorService(rowkey.getBytes());

            // 因为在region上可能会有很多不同rpcservice，所以必须确定你需要哪一个service
            MyStatisticsInterface.myStatisticsService.BlockingInterface service = MyStatisticsInterface.myStatisticsService.newBlockingStub(channel);

            // 构建参数，设置 RPC 入口参数
            MyStatisticsInterface.getStatisticsRequest.Builder request = MyStatisticsInterface.getStatisticsRequest.newBuilder();
            request.setType(type);
            if (null != famillyName) {
                request.setFamillyName(famillyName);
            }

            if (null != columnName) {
                request.setColumnName(columnName);
            }

            // 调用 RPC
            MyStatisticsInterface.getStatisticsResponse ret = service.getStatisticsResult(null, request.build());

            // 解析结果,由于只向一个region请求服务，所以在客户端也就不存在去归并的操作
            result = ret.getResult();
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