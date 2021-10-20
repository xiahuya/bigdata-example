package cn.xhjava;

import com.alibaba.fastjson.JSON;
import com.clb.domain.NuwaConsumerResponse;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Xiahu
 * @create 2020/8/11
 */
public class JsonProduce {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private Integer house = 19;
    private Integer startMin = 35;
    private Integer endMin = null;
    private Integer tableNum = 1;

    private static String sourceDataPath = "hdfs://master:8020/tmp/flink/nuwa/2020-08-10/%s-%s_%s-%s/hid0101_cache_xdcs_pacs_hj/merge_test%s";
    private static String writeBatchTime = "2020-08-10/%s-%s_%s-%s";

    public String produceMsg() {
        while (true) {
            if (startMin == 59) {
                endMin = 00;
                house = house + 1;
                break;
            } else {
                endMin = startMin + 1;
            }
            for (int i = 1; i < 17; i++) {
                NuwaConsumerResponse nuwa = new NuwaConsumerResponse();
                nuwa.setWriteSuccessTime(sdf.format(new Date()));
                String sourceDataPathStr = String.format(sourceDataPath, house, startMin, house, endMin, i);
                nuwa.setSourceDataPath(sourceDataPathStr);
                nuwa.setHudiTable("hid0101_cache_xdcs_pacs_hj.merge_test" + i);
                nuwa.setHudiPartitionColumnName("");
                nuwa.setNamespace("cache.hid0101_xdcs_pacs.hj.merge_test" + i + ".1.0.0");
                nuwa.setHudiTablePath("/tmp/flink/hj/merge_test" + i);
                String writeBatchTimeStr = String.format(writeBatchTime, house, startMin, house, endMin);
                nuwa.setWriteBatchTime(writeBatchTimeStr);
                String s = JSON.toJSONString(nuwa);
                System.out.println(s);
            }

            startMin = startMin + 1;
            if (house == 20 && startMin == 47) {
                break;
            }
        }

        return null;
    }

    public static void main(String[] args) {
        new JsonProduce().produceMsg();
    }
}
