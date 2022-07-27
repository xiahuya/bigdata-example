package cn.xhjava.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author Xiahu
 * @create 2020/11/19
 * 单进单出udf
 * 需求:
 *      key 为 'st',输出服务器时间
 *      key 为 'et',输出字符串
 *      key 为 cm 下的字段,输出对应value
 */
public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String key) throws JSONException {

        String[] log = line.split("\\|");

        if (log.length != 2 || StringUtils.isBlank(log[1])) {
            return "";
        }
        JSONObject baseJson = new JSONObject(log[1].trim());

        String result = "";

        // 获取服务器时间
        if ("st".equals(key)) {
            result = log[0].trim();
        } else if ("et".equals(key)) {
            // 获取事件数组
            if (baseJson.has("et")) {
                result = baseJson.getString("et");
            }
        } else {
            JSONObject cm = baseJson.getJSONObject("cm");
            // 获取key对应公共字段的value
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }

        return result;
    }

    public static void main(String[] args) {
        String line = "1583776223469|\n" +
                "{\n" +
                "\t\"cm\": {\n" +
                "\t\t\"ln\": \"-48.5\",\n" +
                "\t\t\"sv\": \"V2.5.7\",\n" +
                "\t\t\"os\": \"8.0.9\",\n" +
                "\t\t\"g\": \"6F76AVD5@gmail.com\",\n" +
                "\t\t\"mid\": \"0\",\n" +
                "\t\t\"nw\": \"4G\",\n" +
                "\t\t\"l\": \"pt\",\n" +
                "\t\t\"vc\": \"3\",\n" +
                "\t\t\"hw\": \"750*1134\",\n" +
                "\t\t\"ar\": \"MX\",\n" +
                "\t\t\"uid\": \"0\",\n" +
                "\t\t\"t\": \"1583707297317\",\n" +
                "\t\t\"la\": \"-52.9\",\n" +
                "\t\t\"md\": \"sumsung-18\",\n" +
                "\t\t\"vn\": \"1.2.4\",\n" +
                "\t\t\"ba\": \"Sumsung\",\n" +
                "\t\t\"sr\": \"V\"\n" +
                "\t},\n" +
                "\t\"ap\": \"app\",\n" +
                "\t\"et\": [\n" +
                "\t\t{\n" +
                "\t\t\t\"ett\": \"1583705574227\",\n" +
                "\t\t\t\"en\": \"display\",\n" +
                "\t\t\t\"kv\": {\n" +
                "\t\t\t\t\"goodsid\": \"0\",\n" +
                "\t\t\t\t\"action\": \"1\",\n" +
                "\t\t\t\t\"extend1\": \"1\",\n" +
                "\t\t\t\t\"place\": \"0\",\n" +
                "\t\t\t\t\"category\": \"63\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"ett\": \"1583760986259\",\n" +
                "\t\t\t\"en\": \"loading\",\n" +
                "\t\t\t\"kv\": {\n" +
                "\t\t\t\t\"extend2\": \"\",\n" +
                "\t\t\t\t\"loading_time\": \"4\",\n" +
                "\t\t\t\t\"action\": \"3\",\n" +
                "\t\t\t\t\"extend1\": \"\",\n" +
                "\t\t\t\t\"type\": \"3\",\n" +
                "\t\t\t\t\"type1\": \"\",\n" +
                "\t\t\t\t\"loading_way\": \"1\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"ett\": \"1583746639124\",\n" +
                "\t\t\t\"en\": \"ad\",\n" +
                "\t\t\t\"kv\": {\n" +
                "\t\t\t\t\"activityId\": \"1\",\n" +
                "\t\t\t\t\"displayMills\": \"111839\",\n" +
                "\t\t\t\t\"entry\": \"1\",\n" +
                "\t\t\t\t\"action\": \"5\",\n" +
                "\t\t\t\t\"contentType\": \"0\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"ett\": \"1583758016208\",\n" +
                "\t\t\t\"en\": \"notification\",\n" +
                "\t\t\t\"kv\": {\n" +
                "\t\t\t\t\"ap_time\": \"1583694079866\",\n" +
                "\t\t\t\t\"action\": \"1\",\n" +
                "\t\t\t\t\"type\": \"3\",\n" +
                "\t\t\t\t\"content\": \"\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"ett\": \"1583699890760\",\n" +
                "\t\t\t\"en\": \"favorites\",\n" +
                "\t\t\t\"kv\": {\n" +
                "\t\t\t\t\"course_id\": 4,\n" +
                "\t\t\t\t\"id\": 0,\n" +
                "\t\t\t\t\"add_time\": \"1583730648134\",\n" +
                "\t\t\t\t\"userid\": 7\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t]\n" +
                "}\n";
//        String mid = new BaseFieldUDF().evaluate(line, "et");
//
//        System.out.println(mid);

    }

}
