package cn.xhjava.http.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * @Author: hj
 * @Date: 2021/7/16 下午4:27
 * @Desc:
 */
public class BuildSql {
    private static String SQL_TEMP = "select %s,sum(price) as price from ab_ip_feelist where chargedate >= '%s' and chargedate <= '%s' group by %s limit 20";
//    private static String SQL_TEMP = "select price as price from ab_ip_feelist limit 20";
    private static String[] dateArray = {"01-01", "02-01", "03-01", "04-01", "05-01", "06-01", "07-01", "08-01", "09-01", "10-01", "11-01", "12-01"};
    private static String[] groupByArray = {"doctorname", "deptname", "itemtypename"};


    public String buildRandomSql(int start, int end) {
        Random random = new Random();
        int startYear = random.nextInt(end - start) + start;
        int endYear = startYear + 1;

        String startTime = String.valueOf(startYear) + "-" + dateArray[random.nextInt(12)];
        String endTime = String.valueOf(endYear) + "-" + dateArray[random.nextInt(12)];

        Map<Integer, String> groupByItem = new LinkedHashMap<Integer, String>();
        int itemsize = random.nextInt(3) + 1;
        while (groupByItem.size() < itemsize) {
            int index = random.nextInt(3);
            groupByItem.put(index, groupByArray[index]);
        }

        StringBuffer groupByStr = new StringBuffer();

        int i = 0;
        for (Map.Entry<Integer, String> kv : groupByItem.entrySet()) {
            groupByStr.append(kv.getValue());
            if (i != groupByItem.size() - 1) {
                groupByStr.append(",");
            }
            i++;
        }

        return String.format(SQL_TEMP, groupByStr.toString(), startTime, endTime, groupByStr);
    }

    private static String SQL_TEMP2 = "select sum(price) as price from ab_ip_feelist where chargedate >= '%s' and chargedate <= '%s' limit 20";
    public String buildRandomSqlNoGroupBy(int start, int end) {
        Random random = new Random();
        int startYear = random.nextInt(end - start) + start;
        int endYear = startYear + 1;

        String startTime = String.valueOf(startYear) + "-" + dateArray[random.nextInt(12)];
        String endTime = String.valueOf(endYear) + "-" + dateArray[random.nextInt(12)];

        Map<Integer, String> groupByItem = new LinkedHashMap<Integer, String>();
        int itemsize = random.nextInt(3) + 1;
        while (groupByItem.size() < itemsize) {
            int index = random.nextInt(3);
            groupByItem.put(index, groupByArray[index]);
        }

        StringBuffer groupByStr = new StringBuffer();

        int i = 0;
        for (Map.Entry<Integer, String> kv : groupByItem.entrySet()) {
            groupByStr.append(kv.getValue());
            if (i != groupByItem.size() - 1) {
                groupByStr.append(",");
            }
            i++;
        }

        return String.format(SQL_TEMP2, startTime, endTime);
    }



    public static void main(String[] args) {
        BuildSql buildSql = new BuildSql();
        for (int i = 0; i < 100; i++) {
            System.out.println(buildSql.buildRandomSqlNoGroupBy(2013, 2021));
        }
    }


}
