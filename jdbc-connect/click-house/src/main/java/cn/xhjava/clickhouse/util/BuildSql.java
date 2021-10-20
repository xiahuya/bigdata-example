package cn.xhjava.clickhouse.util;

import java.util.*;

/**
 * @author Xiahu
 * @create 2021-08-01
 */
public class BuildSql {
    private static String SQL_TEMP = "select %s,sum(price) as price from ab_ip_feelist where chargedate >= '%s' and chargedate <= '%s' group by %s limit 20";
    private static String[] dateArray = {"07-02", "08-02", "09-02", "10-02", "11-02", "12-02"};
    private static String[] groupByArray = {"doctorname", "deptname", "itemtypename"};

    /*private static List<String> indexs = Arrays.asList("ab_ip_feelist_2012",
            "ab_ip_feelist_2013",
            "ab_ip_feelist_2014",
            "ab_ip_feelist_2015",
            "ab_ip_feelist_2016",
            "ab_ip_feelist_2017",
            "ab_ip_feelist_2018",
            "ab_ip_feelist_2019",
            "ab_ip_feelist_2020",
            "ab_ip_feelist_2021");*/

    private static List<String> indexs = Arrays.asList("vt_zyfy_day");


    private static Map<Integer, String> indexsMap;

    static {
        indexsMap = new HashMap<>();
        indexsMap.put(2012, "ab_ip_feelist_2013");
        indexsMap.put(2013, "ab_ip_feelist_2014");
        indexsMap.put(2014, "ab_ip_feelist_2015");
        indexsMap.put(2015, "ab_ip_feelist_2016");
        indexsMap.put(2016, "ab_ip_feelist_2017");
        indexsMap.put(2017, "ab_ip_feelist_2018");
        indexsMap.put(2018, "ab_ip_feelist_2019");
        indexsMap.put(2019, "ab_ip_feelist_2020");
        indexsMap.put(2020, "ab_ip_feelist_2021");
        indexsMap.put(2021, "ab_ip_feelist_2021");
    }


    // 普通sql
    private static final String SQL_TEMP_1 = "select totalmoney as totalmoney,deptcode as deptcode,deptname as deptname from ab_ip_feelist_all limit 20";
    // 普通sql + 条件
    private static final String SQL_TEMP_2 = "select totalmoney as totalmoney,deptcode as deptcode,deptname as deptname from ab_ip_feelist_all where confirmdatetime >= '%s' and confirmdatetime <= '%s' limit 20";
    // 普通sql + 聚合 + 条件
    private static final String SQL_TEMP_3 = "select sum(totalmoney) as sum_totalmoney from ab_ip_feelist_all where confirmdatetime >= '%s' and confirmdatetime <= '%s' limit 20";
    // 普通聚合+ 分组 + 条件
    private static final String SQL_TEMP_4 = "select %s,sum(totalmoney) as totalmoney from ab_ip_feelist_all where confirmdatetime >= '%s' and confirmdatetime <= '%s' group by %s limit 20";

    private static final int START_YEAR = 2011;
    private static final int END_YAER = 2021;


    public String getSql(int version) {
        String sql = new String();
        switch (version) {
            case 1:
                // 普通sql
                sql = String.format(SQL_TEMP_1);
                break;
            case 2:
                // 普通sql + 条件
                sql = randomAggDateCondition();
                break;
            case 3:
                // 普通sql  + 聚合 + 条件
                sql = randomDateCondition();
                break;
            case 4:
                sql = randomAggGroupDateCondition();
                break;
            default:
                throw new RuntimeException();
        }

        return sql;
    }

    public String randomAggDateCondition() {
        Random random = new Random();
        int startYear = random.nextInt(END_YAER - START_YEAR) + START_YEAR;
        int endYear = startYear + 1;
        String date = dateArray[random.nextInt(dateArray.length)];

        String startTime = String.valueOf(startYear) + "-" + date;
        String endTime = String.valueOf(endYear) + "-" + date;
        return String.format(SQL_TEMP_2, startTime, endTime);
    }

    public String randomDateCondition() {
        Random random = new Random();
        int startYear = random.nextInt(END_YAER - START_YEAR) + START_YEAR;
        int endYear = startYear + 1;
        String date = dateArray[random.nextInt(dateArray.length)];

        String startTime = String.valueOf(startYear) + "-" + date;
        String endTime = String.valueOf(endYear) + "-" + date;
        return String.format(SQL_TEMP_3, startTime, endTime);
    }


    public String randomAggGroupDateCondition() {
        Random random = new Random();
        int startYear = random.nextInt(END_YAER - START_YEAR) + START_YEAR;
        int endYear = startYear + 1;
        String date = dateArray[random.nextInt(dateArray.length)];
        String startTime = String.valueOf(startYear) + "-" + date;
        String endTime = String.valueOf(endYear) + "-" + date;

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


        return String.format(SQL_TEMP_4, groupByStr.toString(), startTime, endTime, groupByStr);
    }


    public static void main(String[] args) {
        BuildSql buildSql = new BuildSql();
        for (int i = 0; i < 100; i++) {
            System.out.println(buildSql.getSql((i % 4) + 1));
        }
    }
}
