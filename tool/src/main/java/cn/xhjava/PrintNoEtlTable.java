package cn.xhjava;

import com.sun.org.apache.bcel.internal.generic.NEW;

import java.io.*;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/12/1 0001
 */
public class PrintNoEtlTable {
    private static Map<String, List<String>> allTable = new HashMap<>();
    private static Map<String, List<String>> skipTable = new HashMap<>();


    static {
        BufferedReader br = null;
        try {
            String line = null;
            //获取allrealtime.txt
            br = new BufferedReader(new FileReader(new File("D:\\code\\XIAHU\\bigdata-example\\tool\\allrealtime.txt")));
            while ((line = br.readLine()) != null) {
                String[] split = line.split("\\.");
                String database = split[0].toLowerCase();
                String table = split[1].toLowerCase();
                if (allTable.containsKey(database)) {
                    List<String> tables = allTable.get(database);
                    tables.add(table);
                } else {
                    List<String> list = new ArrayList<>();
                    list.add(table);
                    allTable.put(database, list);
                }
            }


            //获取allrealtime.txt
            br = new BufferedReader(new FileReader(new File("D:\\code\\XIAHU\\bigdata-example\\tool\\src\\skip_table.txt")));
            while ((line = br.readLine()) != null) {
                String[] split = line.split(":");
                String database = split[0].toLowerCase();
                String tableLine = split[1].toLowerCase();
                List<String> tableList = Arrays.asList(tableLine.split(","));
                if (skipTable.containsKey(database)) {
                    List<String> tables = skipTable.get(database);
                    tables.addAll(tableList);
                } else {
                    skipTable.put(database, tableList);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Map<String, List<String>> resultMap = new HashMap<>();
        Iterator<Map.Entry<String, List<String>>> entryIterator = allTable.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, List<String>> entry = entryIterator.next();
            String database = entry.getKey();
            List<String> skipTableList = entry.getValue();
            if (skipTable.containsKey(database)) {
                List<String> allTableList = skipTable.get(database);
                for (String table : skipTableList) {
                    if (!allTableList.contains(table)) {
                        if (resultMap.containsKey(database)) {
                            resultMap.get(database).add(table);
                        } else {
                            List<String> list = new ArrayList<>();
                            list.add(table);
                            resultMap.put(database, list);
                        }
                    }
                }
            } else {
                resultMap.put(database, skipTableList);
            }
        }


        StringBuffer sb = new StringBuffer();

        resultMap.entrySet().stream().forEach(entry -> {
            String database = entry.getKey();
            List<String> tableList = entry.getValue();
            sb.append(database).append(":");
            tableList.forEach(table -> {
                        sb.append(table).append(",");
                    }
            );
            sb.append("\r\n");
        });

        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("D:\\code\\XIAHU\\bigdata-example\\tool\\src\\etl.txt")));
        bw.write(sb.toString());
        bw.flush();
        bw.close();

    }
}
