package com.clb.hoodie.util;

import com.clb.hoodie.domain.HoodieImportParameter;

/**
 * @author Xiahu
 * @create 2020/7/8
 */
public class HoodieImportUtil {

    public static HoodieImportParameter parseZkData(String data) {
        HoodieImportParameter parameter = new HoodieImportParameter();
        String[] split = data.split(",");
        parameter.setDatabse(split[0].split("\\.")[0]);
        //todo 用于测试
//        parameter.setTable("test_table_1");
        parameter.setTable(split[0].split("\\.")[1]);
        parameter.setInPutPath(split[1]);
        parameter.setOutPutPath(split[2]);
        parameter.setIsPartitionTable(split[3]);
        if (split[3].equals("true")) {
            parameter.setPartitionColumn(split[4]);
        } else {
            parameter.setPartitionColumn("partition_false");
        }
        return parameter;
    }

    public static String buildSparkShellCommand(HoodieImportParameter hoodieImportParameter, String configPath) {
        String sparkShellCommand = "HudiImportBySparkShell.importDataToHudi(spark,\"%s\", \"%s.%s\", \"%s\", \"%s/*\", \"%s\") \n";
        String command = String.format(sparkShellCommand,
                configPath,
                hoodieImportParameter.getDatabse(),
                hoodieImportParameter.getTable(),
                hoodieImportParameter.getOutPutPath(),
                hoodieImportParameter.getInPutPath(),
                hoodieImportParameter.getPartitionColumn());
        return command;

    }

    /**
     * 根据返回值,判断上次hudi import 是否执行成功
     * @param result
     * @param lastCommand
     * @return
     */
    public static String printExecutorResult(String result, String lastCommand) {
        String log = null;
        if (result.contains("String = true")) {
            log = String.format("执行成功 => [%s]", lastCommand);
        } else {
            log = String.format("执行失败 => [%s]", lastCommand);
        }
        return log;
    }

}
