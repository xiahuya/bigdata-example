package cn.xhjava;

import java.io.File;

/**
 * @author Xiahu
 * @create 2021-06-23
 */
public class FileRename {
    private static String dirPath = "E:\\Study\\尚硅谷Java数据结构和算法\\视频";
    private static String replaceStr = "尚硅谷-图解Java数据结构和算法-";

    public static void main(String[] args) {
        File dir = new File(dirPath);
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                String fileName = file.getName();
                if (fileName.contains(replaceStr)) {
                    String newFileName = fileName.replace(replaceStr, "");
                    file.renameTo(new File(dirPath + "\\" + newFileName));
                }
            }
        }
    }
}
