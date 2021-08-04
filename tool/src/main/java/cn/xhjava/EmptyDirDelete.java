package cn.xhjava;

import java.io.File;

/**
 * @author Xiahu
 * @create 2021-06-23
 */
public class EmptyDirDelete {
    private static String dirPath = "E:\\Study\\中华石杉 Elasticsearch顶尖高手系列课程";

    public static void main(String[] args) {
        delete(new File(dirPath));
    }

    public static void delete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files.length > 0) {
                for (File dir : files) {
                    delete(dir);
                }
            } else {
                file.delete();
                System.out.println("删除 " + file.getAbsolutePath());
            }
        }
    }
}
