package cn.xhjava;

import java.io.File;

/**
 * @author Xiahu
 * @create 2021-06-23
 */
public class FileCopy {
    private static String dirPath = "E:\\Study\\中华石杉 Elasticsearch顶尖高手系列课程";

    public static void main(String[] args) {
        copy(new File(dirPath));
    }

    public static void copy(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File dir : files) {
                copy(dir);
            }
        } else {
            String name = file.getAbsolutePath();
            if(name.endsWith(".avi")){
                String newFileName = name.replace("视频\\", "");
                file.renameTo(new File(newFileName));
            }
        }
    }
}
