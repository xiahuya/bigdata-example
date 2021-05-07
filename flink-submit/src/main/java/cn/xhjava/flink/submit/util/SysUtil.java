package cn.xhjava.flink.submit.util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2021/5/7
 */
public class SysUtil {
    public static List<URL> findJarsInDir(File dir)  throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();

        if(dir.exists() && dir.isDirectory()) {
            File[] jarFiles = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar");
                }
            });

            for(File jarFile : jarFiles) {
                urlList.add(jarFile.toURI().toURL());
            }

        }

        return urlList;
    }
}
