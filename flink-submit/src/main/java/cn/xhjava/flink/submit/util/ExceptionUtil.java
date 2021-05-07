package cn.xhjava.flink.submit.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @ClassName ExceptionUtil
 * @Author huangjing
 * @Date 19-7-17 下午2:52
 **/
public class ExceptionUtil {

    public static String getStackTrace(Throwable throwable) {
        String info = "";
        StringWriter sw = null;
        PrintWriter pw = null;
        try {
            sw = new StringWriter();
            pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            info = sw.toString();
        } finally {
            if (sw != null) {
                try {
                    sw.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (pw != null) {
                try {
                    pw.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return info;
    }
}
