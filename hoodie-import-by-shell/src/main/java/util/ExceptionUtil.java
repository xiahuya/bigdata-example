package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author Xiahu
 * @create 2020/5/15
 */
public class ExceptionUtil {
    private static final Logger log = LoggerFactory.getLogger(ExceptionUtil.class);

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
                    log.error(e.getMessage());
                }
            }
            if (pw != null) {
                try {
                    pw.close();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
        return info;
    }
}
