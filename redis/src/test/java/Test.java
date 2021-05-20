import java.util.HashMap;

/**
 * @author Xiahu
 * @create 2021/5/12
 */
public class Test {
    public static void main(String[] args) {
        String key = "张三";
        int h;
        int i = (h = key.hashCode()) ^ (h >>> 16);
        System.out.println(i);
        System.out.println(15 & i);
    }
}
