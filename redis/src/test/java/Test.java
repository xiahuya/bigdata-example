/**
 * @author Xiahu
 * @create 2021/5/12
 */
public class Test {
    public static void main(String[] args) {
        String TABLE = "xh.testTable_%s";
        for (int i = 1; i <= 45; i++) {
            System.out.println(String.format(TABLE, i));
        }
    }
}
