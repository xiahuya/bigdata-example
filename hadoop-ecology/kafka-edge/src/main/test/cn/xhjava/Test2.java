package cn.xhjava;

/**
 * @author Xiahu
 * @create 2021/4/26
 */
public class Test2 {
    public static void main(String[] args) {
        System.out.println(SuperClass.HELLO_BINGO);
    }

}

class SuperClass {
    static {
        System.out.println("SuperClass init!");
    }

    public static int HELLO_BINGO  = 123;
}