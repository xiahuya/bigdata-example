package cn.xhjava;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;


/**
 * @author Xiahu
 * @create 2020/6/5
 */
public class Test {
    public static void main(String[] args) throws IOException {
        System.out.println(InetAddress.getLocalHost().getHostAddress());
        System.out.println(InetAddress.getLocalHost().getHostName());
        System.out.println(InetAddress.getLocalHost().getCanonicalHostName());
        InetAddress ip4 = Inet4Address.getLocalHost();
        byte[] address = ip4.getAddress();
        String s = new String(address, "UTF-8");
        System.out.println(s);

        Process ipaddress = Runtime.getRuntime().exec("ipconfig");
        System.out.println(ipaddress.toString());
    }
}
