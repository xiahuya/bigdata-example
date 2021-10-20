package com.clb.hoodie.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * @author Xiahu
 * @create 2020/7/9
 */
public class SparkShellClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        int count = 3;
        for (int i = 0; i < count; i++) {
            new Thread(new SocketClient()).start();
//            Thread.sleep(1000);
        }
    }
}

class SocketClient implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SocketClient.class);

    @Override
    public void run() {
        try {
            start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {
        Socket s = new Socket("localhost", 8081);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                s.getOutputStream()));
        bw.write("scala> ");
        bw.newLine();
        bw.flush();

        BufferedInputStream bis = new BufferedInputStream(s.getInputStream());
        byte[] bys = new byte[1024];
        int len = 0;
        while ((len = bis.read(bys)) != -1) {
            String str = new String(bys, 0, len);
            log.info(str);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            bw.write("res0: String = true");
            log.info("res0: String = true");
            bw.newLine();
            bw.flush();
        }


    }
}
