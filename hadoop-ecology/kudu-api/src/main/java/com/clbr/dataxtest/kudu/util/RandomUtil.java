package com.clbr.dataxtest.kudu.util;


import java.util.Random;

/**
 * @author XIAHU
 * @create 2019/9/9
 */

public class RandomUtil {

    public static Integer getRandomNum() {
        int max = 40000000;
        int min = 1;
        Random random = new Random();
        int s = random.nextInt(max) % (max - min + 1) + min;

        return  s;
    }

    public static void main(String[] args) {
        for (int i =0 ; i < 1000;i++){
            System.out.println(RandomUtil.getRandomNum());
        }
    }

}