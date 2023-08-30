import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class Main {
    public static void main(String[] args) {
        log.info("info");
        log.debug("debug");
        log.error("error");


        File file = new File(".");
        String[] list = file.list();
        for (String s : list) {
            System.out.println(s);
        }
    }

    //将实现方式放入Sum方法中，在主函数中调用
    public static void Sum(int num) {

        int sum = 1;//初始化sum，当序列中数增加到num时，输出满足条件的序列
        int beg = 1;//从1开始
        int cur = 1;//当前数字
        int count = 0;

        while (beg <= num / 2 + 1) {

            if (sum == num) {
                System.out.print(num + "=");

                for (int k = beg; k <= cur; k++) {

                    if (k == cur) {
                        System.out.print(k + ";");
                        count++;
                    } else {
                        System.out.print(k + "+");
                    }
                }
                System.out.println();
                sum = sum - beg;
                beg++;
                cur++;
                sum += cur;
            }
            if (sum > num) {
                sum = sum - beg;
                beg++;
            } else {
                cur++;
                sum += cur;
            }

        }
        System.out.println(count);

    }

}
