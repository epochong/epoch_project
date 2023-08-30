package juc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chongwang11
 * @date 2023-03-05 19:13
 * @description
 */
public class ScheduleMain {
    private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 10; i++) {

            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(Thread.currentThread().getName());
            }, 2, 10, TimeUnit.SECONDS);
        }

    }
}
