package service;

/**
 * @author chongwang11
 * @date 2023-03-01 16:46
 * @description
 */
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class ProducerConsumer {
    private Set<Integer> data = new HashSet<>();
    private Random random = new Random();
    private int maxDataSize;

    public ProducerConsumer(int maxDataSize) {
        this.maxDataSize = maxDataSize;
    }

    public synchronized void produce() {
        while (data.size() >= maxDataSize) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int newData;
        do {
            newData = random.nextInt(maxDataSize * 2); // 产生一个不大于 maxDataSize * 2 的随机数
        } while (data.contains(newData)); // 直到产生一个非重复的数据为止
        data.add(newData);
        System.out.println("生产数据：" + newData);
        notifyAll();
    }

    public synchronized void consume() {
        while (data.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int consumedData = data.iterator().next();
        data.remove(consumedData);
        System.out.println("消费数据：" + consumedData);
        notifyAll();
    }

    public static void main(String[] args) {
        ProducerConsumer pc = new ProducerConsumer(10);
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 20; i++) {
                pc.produce();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread consumer = new Thread(() -> {
            for (int i = 0; i < 20; i++) {
                pc.consume();
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        producer.start();
        consumer.start();
    }
}