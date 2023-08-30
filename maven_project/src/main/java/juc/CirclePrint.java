package juc;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chongwang11
 * @date 2022 12 08 09 38
 * @description
 */
public class CirclePrint {
    static class ThreadDemo extends Thread {
        private Semaphore current;
        private Semaphore next;
        private String name;

        public ThreadDemo(Semaphore current, Semaphore next, String name) {
            this.current = current;
            this.next = next;
            this.name = name;
        }

        @Override
        public void run() {
            for (int i = 0; i < 5; i++) {
                try {
                    current.acquire();
                    System.out.print(name);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                next.release();
            }
        }

        public static void main(String[] args) {
            Semaphore a = new Semaphore(1);
            Semaphore b = new Semaphore(0);
            Semaphore c = new Semaphore(0);

            new ThreadDemo(a, b, "we ").start();
            new ThreadDemo(b, c, "need ").start();
            new ThreadDemo(c, a, "you.").start();
        }
    }

    static class LockPrint {
        static ReentrantLock lock = new ReentrantLock();
        static Condition a = lock.newCondition();
        static Condition b = lock.newCondition();
        static Condition c = lock.newCondition();

        public void run(String name) {
            lock.lock();
            for (int i = 0; i < 5; i++) {
                if ("we ".equals(name)) {
                    print(name, a, b);
                }
                if ("need ".equals(name)) {
                    print(name, b, c);
                }
                if ("you.".equals(name)) {
                    print(name, c, a);
                }
            }
            lock.unlock();
        }

        public void print(String name, Condition cur, Condition next) {
            try {
                System.out.print(name);
                next.signal();
                cur.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public static void main(String[] args) {
            LockPrint lockPrint = new LockPrint();
            new Thread(() -> lockPrint.run("we ")).start();
            new Thread(() -> lockPrint.run("need ")).start();
            new Thread(() -> lockPrint.run("you.")).start();
        }
    }

    static class ThreadJoinPrint {
        static class Work implements Runnable {
            private Thread preThread;
            private String name;

            public Work(Thread preThread, String name) {
                this.preThread = preThread;
                this.name = name;
            }

            @Override
            public void run() {
                if (preThread != null) {
                    try {
                        preThread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.print(name);
            }
        }
        public static void main(String[] args) {
            Thread t1 = new Thread(new Work(null, "we "));
            Thread t2 = new Thread(new Work(t1, "need "));
            Thread t3 = new Thread(new Work(t2, "you."));
            t1.start();
            t2.start();
            t3.start();
        }
    }


    /**
     * by chat GPT
     */
    public static class ThreadAlternateDemo {
        public static void main(String[] args) {
            final Object lock = new Object();
            Thread t1 = new Thread(new Worker(lock, "A", 0));
            Thread t2 = new Thread(new Worker(lock, "B", 1));
            Thread t3 = new Thread(new Worker(lock, "C", 2));
            t1.start();
            t2.start();
            t3.start();
        }
    }

    static class Worker implements Runnable {
        private final Object lock;
        private final String name;
        private final int id;
        private int count = 0;

        public Worker(Object lock, String name, int id) {
            this.lock = lock;
            this.name = name;
            this.id = id;
        }

        @Override
        public void run() {
            while (count < 10) {
                synchronized (lock) {
                    while (count % 3 != id) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println(name);
                    count++;
                    lock.notifyAll();
                }
            }
        }
    }

}
