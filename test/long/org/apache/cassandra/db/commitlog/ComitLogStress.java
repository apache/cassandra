package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class ComitLogStress
{

    public static final String format = "%s,%s,%s,%s,%s,%s";

    public static void main(String[] args) throws Exception {
        int NUM_THREADS = Runtime.getRuntime().availableProcessors();
        if (args.length >= 1) {
            NUM_THREADS = Integer.parseInt(args[0]);
            System.out.println("Setting num threads to: " + NUM_THREADS);
        }
        ExecutorService executor = new JMXEnabledThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 60,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10 * NUM_THREADS), new NamedThreadFactory(""), "");
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

        org.apache.cassandra.SchemaLoader.loadSchema();
        final AtomicLong count = new AtomicLong();
        final long start = System.currentTimeMillis();
        System.out.println(String.format(format, "seconds", "max_mb", "allocated_mb", "free_mb", "diffrence", "count"));
        scheduled.scheduleAtFixedRate(new Runnable() {
            long lastUpdate = 0;

            public void run() {
                Runtime runtime = Runtime.getRuntime();
                long maxMemory = mb(runtime.maxMemory());
                long allocatedMemory = mb(runtime.totalMemory());
                long freeMemory = mb(runtime.freeMemory());
                long temp = count.get();
                System.out.println(String.format(format, ((System.currentTimeMillis() - start) / 1000),
                        maxMemory, allocatedMemory, freeMemory, (temp - lastUpdate), lastUpdate));
                lastUpdate = temp;
            }
        }, 1, 1, TimeUnit.SECONDS);

        while (true) {
            executor.execute(new CommitlogExecutor());
            count.incrementAndGet();
        }
    }

    private static long mb(long maxMemory) {
        return maxMemory / (1024 * 1024);
    }

    static final String keyString = UUIDGen.getTimeUUID().toString();
    public static class CommitlogExecutor implements Runnable {
        public void run() {
            String ks = "Keyspace1";
            ByteBuffer key = ByteBufferUtil.bytes(keyString);
            RowMutation mutation = new RowMutation(ks, key);
            mutation.add("Standard1", ByteBufferUtil.bytes("name"), ByteBufferUtil.bytes("value"),
                    System.currentTimeMillis());
            CommitLog.instance.add(mutation);
        }
    }
}