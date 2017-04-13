package org.apache.cassandra.cache.capi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

public class CapiChunkDriver {
    private static final Logger logger = LoggerFactory.getLogger(CapiChunkDriver.class);

    public static AtomicLong requested = new AtomicLong();
    public static AtomicLong executed = new AtomicLong();
    public static AtomicLong totalElapsed = new AtomicLong();

    public static boolean pooled = System.getProperty("capi.driver.task.nopool") == null;
    public static int numOfMaxThreads = Integer.parseInt(System.getProperty("capi.driver.thread.max", "32"));
    public static int numOfMinThreads = Integer.parseInt(System.getProperty("capi.driver.thread.min", "16"));
    public static int numOfQueue = Integer.parseInt(System.getProperty("capi.driver.queue", "4"));
    public static int FLUSH_QUEUE_MAX_DEPTH = Integer.parseInt(System.getProperty("capi.driver.queue.max", "10000"));

    public static boolean polling = System.getProperty("capi.driver.thread.polling") != null;

    public static int getAlignedSize(int size) {
        return getLBASize(size) * CapiBlockDevice.BLOCK_SIZE;
    }

    public static int getLBASize(int size) {
        if (size % CapiBlockDevice.BLOCK_SIZE == 0)
            return size / CapiBlockDevice.BLOCK_SIZE;
        else
            return size / CapiBlockDevice.BLOCK_SIZE + 1;
    }

    public static interface AsyncHandler {
        public void success(ByteBuffer bb);

        public void error(String msg);
    }

    final String[] deviceNames;
    final TaskQueue[] queues;
    AtomicInteger nextQueue = new AtomicInteger(-1);

    public CapiChunkDriver(String deviceName, int numOfAsync) throws IOException {
        this(new String[] { deviceName }, numOfAsync);
    }

    public CapiChunkDriver(String[] deviceNames, int numOfAsync) throws IOException {
        this(deviceNames, numOfAsync, numOfMinThreads, numOfMaxThreads, numOfQueue);
    }

    public CapiChunkDriver(String[] deviceNames, int numOfAsync, int numOfQueues) throws IOException {
        this(deviceNames, numOfAsync, numOfMinThreads, numOfMaxThreads, numOfQueues);
    }

    public CapiChunkDriver(String[] deviceNames, int numOfAsync, int numOfMinThreads, int numOfMaxThreads, int numOfDrivers) throws IOException {
        this.deviceNames = deviceNames;
        this.queues = new TaskQueue[numOfDrivers];

        int numOfMinThreadsForEach = (numOfMinThreads / numOfDrivers) + ((numOfMinThreads % numOfDrivers == 0) ? 0 : 1);
        int numOfMaxThreadsForEach = (numOfMaxThreads / numOfDrivers) + ((numOfMaxThreads % numOfDrivers == 0) ? 0 : 1);

        for (int i = 0; i < queues.length; ++i)
            queues[i] = new TaskQueue(numOfAsync, numOfMinThreadsForEach, numOfMaxThreadsForEach);
    }

    private TaskQueue getNextQueue() {
        return queues[Math.abs(nextQueue.incrementAndGet()) % queues.length];
    }

    class HandlerFuture implements Future<ByteBuffer> {

        boolean done = false;
        ByteBuffer ret = null;
        String errMsg = null;

        AsyncHandler handler = new AsyncHandler() {

            @Override
            public void success(ByteBuffer bb) {
                finished(bb);
            }

            @Override
            public void error(String msg) {
            }
        };

        synchronized void finished(ByteBuffer bb) {
            this.ret = bb;
            done = true;
            notifyAll();
        }

        synchronized void finished(String errMsg) {
            this.errMsg = errMsg;
            done = true;
            notifyAll();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public synchronized boolean isDone() {
            return done;
        }

        @Override
        public synchronized ByteBuffer get() throws InterruptedException, ExecutionException {
            while (!done) {
                wait();
            }

            if (errMsg != null)
                throw new ExecutionException(new Exception(errMsg));

            return ret;
        }

        @Override
        public ByteBuffer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }

    }

    public void writeAsync(long lba, ByteBuffer bb, AsyncHandler handler) throws IOException {
        getNextQueue().writeAsync(lba, bb, handler);
    }

    public Future<ByteBuffer> writeAsync(long lba, ByteBuffer bb) throws IOException {
        HandlerFuture ret = new HandlerFuture();
        getNextQueue().writeAsync(lba, bb, ret.handler);
        return ret;
    }

    public void readAsync(long lba, int numOfBlocks, AsyncHandler handler) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(numOfBlocks * CapiBlockDevice.BLOCK_SIZE);
        getNextQueue().readAsync(lba, bb, handler);
    }

    public Future<ByteBuffer> readAsync(long lba, ByteBuffer bb) throws IOException {
        HandlerFuture ret = new HandlerFuture();
        getNextQueue().readAsync(lba, bb, ret.handler);
        return ret;
    }

    public ByteBuffer read(long lba, int numOfBlocks) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(numOfBlocks * CapiBlockDevice.BLOCK_SIZE);
        getNextQueue().read(lba, bb);
        return bb;
    }

    public void write(long lba, ByteBuffer bb) throws IOException {
        getNextQueue().write(lba, bb);
    }

    public void flush() {
        while (true) {
            boolean working = false;
            for (TaskQueue queue : queues) {
                if (queue.numOfRequesting.get() != 0) {
                    working = true;
                }
            }
            if (!working)
                break;
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public void close() throws IOException {
        for (TaskQueue queue : queues)
            queue.close();
    }

    class TaskQueue {
        final Chunk[] chunks;
        final Map<Chunk, String> chunk2Device = new HashMap<>();
        final int numOfAsync;
        final ConcurrentLinkedQueue<Task> waiting = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Task> processing = new ConcurrentLinkedQueue<>();
        final AtomicInteger numOfRequesting = new AtomicInteger(0);
        final AtomicInteger concurrency = new AtomicInteger(0);
        final AtomicBoolean closed = new AtomicBoolean(false);

        final ConcurrentLinkedQueue<Flush> flushTaskPool = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Task> taskPool = new ConcurrentLinkedQueue<>();

        final AtomicInteger nextChunk = new AtomicInteger(0);

        final ExecutorService service;
        final Thread[] flushThreads;

        public TaskQueue(int numOfAsync, int numOfMinThreads, int numOfMaxThreads) throws IOException {
            this.chunks = new Chunk[deviceNames.length];
            for (int i = 0; i < chunks.length; ++i) {
                this.chunks[i] = CapiBlockDevice.getInstance().openChunk(deviceNames[i], numOfAsync);
                this.chunk2Device.put(this.chunks[i], deviceNames[i]);
            }

            this.numOfAsync = numOfAsync;

            if (polling) {
                this.flushThreads = new Thread[numOfMinThreads];
                for (int i = 0; i < numOfMinThreads; ++i) {
                    this.flushThreads[i] = new Thread() {
                        public void run() {
                            Flush flush = new Flush();
                            while (!closed.get()) {
                                flush.process();
                            }
                        }
                    };
                    this.flushThreads[i].start();
                }
                this.service = null;
            } else {
                this.flushThreads = null;
                this.service = new ThreadPoolExecutor(numOfMinThreads, numOfMaxThreads, 100, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(FLUSH_QUEUE_MAX_DEPTH));
            }

            for (int i = 0; i < FLUSH_QUEUE_MAX_DEPTH; ++i)
                flushTaskPool.add(new Flush());
        }

        public String toString() {
            return "[capichunkdriver path=" + chunk2Device.values() + "]";
        }

        private Chunk getNextChunk() {
            return chunks[Math.abs(nextChunk.incrementAndGet() % chunks.length)];
        }

        private Task newTask(long lba) {
            Task ret = null;
            if (pooled) {
                ret = taskPool.poll();
            }
            if (ret == null)
                ret = new Task();
            return ret;
        }

        private void freeTask(Task task) {
            if (pooled) {
                taskPool.add(task);
            }
        }

        public void read(long lba, ByteBuffer bb) throws IOException {
            if (bb.capacity() % CapiBlockDevice.BLOCK_SIZE != 0)
                throw new IllegalArgumentException("bytebuffer is not aligned to the blocksize (" + CapiBlockDevice.BLOCK_SIZE + ")");
            int numOfBlocks = bb.capacity() / CapiBlockDevice.BLOCK_SIZE;
            Chunk chunk = getNextChunk();
            Future<Long> future = chunk.readBlockAsync(lba, numOfBlocks, bb);
            long rc = 0L;
            try {
                while ((rc = future.get(10000L, TimeUnit.SECONDS)) == 0L)
                    ;
            } catch (Exception ex) {
                logger.error("capi read error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
                throw new IOException(ex);
            }

            if (rc != numOfBlocks)
                throw new IOException("capi device error: " + chunk2Device.get(chunk));
        }

        public void write(long lba, ByteBuffer bb) throws IOException {
            ByteBuffer original = null;
            if (bb.capacity() % CapiBlockDevice.BLOCK_SIZE != 0) {
                original = bb;
                bb = ByteBuffer.allocateDirect(original.capacity());
                bb.put(original);
                bb.rewind();
            }

            int numOfBlocks = getLBASize(bb.capacity());
            Chunk chunk = getNextChunk();

            Future<Long> future = chunk.writeBlockAsync(lba, numOfBlocks, bb);
            long rc = 0L;
            try {
                while ((rc = future.get(10000L, TimeUnit.SECONDS)) == 0L)
                    ;
            } catch (Exception ex) {
                logger.error("capi read error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
                throw new IOException(ex);
            }

            if (rc != numOfBlocks)
                throw new IOException("capi device error: " + chunk2Device.get(chunk));
        }

        public void writeAsync(long lba, ByteBuffer bb, AsyncHandler handler) throws IOException {
            ByteBuffer original = null;
            if (bb.capacity() % CapiBlockDevice.BLOCK_SIZE != 0) {
                original = bb;
                bb = ByteBuffer.allocateDirect(original.capacity());
                bb.put(original);
                bb.rewind();
            }
            requested.incrementAndGet();
            waiting.add(newTask(lba).init(true, lba, handler, bb));
            requestProcess();
        }

        public void readAsync(long lba, ByteBuffer bb, AsyncHandler handler) throws IOException {
            if (bb.capacity() % CapiBlockDevice.BLOCK_SIZE != 0)
                throw new IllegalArgumentException("bytebuffer is not aligned to the blocksize (" + CapiBlockDevice.BLOCK_SIZE + ")");
            requested.incrementAndGet();
            waiting.add(newTask(lba).init(false, lba, handler, bb));
            requestProcess();
        }

        private void requestProcess() {
            Flush flush = flushTaskPool.poll();
            if (flush == null)
                flush = new Flush();

            numOfRequesting.incrementAndGet();

            if (service != null)
                service.submit(flush);
        }

        public void close() throws IOException {
            flush();
            for (Chunk chunk : chunks)
                chunk.close();

            if (service != null)
                service.shutdown();

            closed.set(true);
        }

        private class Task {
            boolean write;
            AsyncHandler handler;
            long lba;
            ByteBuffer bb;
            //long createdTs;
            Future<Long> future;
            Chunk chunk = getNextChunk();

            boolean error = false;

            private Task() {
            }

            Task init(boolean write, long lba, AsyncHandler handler, ByteBuffer bb) {
                this.write = write;
                this.lba = lba;
                this.handler = handler;
                this.bb = bb;
                this.future = null;
                this.error = false;
                return this;
            }

            boolean fork() {
                try {
                    int numOfBlocks = getLBASize(bb.capacity());
                    future = write ? chunk.writeBlockAsync(lba, numOfBlocks, bb) : chunk.readBlockAsync(lba, numOfBlocks, bb);
                    return true;
                } catch (IOException ex) {
                    logger.error("capi write error: " + ex.getMessage(), ex);
                    handler.error("capi " + (write ? "write" : "read") + " error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
                    return false;
                }
            }

            void join() {
                try {
                    long rc = 0;

                    if (future == null)
                        throw new IllegalStateException();

                    long start = System.currentTimeMillis();
                    while ((rc = future.get(10000L, TimeUnit.SECONDS)) == 0L)
                        ;
                    long elapsed = System.currentTimeMillis() - start;

                    executed.incrementAndGet();
                    totalElapsed.addAndGet(elapsed);

                    if (rc < 0)
                        error = true;
                    else if (!write)
                        bb.rewind();

                } catch (Exception ex) {
                    logger.error("capi write error: " + ex.getMessage(), ex);
                    handler.error("capi " + (write ? "write" : "read") + " error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
                }
            }

            void report() {
                if (error)
                    handler.error("capi " + (write ? "write" : "read") + " error: deviceName=" + chunk2Device.get(chunk) + ", lba=" + lba);
                else
                    handler.success(bb);

                freeTask(this);
            }
        }

        public class Flush implements Runnable {

            public void run() {
                if (closed.get()) {
                    flushTaskPool.add(this);
                    return;
                }

                process();

                flushTaskPool.add(this);
            }

            private void process() {
                Task joining = null;
                Task next = null;
                while (numOfRequesting.get() != 0 && !closed.get()) {
                    int currentConcurrency = concurrency.get();

                    if (currentConcurrency < numOfAsync) {
                        if (!concurrency.compareAndSet(currentConcurrency, currentConcurrency + 1))
                            continue;

                        next = waiting.poll();

                        if (next != null) {
                            // if a new tasks exists, fork a process
                            next.fork();
                            processing.add(next);
                            requestProcess();
                            numOfRequesting.decrementAndGet();
                            return;
                        }

                        // no new task. decrease concurrency
                        concurrency.decrementAndGet();
                    } else {
                        if (currentConcurrency != numOfAsync)
                            throw new IllegalStateException();
                    }

                    joining = processing.poll();
                    if (joining == null)
                        continue;

                    joining.join();
                    concurrency.decrementAndGet();
                    try {
                        joining.report();
                    } catch (Throwable th) {
                        logger.error(th.getMessage(), th);
                    }
                    numOfRequesting.decrementAndGet();
                    return;
                }
            }
        }
    }

}
