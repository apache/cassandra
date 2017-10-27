/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.binlog;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

/**
 * Bin log is a is quick and dirty binary log that is kind of a NIH version of binary logging with a traditional logging
 * framework. It's goal is good enough performance, predictable footprint, simplicity in terms of implementation and configuration
 * and most importantly minimal impact on producers of log records.
 *
 * Performance safety is accomplished by feeding items to the binary log using a weighted queue and dropping records if the binary log falls
 * sufficiently far behind.
 *
 * Simplicity and good enough perforamance is achieved by using a single log writing thread as well as Chronicle Queue
 * to handle writing the log, making it available for readers, as well as log rolling.
 *
 */
public class BinLog implements Runnable, StoreFileListener
{
    private static final Logger logger = LoggerFactory.getLogger(BinLog.class);

    private ChronicleQueue queue;
    private ExcerptAppender appender;
    @VisibleForTesting
    Thread binLogThread = new NamedThreadFactory("Binary Log thread").newThread(this);
    final WeightedQueue<ReleaseableWriteMarshallable> sampleQueue;
    private final long maxLogSize;

    /**
     * The files in the chronicle queue that have already rolled
     */
    private Queue<File> chronicleStoreFiles = new ConcurrentLinkedQueue<>();

    /**
     * The number of bytes in store files that have already rolled
     */
    private long bytesInStoreFiles;

    private static final ReleaseableWriteMarshallable NO_OP = new ReleaseableWriteMarshallable()
    {
        @Override
        public void writeMarshallable(WireOut wire)
        {
        }

        @Override
        public void release()
        {
        }
    };

    private volatile boolean shouldContinue = true;

    /**
     *
     * @param path Path to store the BinLog. Can't be shared with anything else.
     * @param rollCycle How often to roll the log file so it can potentially be deleted
     * @param maxQueueWeight Maximum weight of in memory queue for records waiting to be written to the file before blocking or dropping
     * @param maxLogSize Maximum size of the rolled files to retain on disk before deleting the oldest file
     */
    public BinLog(Path path, RollCycle rollCycle, int maxQueueWeight, long maxLogSize)
    {
        Preconditions.checkNotNull(path, "path was null");
        Preconditions.checkNotNull(rollCycle, "rollCycle was null");
        Preconditions.checkArgument(maxQueueWeight > 0, "maxQueueWeight must be > 0");
        Preconditions.checkArgument(maxLogSize > 0, "maxLogSize must be > 0");
        ChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path.toFile());
        builder.rollCycle(rollCycle);
        builder.storeFileListener(this);
        queue = builder.build();
        appender = queue.acquireAppender();
        sampleQueue = new WeightedQueue<>(maxQueueWeight);
        this.maxLogSize = maxLogSize;
    }

    /**
     * Start the consumer thread that writes log records. Can only be done once.
     */
    public void start()
    {
        if (!shouldContinue)
        {
            throw new IllegalStateException("Can't reuse stopped BinLog");
        }
        binLogThread.start();
    }

    /**
     * Stop the consumer thread that writes log records. Can be called multiple times.
     * @throws InterruptedException
     */
    public synchronized void stop() throws InterruptedException
    {
        if (!shouldContinue)
        {
            return;
        }

        shouldContinue = false;
        sampleQueue.put(NO_OP);
        binLogThread.join();
        appender = null;
        queue = null;
    }

    /**
     * Offer a record to the log. If the in memory queue is full the record will be dropped and offer will return false.
     * @param record The record to write to the log
     * @return true if the record was queued and false otherwise
     */
    public boolean offer(ReleaseableWriteMarshallable record)
    {
        if (!shouldContinue)
        {
            return false;
        }

        return sampleQueue.offer(record);
    }

    /**
     * Put a record into the log. If the in memory queue is full the putting thread will be blocked until there is space or it is interrupted.
     * @param record The record to write to the log
     * @throws InterruptedException
     */
    public void put(ReleaseableWriteMarshallable record) throws InterruptedException
    {
        if (!shouldContinue)
        {
            return;
        }

        //Resolve potential deadlock at shutdown when queue is full
        while (shouldContinue)
        {
            if (sampleQueue.offer(record, 1, TimeUnit.SECONDS))
            {
                return;
            }
        }
    }

    private void processTasks(List<ReleaseableWriteMarshallable> tasks)
    {
        for (int ii = 0; ii < tasks.size(); ii++)
        {
            WriteMarshallable t = tasks.get(ii);
            //Don't write an empty document
            if (t == NO_OP)
            {
                continue;
            }

            appender.writeDocument(t);
        }
    }

    @Override
    public void run()
    {
        List<ReleaseableWriteMarshallable> tasks = new ArrayList<>(16);
        while (shouldContinue)
        {
            try
            {
                tasks.clear();
                ReleaseableWriteMarshallable task = sampleQueue.take();
                tasks.add(task);
                sampleQueue.drainTo(tasks, 15);

                processTasks(tasks);
            }
            catch (Throwable t)
            {
                logger.error("Unexpected exception in binary log thread", t);
            }
            finally
            {
                for (int ii = 0; ii < tasks.size(); ii++)
                {
                    tasks.get(ii).release();
                }
            }
        }

        //Clean up the buffers on thread exit, finalization will check again once this
        //is no longer reachable ensuring there are no stragglers in the queue.
        finalize();
    }

    /**
     * Track store files as they are added and their storage impact. Delete them if over storage limit.
     * @param cycle
     * @param file
     */
    public synchronized void onReleased(int cycle, File file)
    {
        chronicleStoreFiles.offer(file);
        //This isn't accurate because the files are sparse, but it's at least pessimistic
        bytesInStoreFiles += file.length();
        logger.debug("Chronicle store file {} rolled file size {}", file.getPath(), file.length());
        while (bytesInStoreFiles > maxLogSize & !chronicleStoreFiles.isEmpty())
        {
            File toDelete = chronicleStoreFiles.poll();
            long toDeleteLength = toDelete.length();
            if (!toDelete.delete())
            {
                logger.error("Failed to delete chronicle store file: {} store file size: {} bytes in store files: {}. " +
                             "You will need to clean this up manually or reset full query logging.",
                             toDelete.getPath(), toDeleteLength, bytesInStoreFiles);
            }
            else
            {
                bytesInStoreFiles -= toDeleteLength;
                logger.info("Deleted chronicle store file: {} store file size: {} bytes in store files: {} max log size: {}.", file.getPath(), toDeleteLength, bytesInStoreFiles, maxLogSize);
            }
        }
    }

    /**
     * There is a race where we might not release a buffer, going to let finalization
     * catch it since it shouldn't happen to a lot of buffers. Only test code would run
     * into it anyways.
     */
    @Override
    public void finalize()
    {
        ReleaseableWriteMarshallable toRelease;
        while (((toRelease = sampleQueue.poll()) != null))
        {
            toRelease.release();
        }
    }

    public abstract static class ReleaseableWriteMarshallable implements WriteMarshallable
    {
        protected abstract void release();
    }
}
