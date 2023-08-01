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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import net.openhft.posix.PosixAPI;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.CHRONICLE_ANNOUNCER_DISABLE;

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
public class BinLog implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(BinLog.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final NoSpamLogger.NoSpamLogStatement droppedSamplesStatement = noSpamLogger.getStatement("Dropped {} binary log samples", 1, TimeUnit.MINUTES);

    public final Path path;

    public static final String VERSION = "version";
    public static final String TYPE = "type";

    static
    {
        // Avoid the chronicle announcement which is commercial advertisement, and debug info we already print at startup
        // https://github.com/OpenHFT/Chronicle-Core/blob/chronicle-core-2.23.36/src/main/java/net/openhft/chronicle/core/announcer/Announcer.java#L32-L33
        CHRONICLE_ANNOUNCER_DISABLE.setBoolean(true);
    }

    private ChronicleQueue queue;
    private ExcerptAppender appender;
    @VisibleForTesting
    Thread binLogThread = new NamedThreadFactory("Binary Log thread").newThread(this);
    final WeightedQueue<ReleaseableWriteMarshallable> sampleQueue;
    private final BinLogArchiver archiver;
    private final boolean blocking;

    private final AtomicLong droppedSamplesSinceLastLog = new AtomicLong();

    private BinLogOptions options;

    /*
    This set contains all the paths we are currently logging to, it is used to make sure
    we don't start writing audit and full query logs to the same path.
    */
    private static final Set<Path> currentPaths = Collections.synchronizedSet(new HashSet<>());

    private static final ReleaseableWriteMarshallable NO_OP = new ReleaseableWriteMarshallable()
    {
        @Override
        protected long version()
        {
            return 0;
        }

        @Override
        protected String type()
        {
            return "no-op";
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
        }

        @Override
        public void release()
        {
        }
    };

    private volatile boolean shouldContinue = true;

    private BinLog(Path path, BinLogOptions options, BinLogArchiver archiver)
    {
        Preconditions.checkNotNull(PosixAPI.posix(), "Cannot initialize OpenHFT Posix");
        Preconditions.checkNotNull(path, "path was null");
        Preconditions.checkNotNull(options.roll_cycle, "roll_cycle was null");
        Preconditions.checkArgument(options.max_queue_weight > 0, "max_queue_weight must be > 0");
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single(path.toFile()); // checkstyle: permit this invocation
        builder.rollCycle(RollCycles.valueOf(options.roll_cycle));

        sampleQueue = new WeightedQueue<>(options.max_queue_weight);
        this.archiver = archiver;
        builder.storeFileListener(this.archiver);
        queue = builder.build();
        appender = queue.acquireAppender();
        this.blocking = options.block;
        this.path = path;
        this.options = options;
    }

    public BinLogOptions getBinLogOptions()
    {
        return options;
    }

    /**
     * Start the consumer thread that writes log records. Can only be done once.
     */
    @VisibleForTesting
    void start()
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
        BackgroundResourceReleaser.stop();
        binLogThread.join();
        appender.close();
        appender = null;
        queue.close();
        queue = null;
        archiver.stop();
        currentPaths.remove(path);
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

    // todo: refactor to helper class?
    public void logRecord(ReleaseableWriteMarshallable record)
    {
        boolean putInQueue = false;
        try
        {
            if (blocking)
            {
                try
                {
                    put(record);
                    putInQueue = true;
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
            }
            else
            {
                if (!offer(record))
                {
                    logDroppedSample();
                }
                else
                {
                    putInQueue = true;
                }
            }
        }
        finally
        {
            if (!putInQueue)
            {
                record.release();
            }
        }
    }

    /**
     * This is potentially lossy, but it's not super critical as we will always generally know
     * when this is happening and roughly how bad it is.
     */
    private void logDroppedSample()
    {
        droppedSamplesSinceLastLog.incrementAndGet();
        if (droppedSamplesStatement.warn(new Object[] {droppedSamplesSinceLastLog.get()}))
        {
            droppedSamplesSinceLastLog.set(0);
        }
    }


    public abstract static class ReleaseableWriteMarshallable implements WriteMarshallable
    {
        @Override
        public final void writeMarshallable(WireOut wire)
        {
            wire.write(VERSION).int16(version());
            wire.write(TYPE).text(type());

            writeMarshallablePayload(wire);
        }

        protected abstract long version();

        protected abstract String type();

        protected abstract void writeMarshallablePayload(WireOut wire);

        public abstract void release();
    }

    public static class Builder
    {
        private Path path;
        private String rollCycle;
        private int maxQueueWeight;
        private long maxLogSize;
        private String archiveCommand;
        private int maxArchiveRetries;
        private boolean blocking;

        public Builder path(Path path)
        {
            Preconditions.checkNotNull(path, "path was null");
            File pathAsFile = new File(path);
            //Exists and is a directory or can be created
            Preconditions.checkArgument(!pathAsFile.toString().isEmpty(), "you might have forgotten to specify a directory to save logs");
            Preconditions.checkArgument((pathAsFile.exists() && pathAsFile.isDirectory()) || (!pathAsFile.exists() && pathAsFile.tryCreateDirectories()), "path exists and is not a directory or couldn't be created");
            Preconditions.checkArgument(pathAsFile.isReadable() && pathAsFile.isWritable() && pathAsFile.isExecutable(), "path is not readable, writable, and executable");
            this.path = path;
            return this;
        }

        public Builder rollCycle(String rollCycle)
        {
            Preconditions.checkNotNull(rollCycle, "rollCycle was null");
            rollCycle = rollCycle.toUpperCase();
            Preconditions.checkNotNull(RollCycles.valueOf(rollCycle), "unrecognized roll cycle");
            this.rollCycle = rollCycle;
            return this;
        }

        public Builder maxQueueWeight(int maxQueueWeight)
        {
            Preconditions.checkArgument(maxQueueWeight > 0, "maxQueueWeight must be > 0");
            this.maxQueueWeight = maxQueueWeight;
            return this;
        }

        public Builder maxLogSize(long maxLogSize)
        {
            Preconditions.checkArgument(maxLogSize > 0, "maxLogSize must be > 0");
            this.maxLogSize = maxLogSize;
            return this;
        }

        public Builder archiveCommand(String archiveCommand)
        {
            this.archiveCommand = archiveCommand;
            return this;
        }

        public Builder maxArchiveRetries(int maxArchiveRetries)
        {
            this.maxArchiveRetries = maxArchiveRetries;
            return this;
        }

        public Builder blocking(boolean blocking)
        {
            this.blocking = blocking;
            return this;
        }


        public BinLog build(boolean cleanDirectory)
        {
            logger.info("Attempting to configure bin log: Path: {} Roll cycle: {} Blocking: {} Max queue weight: {} Max log size:{} Archive command: {}", path, rollCycle, blocking, maxQueueWeight, maxLogSize, archiveCommand);
            synchronized (currentPaths)
            {
                if (currentPaths.contains(path))
                    throw new IllegalStateException("Already logging to " + path);
                currentPaths.add(path);
            }
            try
            {
                Throwable sanitationThrowable = cleanEmptyLogFiles(new File(path), null);
                if (sanitationThrowable != null)
                    throw new RuntimeException(format("Unable to clean up %s directory from empty %s files.",
                                                      path.toAbsolutePath(), SingleChronicleQueue.SUFFIX),
                                               sanitationThrowable);

                // create the archiver before cleaning directories - ExternalArchiver will try to archive any existing file.
                BinLogArchiver archiver = Strings.isNullOrEmpty(archiveCommand) ? new DeletingArchiver(maxLogSize) : new ExternalArchiver(archiveCommand, path, maxArchiveRetries);
                if (cleanDirectory)
                {
                    logger.info("Cleaning directory: {} as requested", path);
                    if (new File(path).exists())
                    {
                        Throwable error = cleanDirectory(new File(path), null);
                        if (error != null)
                        {
                            throw new RuntimeException(error);
                        }
                    }
                }

                final BinLogOptions options = new BinLogOptions();

                options.max_log_size = maxLogSize;
                options.max_queue_weight = maxQueueWeight;
                options.block = blocking;
                options.roll_cycle = rollCycle;
                options.archive_command = archiveCommand;
                options.max_archive_retries = maxArchiveRetries;

                BinLog binlog = new BinLog(path, options, archiver);
                binlog.start();
                return binlog;
            }
            catch (Exception e)
            {
                currentPaths.remove(path);
                throw e;
            }
        }
    }

    /**
     * ChronicleQueue fails to start on cq4 files which are empty. Find such files in log dir and remove them.
     */
    private static Throwable cleanEmptyLogFiles(File directory, Throwable accumulate)
    {
        return cleanDirectory(directory, accumulate,
                              (dir) -> dir.tryList(file -> {
                                  boolean foundEmptyCq4File = !file.isDirectory()
                                                              && file.length() == 0
                                                              && file.name().endsWith(SingleChronicleQueue.SUFFIX);

                                  if (foundEmptyCq4File)
                                      logger.warn("Found empty ChronicleQueue file {}. This file wil be deleted as part of BinLog initialization.",
                                                  file.absolutePath());

                                  return foundEmptyCq4File;
                              }));
    }

    public static Throwable cleanDirectory(File directory, Throwable accumulate)
    {
        return cleanDirectory(directory, accumulate, File::tryList);
    }

    private static Throwable cleanDirectory(File directory, Throwable accumulate, Function<File, File[]> lister)
    {
        accumulate = checkDirectory(directory, accumulate);

        if (accumulate != null)
            return accumulate;

        File[] files = lister.apply(directory);

        if (files != null)
            for (File f : files)
                accumulate = deleteRecursively(f, accumulate);

        if (accumulate instanceof FSError)
            JVMStabilityInspector.inspectThrowable(accumulate);

        return accumulate;
    }

    private static Throwable deleteRecursively(File fileOrDirectory, Throwable accumulate)
    {
        if (fileOrDirectory.isDirectory())
        {
            File[] files = fileOrDirectory.tryList();
            if (files != null)
                for (File f : files)
                    accumulate = f.delete(accumulate, null);
        }
        return fileOrDirectory.delete(accumulate, null);
    }

    private static Throwable checkDirectory(File directory, Throwable accumulate)
    {
        if (!directory.exists())
            accumulate = Throwables.merge(accumulate, new RuntimeException(format("%s does not exist", directory)));

        if (!directory.isDirectory())
            accumulate = Throwables.merge(accumulate, new RuntimeException(format("%s is not a directory", directory)));

        return accumulate;
    }
}
