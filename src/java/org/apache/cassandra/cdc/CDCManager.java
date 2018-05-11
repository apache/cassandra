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
package org.apache.cassandra.cdc;

import java.io.BufferedReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerCDC;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CDCMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.DatabaseDescriptor.getFlushWriters;

public class CDCManager implements CDCManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CDCManager.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.cdc:type=CDCManager";
    public static final long READER_TIMEOUT_MS = 1 * 3600 * 1000; // 1 hour
    public static final long SCAN_PERIOD_MS = 1000;
    public static final CDCManager instance;

    @VisibleForTesting
    static FilenameFilter idxFilesFilter = (dir, name) -> CommitLogDescriptor.isIdxValid(name);

    static Predicate<File> idxFilesPredicate = new Predicate<File>()
    {
        public boolean test(File file)
        {
            return CommitLogDescriptor.isIdxValid(file.name());
        }
    };

    private boolean started = false;
    private long activeIdxCount = -1;
    private long readerTimeout = READER_TIMEOUT_MS;
    private long scanPeriod = SCAN_PERIOD_MS;

    private ScheduledExecutorPlus cdcExecutor = newCdcExecutor();
    private final ExecutorPlus workerExecutor = executorFactory()
                                                .withJmxInternal()
                                                .configurePooled("CDCWorker", 10)
                                                .withKeepAlive(60, TimeUnit.SECONDS)
                                                .build();

    private final CDCMetrics metrics;
    private final Map<Long, CDCReaderStatus> readerStatuses;

    private CDCManager()
    {
        metrics = new CDCMetrics();
        readerStatuses = new HashMap<>();
    }

    static
    {
        logger.info("Initializing CDC...");
        instance = new CDCManager();
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }

    private static ScheduledExecutorPlus newCdcExecutor()
    {
        return executorFactory().scheduled(false, "CDCManager", Thread.NORM_PRIORITY);
//        return new DebuggableScheduledThreadPoolExecutor(1, new NamedThreadFactory("CDCManager"));
    }

    /**
     * The CDCHandler class.
     */
    private class CDCHandler implements CommitLogReadHandler
    {
        @Override
        public boolean shouldSkipSegmentOnError(CommitLogReadException exception)
        {
            logger.error("CDC log reader unable to read data", exception);
            metrics.errors.mark();
            // TODO: make the return value configurable
            return false;
        }

        @Override
        public void handleUnrecoverableError(CommitLogReadException exception)
        {
            logger.error("CDC log reader got an unrecoverable error", exception);
            metrics.errors.mark();
        }

        @Override
        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            boolean cdc = false;
            boolean hasError = false;
            for (TableId tableId : m.getTableIds())
            {
                if (Schema.instance.getTableMetadata(tableId).params.cdc)
                    cdc = true;
            }

            if (cdc)
            {
                String ksName = m.getKeyspaceName();
                Keyspace ks = Schema.instance.getKeyspaceInstance(ksName);
                ICDCHandler handler = ks.getCDCHandler();
                // return quietly if handler is null as we don't want per-mutation log for misconfigured keyspaces
                if (handler == null)
                {
                    return;
                }
                long st = System.currentTimeMillis();
                try
                {
                    handler.process(m);
                }
                catch (IOException e)
                {
                    // Throw a RuntimeException because handleMutation() does not throw IOException.
                    hasError = true;
                    logger.info("Throwing IOException as RuntimeException in handleMutation...");
                    throw new RuntimeException(e);
                }
                catch (RuntimeException e)
                {
                    hasError = true;
                    throw e;
                }
                finally
                {
                    metrics.latency.update(System.currentTimeMillis() - st, TimeUnit.MILLISECONDS);
                    if (hasError)
                    {
                        metrics.failures.mark();
                        logger.info("Setting furthest position to {} for commitLog.id={} due to error", entryLocation, desc.id);
                        readerStatuses.get(desc.id).setFurthestPosition(entryLocation);
                    }
                    else
                    {
                        metrics.successes.mark();
                    }
                }
            }
        }
    }

    private final CommitLogReadHandler handler = new CDCHandler();

    /**
     * The CDCReader thread class.
     */
    private final class CDCReader implements Runnable
    {
        private final CommitLogPosition minPos;
        private final CommitLogPosition maxPos;
        private final CommitLogDescriptor commitLog;
        private boolean isCompleted;
        private final File logFile;
        private final File idxFile;

        /**
         * Creates a CDCReader instance.
         *
         * @param commitLog   the commit log to read
         * @param minPosition the min position to read from
         * @param maxPosition the max position
         * @param isCompleted whether the commit log is completed
         */
        private CDCReader(CommitLogDescriptor commitLog, CommitLogPosition minPosition, CommitLogPosition maxPosition, boolean isCompleted)
        {
            this.commitLog = commitLog;
            this.minPos = minPosition;
            this.maxPos = maxPosition;
            this.isCompleted = isCompleted;
            this.logFile = new File(DatabaseDescriptor.getCDCLogLocation() + File.pathSeparator() + commitLog.fileName());
            this.idxFile = new File(DatabaseDescriptor.getCDCLogLocation() + File.pathSeparator() + commitLog.cdcIndexFileName());
        }

        public void run()
        {
            CommitLogReader reader = new CommitLogReader();

            if (!logFile.exists())
            {
                // if commit log file does not exist, then delete its idx file
                logger.error("Commit log file {} does not exist. Deleting the idx file...", logFile.name());
                metrics.errors.mark();
                deleteIdxFile();
                return;
            }

            try
            {
                reader.readCommitLogSegment(handler, logFile, minPos, maxPos, false);
            }
            catch (IOException | RuntimeException e)
            {
                logger.error("Error processing mutations or unable to read log segment", e);
                metrics.errors.mark();
                return;
            }
            if (isCompleted)
            {
                logger.info("Deleting completed files: commitLog.id={}...", commitLog.id);
                deleteLogFile();
                deleteIdxFile();
                readerStatuses.remove(commitLog.id);
            }
        }

        /**
         * Deletes the commit log file.
         */
        private void deleteLogFile()
        {
            if (!logFile.tryDelete())
                logger.error("Failed to delete file {}. Maybe it does not exist already.", logFile.name());
            else
                logger.debug("Deleted file {}", logFile.name());
        }

        /**
         * Deletes the commit log idx file.
         */
        private void deleteIdxFile()
        {
            if (!idxFile.tryDelete())
                logger.error("Failed to delete file {}. Maybe it does not exist already.", idxFile.name());
            else
                logger.debug("Deleted file {}", idxFile.name());
        }
    }

    /**
     * Tracks the status of a CDCReader thread.
     */
    private final class CDCReaderStatus
    {
        private final Future<?> future;
        private final long startTime;
        private int furthestPosition;

        /**
         * Creates an CDCReaderStatus instance.
         *
         * @param furthestPosition the end position which this thread is supposed to read up to
         * @param future           the Future instance of the thread
         * @param startTime        the start time of the thread in ms
         */
        private CDCReaderStatus(int furthestPosition, Future<?> future, long startTime)
        {
            this.furthestPosition = furthestPosition;
            this.future = future;
            this.startTime = startTime;
        }

        public int getFurthestPosition()
        {
            return furthestPosition;
        }

        public void setFurthestPosition(int furthestPosition)
        {
            this.furthestPosition = furthestPosition;
        }

        public Future<?> getFuture()
        {
            return future;
        }

        public long getStartTime()
        {
            return startTime;
        }

        public boolean isDone()
        {
            return getFuture().isDone();
        }

        public boolean cancel()
        {
            return getFuture().cancel(true);
        }
    }

    /**
     * Periodically scans idx files in cdc_raw_directory.
     */
    private void scanLogs()
    {
        try
        {
            updateCDCTotalSize();
            File cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
            File[] idxFiles = cdcDir.list(idxFilesPredicate);
            activeIdxCount = idxFiles.length;
            logger.debug("CDC log scan: activeIdxCount: {}", activeIdxCount);
            for (File f : idxFiles)
            {
                CommitLogDescriptor commitLog = CommitLogDescriptor.fromIdxFileName(f.name());
                if (readerStatuses.containsKey(commitLog.id))
                {
                    CDCReaderStatus readerStatus = readerStatuses.get(commitLog.id);
                    logger.debug("Reader status: commitLog.id={}, isDone={}, furthestPosition={}, startTime={}",
                                 commitLog.id, readerStatus.isDone(), readerStatus.getFurthestPosition(), readerStatus.getStartTime());

                    if (!readerStatus.isDone())
                    {
                        if (System.currentTimeMillis() - readerStatus.getStartTime() > getReaderTimeout())
                        {
                            logger.warn("Reader timed out for commitLog.id={}, canceling the reader thread...", commitLog.id);
                            readerStatus.cancel();
                            metrics.timeouts.mark();
                        }
                        else
                        {
                            logger.debug("The reader thread for log {} is still running, doing nothing...", commitLog.id);
                            continue;
                        }
                    }
                }

                boolean isCompleted = false;
                boolean needsRead = false;
                int offset = -1;

                // Read index file
                BufferedReader idxFile = new BufferedReader(new FileReader(f));
                String resStr = idxFile.readLine();

                try
                {
                    offset = Integer.parseInt(resStr);
                }
                catch (NumberFormatException e)
                {
                    // Retry if index is invalid (usually due to empty string)
                    logger.error("Invalid index input {} in {}", resStr, f.name());
                    metrics.errors.mark();
                    continue;
                }

                resStr = idxFile.readLine();
                if (resStr != null)
                {
                    if (resStr.equals("COMPLETED"))
                    {
                        logger.info("File {} is completed. The log will be deleted.", f.name());
                        isCompleted = true;
                        needsRead = true;
                        // offset = Integer.MAX_VALUE;
                    }
                }

                CommitLogPosition minPos = CommitLogPosition.NONE;
                CommitLogPosition maxPos = new CommitLogPosition(commitLog.id, offset);
                if (readerStatuses.containsKey(commitLog.id))
                {
                    logger.debug("Handling the existing log file...");
                    int pos = readerStatuses.get(commitLog.id).getFurthestPosition();
                    if (pos < offset)
                    {
                        needsRead = true;
                    }
                    minPos = new CommitLogPosition(commitLog.id, pos);
                }
                else
                {
                    logger.debug("Handling a new log file whose idx file is {}", f.name());
                    needsRead = true;
                }

                if (needsRead)
                {
                    logger.debug("Creating a new reader thread: commitLog.id={}, minPos={}, maxPos={}, isCompleted={}",
                                 commitLog.id, minPos, maxPos, isCompleted);
                    readerStatuses.put(commitLog.id,
                                       new CDCReaderStatus(offset, workerExecutor.submit(new CDCReader(commitLog, minPos, maxPos, isCompleted)), System.currentTimeMillis()));
                }
                else
                {
                    logger.debug("No log needs reading...");
                }
            }
        }
        catch (Exception e)
        {
            logger.error("CDC unable to scan the logs", e);
            metrics.errors.mark();
        }
    }

    /**
     * Checks if there's any cdc-enabled table but empty handler on the keyspace and log an error.
     */
    private void validateTables()
    {
        for (String keyspaceName : Schema.instance.getUserKeyspaces().names())
        {
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);
            for (TableMetadata table : Schema.instance.getTablesAndViews(keyspaceName))
            {
                if (table.params.cdc == true && keyspace.getCDCHandler() == null)
                {
                    logger.error("CDCManager is started, but cdc_handler for keyspace {} is not set. The CDC data for table {} will be deleted without consuming.",
                                 keyspaceName, table.name);
                }
            }
        }
        return;
    }

    /**
     * Gets the number of active CDC idx files.
     */
    public long getActiveIdxCount()
    {
        return activeIdxCount;
    }

    /**
     * Gets number of active worker threads.  Under low-throughput it may be difficult to "catch" a positive value from outside the worker threads because
     * the scan period is much longer than the actual life of a thread.
     */
    public int getActiveThreadCount()
    {
        return workerExecutor.getActiveTaskCount();
    }

    /**
     * Returns whether the CDC is started.
     */
    public boolean isStarted()
    {
        return started;
    }

    /**
     * Schedules the scan at fixed rate.
     */
    public void startCDCReader()
    {
        logger.info("Starting CDC...");
        // validate cdc-enabled tables for null cdc_handler
        validateTables();
        if (cdcExecutor.isShutdown())
        {
            logger.info("The CDC Executor was shut down. Creating a new CDC Executor...");
            cdcExecutor = newCdcExecutor();
        }
        cdcExecutor.scheduleAtFixedRate(this::scanLogs, 0, this.scanPeriod, TimeUnit.MILLISECONDS);
        started = true;
    }

    /**
     * Shut down the CDC executor.
     */
    public void stopCDCReader()
    {
        logger.info("Stopping CDC...");
        cdcExecutor.shutdown();
        started = false;
    }

    /**
     * Truncates CDC data.  This deletes all CDC logs except for the current active commitlog.
     */
    public void truncateCDCData()
    {
        logger.info("Truncating CDC data...");

        try
        {
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).list())
            {
                CommitLogDescriptor commitLog = CommitLogDescriptor.fromFileName(f.name());
                // delete all CDC commitlog hard links except the one that is the currently allocating
                if (commitLog.id != CommitLog.instance.getCurrentPosition().segmentId)
                    FileUtils.deleteWithConfirm(f, null);
                else
                    logger.info("Not deleting CDC hard link {} because it's currently allocating.", f.name());
            }
        } catch (Exception e) {
            logger.error("truncateCDCData Exception");
        }
    }

    /**
     * Stops worker threads.
     */
    public void stopWorkers()
    {
        logger.info("Stopping workers...");
        for (Map.Entry<Long, CDCReaderStatus> entry : readerStatuses.entrySet())
        {
            logger.info("Canceling worker thread of commitLog.id={}...", entry.getKey());
            entry.getValue().cancel();
        }
    }

    /**
     * Sets timeout in ms for worker threads.  Does not affect already running threads.
     */
    public void setReaderTimeout(long timeout)
    {
        readerTimeout = timeout;
        logger.info("Updated reader timeout to {} ms", timeout);
    }

    /**
     * Gets timeout in ms for worker threads.
     */
    public long getReaderTimeout()
    {
        return readerTimeout;
    }

    /**
     * Sets scan period in ms.  Does not take effect until restarting CDC.
     */
    public void setScanPeriod(long scanPeriod)
    {
        this.scanPeriod = scanPeriod;
        logger.info("Updated scan period to {} ms", scanPeriod);
    }

    /**
     * Gets scan period in ms.
     */
    public long getScanPeriod()
    {
        return scanPeriod;
    }

    /**
     * Updates CDC total size.
     */
    public static void updateCDCTotalSize()
    {
        try
        {
            ((CommitLogSegmentManagerCDC) CommitLog.instance.segmentManager).updateCDCTotalSize();
        }
        catch (ClassCastException e)
        {
            logger.error("Unable to update CDC total size", e);
        }
    }

    /**
     * Sets the core thread size.
     */
    public void setCoreCDCThreads(int size)
    {
        workerExecutor.setCorePoolSize(size);
    }

    /**
     * Gets the core thread size.
     */
    public int getCoreCDCThreads()
    {
        return workerExecutor.getCorePoolSize();
    }
}
