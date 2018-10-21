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

package org.apache.cassandra.audit;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.RollCycles;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.binlog.BinLogArchiver;
import org.apache.cassandra.utils.binlog.DeletingArchiver;
import org.apache.cassandra.utils.binlog.ExternalArchiver;

abstract class BinLogAuditLogger implements IAuditLogger
{
    protected static final Logger logger = LoggerFactory.getLogger(BinLogAuditLogger.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final NoSpamLogger.NoSpamLogStatement droppedSamplesStatement = noSpamLogger.getStatement("Dropped {} binary log samples", 1, TimeUnit.MINUTES);

    volatile BinLog binLog;
    protected volatile boolean blocking;
    protected Path path;

    private final AtomicLong droppedSamplesSinceLastLog = new AtomicLong();

    /**
     * Configure the global instance of the FullQueryLogger. Clean the provided directory before starting
     * @param path Dedicated path where the FQL can store it's files.
     * @param rollCycle How often to roll FQL log segments so they can potentially be reclaimed
     * @param blocking Whether the FQL should block if the FQL falls behind or should drop log records
     * @param maxQueueWeight Maximum weight of in memory queue for records waiting to be written to the file before blocking or dropping
     * @param maxLogSize Maximum size of the rolled files to retain on disk before deleting the oldest file
     * @param archiveCommand the archive command to execute on rolled log files
     * @param maxArchiveRetries max number of retries of failed archive commands
     */
    public synchronized void configure(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize, String archiveCommand, int maxArchiveRetries)
    {
        this.configure(path, rollCycle, blocking, maxQueueWeight, maxLogSize, true, archiveCommand, maxArchiveRetries);
    }

    /**
     * Configure the global instance of the FullQueryLogger
     * @param path Dedicated path where the FQL can store it's files.
     * @param rollCycle How often to roll FQL log segments so they can potentially be reclaimed
     * @param blocking Whether the FQL should block if the FQL falls behind or should drop log records
     * @param maxQueueWeight Maximum weight of in memory queue for records waiting to be written to the file before blocking or dropping
     * @param maxLogSize Maximum size of the rolled files to retain on disk before deleting the oldest file
     * @param cleanDirectory Indicates to clean the directory before starting FullQueryLogger or not
     * @param archiveCommand the archive command to execute on rolled log files
     * @param maxArchiveRetries max number of retries of failed archive commands
     */
    public synchronized void configure(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize, boolean cleanDirectory, String archiveCommand, int maxArchiveRetries)
    {
        Preconditions.checkNotNull(path, "path was null");
        File pathAsFile = path.toFile();
        Preconditions.checkNotNull(rollCycle, "rollCycle was null");
        rollCycle = rollCycle.toUpperCase();

        //Exists and is a directory or can be created
        Preconditions.checkArgument((pathAsFile.exists() && pathAsFile.isDirectory()) || (!pathAsFile.exists() && pathAsFile.mkdirs()), "path exists and is not a directory or couldn't be created");
        Preconditions.checkArgument(pathAsFile.canRead() && pathAsFile.canWrite() && pathAsFile.canExecute(), "path is not readable, writable, and executable");
        Preconditions.checkNotNull(RollCycles.valueOf(rollCycle), "unrecognized roll cycle");
        Preconditions.checkArgument(maxQueueWeight > 0, "maxQueueWeight must be > 0");
        Preconditions.checkArgument(maxLogSize > 0, "maxLogSize must be > 0");
        logger.info("Attempting to configure full query logger path: {} Roll cycle: {} Blocking: {} Max queue weight: {} Max log size:{}, archive command: {}", path, rollCycle, blocking, maxQueueWeight, maxLogSize, archiveCommand);

        if (binLog != null)
        {
            logger.warn("Full query logger already configured. Ignoring requested configuration.");
            throw new IllegalStateException("Already configured");
        }

        // create the archiver before cleaning directories - ExternalArchiver will try to archive any existing file.
        BinLogArchiver archiver = Strings.isNullOrEmpty(archiveCommand) ? new DeletingArchiver(maxLogSize) : new ExternalArchiver(archiveCommand, path, maxArchiveRetries);
        if (cleanDirectory)
        {
            logger.info("Cleaning directory: {} as requested",path);
            if (path.toFile().exists())
            {
                Throwable error = cleanDirectory(path.toFile(), null);
                if (error != null)
                {
                    throw new RuntimeException(error);
                }
            }
        }
        this.path = path;
        this.blocking = blocking;
        binLog = new BinLog(path, RollCycles.valueOf(rollCycle), maxQueueWeight, archiver);
        binLog.start();
    }

    public Path path()
    {
        return path;
    }

    /**
     * Need the path as a parameter as well because if the process is restarted the config file might be the only
     * location for retrieving the path to the full query log files, but JMX also allows you to specify a path
     * that isn't persisted anywhere so we have to clean that one a well.
     */
    public synchronized void reset(String fullQueryLogPath)
    {
        try
        {
            Set<File> pathsToClean = Sets.newHashSet();

            //First decide whether to clean the path configured in the YAML
            if (fullQueryLogPath != null)
            {
                File fullQueryLogPathFile = new File(fullQueryLogPath);
                if (fullQueryLogPathFile.exists())
                {
                    pathsToClean.add(fullQueryLogPathFile);
                }
            }

            //Then decide whether to clean the last used path, possibly configured by JMX
            if (path != null)
            {
                File pathFile = path.toFile();
                if (pathFile.exists())
                {
                    pathsToClean.add(pathFile);
                }
            }

            logger.info("Reset (and deactivation) of full query log requested.");
            if (binLog != null)
            {
                logger.info("Stopping full query log. Cleaning {}.", pathsToClean);
                binLog.stop();
                binLog = null;
            }
            else
            {
                logger.info("Full query log already deactivated. Cleaning {}.", pathsToClean);
            }

            Throwable accumulate = null;
            for (File f : pathsToClean)
            {
                accumulate = cleanDirectory(f, accumulate);
            }
            if (accumulate != null)
            {
                throw new RuntimeException(accumulate);
            }
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException)e;
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Stop the full query log leaving behind any generated files.
     */
    public synchronized void stop()
    {
        try
        {
            logger.info("Deactivation of full query log requested.");
            if (binLog != null)
            {
                logger.info("Stopping full query log");
                binLog.stop();
                binLog = null;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check whether the full query log is enabled.
     * @return true if records are recorded and false otherwise.
     */
    public boolean enabled()
    {
        return binLog != null;
    }

    void logRecord(BinLog.ReleaseableWriteMarshallable record, BinLog binLog)
    {
        boolean putInQueue = false;
        try
        {
            if (blocking)
            {
                try
                {
                    binLog.put(record);
                    putInQueue = true;
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            else
            {
                if (!binLog.offer(record))
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

    private static Throwable cleanDirectory(File directory, Throwable accumulate)
    {
        if (!directory.exists())
        {
            return Throwables.merge(accumulate, new RuntimeException(String.format("%s does not exists", directory)));
        }
        if (!directory.isDirectory())
        {
            return Throwables.merge(accumulate, new RuntimeException(String.format("%s is not a directory", directory)));
        }
        for (File f : directory.listFiles())
        {
            accumulate = deleteRecursively(f, accumulate);
        }
        if (accumulate instanceof FSError)
        {
            FileUtils.handleFSError((FSError)accumulate);
        }
        return accumulate;
    }

    private static Throwable deleteRecursively(File fileOrDirectory, Throwable accumulate)
    {
        if (fileOrDirectory.isDirectory())
        {
            for (File f : fileOrDirectory.listFiles())
            {
                accumulate = FileUtils.deleteWithConfirm(f, true, accumulate);
            }
        }
        return FileUtils.deleteWithConfirm(fileOrDirectory, true , accumulate);
    }
}
