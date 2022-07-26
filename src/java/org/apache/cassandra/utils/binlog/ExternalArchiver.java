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

import java.io.File; // checkstyle: permit this import
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Archives binary log files immediately when they are rolled using a configure archive command.
 *
 * The archive command should be "/path/to/script.sh %path" where %path will be replaced with the file to be archived
 */
public class ExternalArchiver implements BinLogArchiver
{
    private static final Logger logger = LoggerFactory.getLogger(ExternalArchiver.class);
    // used to replace %path with the actual file to archive when calling the archive command
    private static final Pattern PATH = Pattern.compile("%path");
    private static final long DEFAULT_RETRY_DELAY_MS = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    /**
     * use a DelayQueue to simplify retries - we want first tries to be executed immediately and retries should wait DEFAULT_RETRY_DELAY_MS
     */
    private final DelayQueue<DelayFile> archiveQueue = new DelayQueue<>();
    private final String archiveCommand;
    private final ExecutorService executor = executorFactory().sequential("BinLogArchiver");
    private final Path path;
    /**
     * for testing, to be able to make sure that the command is executed
     */
    private final ExecCommand commandExecutor;
    private volatile boolean shouldContinue = true;

    public ExternalArchiver(String archiveCommand, Path path, int maxArchiveRetries)
    {
        this(archiveCommand, path, DEFAULT_RETRY_DELAY_MS, maxArchiveRetries, ExternalArchiver::exec);
    }

    @VisibleForTesting
    ExternalArchiver(String archiveCommand, Path path, long retryDelayMs, int maxRetries, ExecCommand command)
    {
        this.archiveCommand = archiveCommand;
        this.commandExecutor = command;
        // if there are any .cq4 files in path, archive them on startup - this handles any leftover files from crashes etc
        archiveExisting(path);
        this.path = path;

        executor.execute(() -> {
           while (shouldContinue)
           {
               DelayFile toArchive = null;
               try
               {
                   toArchive = archiveQueue.poll(100, TimeUnit.MILLISECONDS);
                   if (toArchive != null)
                       archiveFile(toArchive.file);
               }
               catch (Throwable t)
               {
                   if (toArchive != null)
                   {

                       if (toArchive.retries < maxRetries)
                       {
                           logger.error("Got error archiving {}, retrying in {} minutes", toArchive.file, TimeUnit.MINUTES.convert(retryDelayMs, TimeUnit.MILLISECONDS), t);
                           archiveQueue.add(new DelayFile(toArchive.file, retryDelayMs, TimeUnit.MILLISECONDS, toArchive.retries + 1));
                       }
                       else
                       {
                           logger.error("Max retries {} reached for {}, leaving on disk", toArchive.retries, toArchive.file, t);
                       }
                   }
                   else
                       logger.error("Got error waiting for files to archive", t);
               }
           }
           logger.debug("Exiting archiver thread");
        });
    }

    public void onReleased(int cycle, File file)
    {
        logger.debug("BinLog file released: {}", file);
        archiveQueue.add(new DelayFile(file, 0, TimeUnit.MILLISECONDS, 0));
    }

    /**
     * Stops the archiver thread and tries to archive all existing files
     *
     * this handles the case where a user explicitly disables full/audit log and would expect all log files to be archived
     * rolled or not
     */
    public void stop()
    {
        shouldContinue = false;
        try
        {
            // wait for the archiver thread to stop;
            executor.submit(() -> {}).get();
            // and try to archive all remaining files before exiting
            archiveExisting(path);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Iterates over all files in path, executing the archive command for each.
     */
    private void archiveExisting(Path path)
    {
        if (path == null)
            return;
        for (File f : path.toFile().listFiles((f) -> f.isFile() && f.getName().endsWith(SingleChronicleQueue.SUFFIX))) // checkstyle: permit this invocation
        {
            try
            {
                logger.debug("Archiving existing file {}", f);
                archiveFile(f);
            }
            catch (IOException e)
            {
                logger.error("Got error archiving existing file {}", f, e);
            }
        }
    }

    private void archiveFile(File f) throws IOException
    {
        String cmd = PATH.matcher(archiveCommand).replaceAll(Matcher.quoteReplacement(f.getAbsolutePath()));
        logger.debug("Executing archive command: {}", cmd);
        commandExecutor.exec(cmd);
    }

    static void exec(String command) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder(command.split(" "));
        pb.redirectErrorStream(true);
        FBUtilities.exec(pb);
    }

    private static class DelayFile implements Delayed
    {
        public final File file;
        private final long delayTime;
        private final int retries;

        public DelayFile(File file, long delay, TimeUnit delayUnit, int retries)
        {
            this.file = file;
            this.delayTime = currentTimeMillis() + MILLISECONDS.convert(delay, delayUnit);
            this.retries = retries;
        }
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(delayTime - currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed o)
        {
            DelayFile other = (DelayFile)o;
            return Longs.compare(delayTime, other.delayTime);
        }
    }

    interface ExecCommand
    {
        public void exec(String command) throws IOException;
    }
}
