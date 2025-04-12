/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class CommitLogArchiver
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogArchiver.class);

    public static final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss[.[SSSSSS][SSS]]").withZone(ZoneId.of("GMT"));
    private static final String COMMITLOG_ARCHIVNG_PROPERTIES_FILE_NAME = "commitlog_archiving.properties";
    private static final String DELIMITER = ",";
    private static final Pattern NAME = Pattern.compile("%name");
    private static final Pattern PATH = Pattern.compile("%path");
    private static final Pattern FROM = Pattern.compile("%from");
    private static final Pattern TO = Pattern.compile("%to");

    public final Map<String, Future<?>> archivePending = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    final String archiveCommand;
    final String restoreCommand;
    final String restoreDirectories;
    TimeUnit precision;
    long restorePointInTimeInMicroseconds;
    final CommitLogPosition snapshotCommitLogPosition;

    public CommitLogArchiver(String archiveCommand,
                             String restoreCommand,
                             String restoreDirectories,
                             long restorePointInTimeInMicroseconds,
                             CommitLogPosition snapshotCommitLogPosition,
                             TimeUnit precision)
    {
        this.archiveCommand = archiveCommand;
        this.restoreCommand = restoreCommand;
        this.restoreDirectories = restoreDirectories;
        this.restorePointInTimeInMicroseconds = restorePointInTimeInMicroseconds;
        this.snapshotCommitLogPosition = snapshotCommitLogPosition;
        this.precision = precision;
        executor = !Strings.isNullOrEmpty(archiveCommand)
                   ? executorFactory()
                     .withJmxInternal()
                     .sequential("CommitLogArchiver")
                   : null;
    }

    public static CommitLogArchiver disabled()
    {
        return new CommitLogArchiver(null, null, null, Long.MAX_VALUE, CommitLogPosition.NONE, TimeUnit.MICROSECONDS);
    }

    public static CommitLogArchiver construct()
    {
        Properties commitlogProperties = new Properties();
        try (InputStream stream = CommitLogArchiver.class.getClassLoader().getResourceAsStream(COMMITLOG_ARCHIVNG_PROPERTIES_FILE_NAME))
        {
            if (stream == null)
            {
                logger.trace("No {} found; archiving and point-in-time-restoration will be disabled", COMMITLOG_ARCHIVNG_PROPERTIES_FILE_NAME);
                return disabled();
            }
            else
            {
                commitlogProperties.load(stream);
                return getArchiverFromProperties(commitlogProperties);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to load " + COMMITLOG_ARCHIVNG_PROPERTIES_FILE_NAME, e);
        }
    }

    @VisibleForTesting
    static CommitLogArchiver getArchiverFromProperties(Properties commitlogCommands)
    {
        assert !commitlogCommands.isEmpty();
        String archiveCommand = commitlogCommands.getProperty("archive_command");
        String restoreCommand = commitlogCommands.getProperty("restore_command");
        String restoreDirectories = commitlogCommands.getProperty("restore_directories");
        if (restoreDirectories != null && !restoreDirectories.isEmpty())
        {
            for (String dir : restoreDirectories.split(DELIMITER))
            {
                File directory = new File(dir);
                if (!directory.exists())
                {
                    if (!directory.tryCreateDirectory())
                    {
                        throw new RuntimeException("Unable to create directory: " + dir);
                    }
                }
            }
        }

        String precisionPropertyValue = commitlogCommands.getProperty("precision", TimeUnit.MICROSECONDS.name());
        TimeUnit precision;
        try
        {
            precision = TimeUnit.valueOf(precisionPropertyValue);
        }
        catch (IllegalArgumentException ex)
        {
            throw new RuntimeException("Unable to parse precision of value " + precisionPropertyValue, ex);
        }
        if (precision == TimeUnit.NANOSECONDS)
            throw new RuntimeException("NANOSECONDS level precision is not supported.");

        String targetTime = commitlogCommands.getProperty("restore_point_in_time");
        long restorePointInTime = Long.MAX_VALUE;
        try
        {
            if (!Strings.isNullOrEmpty(targetTime))
            {
                // get restorePointInTime in microseconds level by default as cassandra use this level's timestamp
                restorePointInTime = getRestorationPointInTimeInMicroseconds(targetTime);
            }
        }
        catch (DateTimeParseException e)
        {
            throw new RuntimeException("Unable to parse restore target time", e);
        }

        String snapshotPosition = commitlogCommands.getProperty("snapshot_commitlog_position");
        CommitLogPosition snapshotCommitLogPosition;
        try
        {

            snapshotCommitLogPosition = Strings.isNullOrEmpty(snapshotPosition)
                                        ? CommitLogPosition.NONE
                                        : CommitLogPosition.serializer.fromString(snapshotPosition);
        }
        catch (ParseException | NumberFormatException e)
        {
            throw new RuntimeException("Unable to parse snapshot commit log position", e);
        }

        return new CommitLogArchiver(archiveCommand,
                                     restoreCommand,
                                     restoreDirectories,
                                     restorePointInTime,
                                     snapshotCommitLogPosition,
                                     precision);
    }

    public void maybeArchive(final CommitLogSegment segment)
    {
        if (Strings.isNullOrEmpty(archiveCommand))
            return;

        archivePending.put(segment.getName(), executor.submit(new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
            {
                segment.waitForFinalSync();
                String command = NAME.matcher(archiveCommand).replaceAll(Matcher.quoteReplacement(segment.getName()));
                command = PATH.matcher(command).replaceAll(Matcher.quoteReplacement(segment.getPath()));
                exec(command);
            }
        }));
    }

    /**
     * Differs from the above because it can be used on any file, rather than only
     * managed commit log segments (and thus cannot call waitForFinalSync), and in
     * the treatment of failures.
     * <p>
     * Used to archive files present in the commit log directory at startup (CASSANDRA-6904).
     * Since the files being already archived by normal operation could cause subsequent
     * hard-linking or other operations to fail, we should not throw errors on failure
     */
    public void maybeArchive(final String path, final String name)
    {
        if (Strings.isNullOrEmpty(archiveCommand))
            return;

        archivePending.put(name, executor.submit(() -> {
            try
            {
                String command = NAME.matcher(archiveCommand).replaceAll(Matcher.quoteReplacement(name));
                command = PATH.matcher(command).replaceAll(Matcher.quoteReplacement(path));
                exec(command);
            }
            catch (IOException e)
            {
                logger.warn("Archiving file {} failed, file may have already been archived.", name, e);
            }
        }));
    }

    public boolean maybeWaitForArchiving(String name)
    {
        Future<?> f = archivePending.remove(name);
        if (f == null)
            return true; // archiving disabled

        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof RuntimeException)
            {
                if (e.getCause().getCause() instanceof IOException)
                {
                    logger.error("Looks like the archiving of file {} failed earlier, Cassandra is going to ignore this segment for now.", name, e.getCause().getCause());
                    return false;
                }
            }
            throw new RuntimeException(e);
        }

        return true;
    }

    public void maybeRestoreArchive()
    {
        if (Strings.isNullOrEmpty(restoreDirectories))
            return;

        for (String dir : restoreDirectories.split(DELIMITER))
        {
            File[] files = new File(dir).tryList();
            if (files == null)
            {
                throw new RuntimeException("Unable to list directory " + dir);
            }
            for (File fromFile : files)
            {
                CommitLogDescriptor fromHeader = CommitLogDescriptor.fromHeader(fromFile, DatabaseDescriptor.getEncryptionContext());
                CommitLogDescriptor fromName = CommitLogDescriptor.isValid(fromFile.name()) ? CommitLogDescriptor.fromFileName(fromFile.name()) : null;
                CommitLogDescriptor descriptor;
                if (fromHeader == null && fromName == null)
                    throw new IllegalStateException("Cannot safely construct descriptor for segment, either from its name or its header: " + fromFile.path());
                else if (fromHeader != null && fromName != null && !fromHeader.equalsIgnoringCompression(fromName))
                    throw new IllegalStateException(String.format("Cannot safely construct descriptor for segment, as name and header descriptors do not match (%s vs %s): %s", fromHeader, fromName, fromFile.path()));
                else if (fromName != null && fromHeader == null)
                    throw new IllegalStateException("Cannot safely construct descriptor for segment, as name descriptor implies a version that should contain a header descriptor, but that descriptor could not be read: " + fromFile.path());
                else if (fromHeader != null)
                    descriptor = fromHeader;
                else descriptor = fromName;

                if (descriptor.version > CommitLogDescriptor.current_version)
                    throw new IllegalStateException("Unsupported commit log version: " + descriptor.version);

                if (descriptor.compression != null)
                {
                    try
                    {
                        CompressionParams.createCompressor(descriptor.compression);
                    }
                    catch (ConfigurationException e)
                    {
                        throw new IllegalStateException("Unknown compression", e);
                    }
                }

                File toFile = new File(DatabaseDescriptor.getCommitLogLocation(), descriptor.fileName());
                if (toFile.exists())
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Skipping restore of archive {} as the segment already exists in the restore location {}",
                                     fromFile.path(), toFile.path());
                    continue;
                }

                String command = FROM.matcher(restoreCommand).replaceAll(Matcher.quoteReplacement(fromFile.path()));
                command = TO.matcher(command).replaceAll(Matcher.quoteReplacement(toFile.path()));
                try
                {
                    exec(command);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void exec(String command) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder(command.split(" "));
        pb.redirectErrorStream(true);
        FBUtilities.exec(pb);
    }

    /**
     * We change the restore_point_in_time from configuration file into microseconds level as Cassandra use microseconds
     * as the timestamp.
     *
     * @param restorationPointInTime value of "restore_point_in_time" in properties file.
     * @return microseconds value of restore_point_in_time
     */
    @VisibleForTesting
    public static long getRestorationPointInTimeInMicroseconds(String restorationPointInTime)
    {
        assert !Strings.isNullOrEmpty(restorationPointInTime) : "restore_point_in_time is null or empty!";
        Instant instant = format.parse(restorationPointInTime, Instant::from);
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;
    }

    public long getRestorePointInTimeInMicroseconds()
    {
        return this.restorePointInTimeInMicroseconds;
    }

    @VisibleForTesting
    public void setRestorePointInTimeInMicroseconds(long restorePointInTimeInMicroseconds)
    {
        this.restorePointInTimeInMicroseconds = restorePointInTimeInMicroseconds;
    }

    @VisibleForTesting
    public void setPrecision(TimeUnit timeUnit)
    {
        this.precision = timeUnit;
    }
}