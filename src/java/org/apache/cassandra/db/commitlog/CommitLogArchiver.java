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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class CommitLogArchiver
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogArchiver.class);
    public static final SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
    private static final String DELIMITER = ",";
    private static final Pattern NAME = Pattern.compile("%name");
    private static final Pattern PATH = Pattern.compile("%path");
    private static final Pattern FROM = Pattern.compile("%from");
    private static final Pattern TO = Pattern.compile("%to");
    static
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5909
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public final Map<String, Future<?>> archivePending = new ConcurrentHashMap<String, Future<?>>();
    private final ExecutorService executor;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8734
    final String archiveCommand;
    final String restoreCommand;
    final String restoreDirectories;
    public long restorePointInTime;
    public final TimeUnit precision;

    public CommitLogArchiver(String archiveCommand, String restoreCommand, String restoreDirectories,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10049
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10049
            long restorePointInTime, TimeUnit precision)
    {
        this.archiveCommand = archiveCommand;
        this.restoreCommand = restoreCommand;
        this.restoreDirectories = restoreDirectories;
        this.restorePointInTime = restorePointInTime;
        this.precision = precision;
        executor = !Strings.isNullOrEmpty(archiveCommand) ? new JMXEnabledThreadPoolExecutor("CommitLogArchiver") : null;
    }

    public static CommitLogArchiver disabled()
    {
        return new CommitLogArchiver(null, null, null, Long.MAX_VALUE, TimeUnit.MICROSECONDS);
    }

    public static CommitLogArchiver construct()
    {
        Properties commitlog_commands = new Properties();
        try (InputStream stream = CommitLogArchiver.class.getClassLoader().getResourceAsStream("commitlog_archiving.properties"))
        {
            if (stream == null)
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10241
                logger.trace("No commitlog_archiving properties found; archive + pitr will be disabled");
                return disabled();
            }
            else
            {
                commitlog_commands.load(stream);
                String archiveCommand = commitlog_commands.getProperty("archive_command");
                String restoreCommand = commitlog_commands.getProperty("restore_command");
                String restoreDirectories = commitlog_commands.getProperty("restore_directories");
                if (restoreDirectories != null && !restoreDirectories.isEmpty())
                {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8111
                    for (String dir : restoreDirectories.split(DELIMITER))
                    {
                        File directory = new File(dir);
                        if (!directory.exists())
                        {
                            if (!directory.mkdir())
                            {
                                throw new RuntimeException("Unable to create directory: " + dir);
                            }
                        }
                    }
                }
                String targetTime = commitlog_commands.getProperty("restore_point_in_time");
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10049
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10049
                TimeUnit precision = TimeUnit.valueOf(commitlog_commands.getProperty("precision", "MICROSECONDS"));
                long restorePointInTime;
                try
                {
                    restorePointInTime = Strings.isNullOrEmpty(targetTime) ? Long.MAX_VALUE : format.parse(targetTime).getTime();
                }
                catch (ParseException e)
                {
                    throw new RuntimeException("Unable to parse restore target time", e);
                }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10049
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10049
                return new CommitLogArchiver(archiveCommand, restoreCommand, restoreDirectories, restorePointInTime, precision);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to load commitlog_archiving.properties", e);
        }

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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8755
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
     *
     * Used to archive files present in the commit log directory at startup (CASSANDRA-6904).
     * Since the files being already archived by normal operation could cause subsequent
     * hard-linking or other operations to fail, we should not throw errors on failure
     */
    public void maybeArchive(final String path, final String name)
    {
        if (Strings.isNullOrEmpty(archiveCommand))
            return;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10202
        archivePending.put(name, executor.submit(new Runnable()
        {
            public void run()
            {
                try
                {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8755
                    String command = NAME.matcher(archiveCommand).replaceAll(Matcher.quoteReplacement(name));
                    command = PATH.matcher(command).replaceAll(Matcher.quoteReplacement(path));
                    exec(command);
                }
                catch (IOException e)
                {
                    logger.warn("Archiving file {} failed, file may have already been archived.", name, e);
                }
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
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10593
            if (e.getCause() instanceof RuntimeException)
            {
                if (e.getCause().getCause() instanceof IOException)
                {
                    logger.error("Looks like the archiving of file {} failed earlier, cassandra is going to ignore this segment for now.", name, e.getCause().getCause());
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

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8111
        for (String dir : restoreDirectories.split(DELIMITER))
        {
            File[] files = new File(dir).listFiles();
            if (files == null)
            {
                throw new RuntimeException("Unable to list directory " + dir);
            }
            for (File fromFile : files)
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6018
                CommitLogDescriptor fromHeader = CommitLogDescriptor.fromHeader(fromFile, DatabaseDescriptor.getEncryptionContext());
                CommitLogDescriptor fromName = CommitLogDescriptor.isValid(fromFile.getName()) ? CommitLogDescriptor.fromFileName(fromFile.getName()) : null;
                CommitLogDescriptor descriptor;
                if (fromHeader == null && fromName == null)
                    throw new IllegalStateException("Cannot safely construct descriptor for segment, either from its name or its header: " + fromFile.getPath());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6809
                else if (fromHeader != null && fromName != null && !fromHeader.equalsIgnoringCompression(fromName))
                    throw new IllegalStateException(String.format("Cannot safely construct descriptor for segment, as name and header descriptors do not match (%s vs %s): %s", fromHeader, fromName, fromFile.getPath()));
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12716
                else if (fromName != null && fromHeader == null)
                    throw new IllegalStateException("Cannot safely construct descriptor for segment, as name descriptor implies a version that should contain a header descriptor, but that descriptor could not be read: " + fromFile.getPath());
                else if (fromHeader != null)
                    descriptor = fromHeader;
                else descriptor = fromName;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
                if (descriptor.version > CommitLogDescriptor.current_version)
                    throw new IllegalStateException("Unsupported commit log version: " + descriptor.version);

                if (descriptor.compression != null)
                {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6809
                    try
                    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9712
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10241
                    logger.trace("Skipping restore of archive {} as the segment already exists in the restore location {}",
                                 fromFile.getPath(), toFile.getPath());
                    continue;
                }

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8755
                String command = FROM.matcher(restoreCommand).replaceAll(Matcher.quoteReplacement(fromFile.getPath()));
                command = TO.matcher(command).replaceAll(Matcher.quoteReplacement(toFile.getPath()));
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
}
