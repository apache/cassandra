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

package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.management.StandardMBean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.profiler.AsyncProfiler;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.profiler.AsyncProfilerMBean;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_UNSAFE_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG_DIR;

public class AsyncProfilerService implements AsyncProfilerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncProfilerService.class);

    private static final EnumSet<AsyncProfilerEvent> VALID_EVENTS = EnumSet.allOf(AsyncProfilerEvent.class);
    private static final EnumSet<AsyncProfilerFormat> VALID_FORMATS = EnumSet.allOf(AsyncProfilerFormat.class);
    private static final Pattern VALID_FILENAME_REGEX_PATTERN = Pattern.compile("^[a-zA-Z0-9-]*\\.?[a-zA-Z0-9-]*$");
    private static final int MAX_SAFE_PROFILING_DURATION = 43200; // 12 hours
    private static final String ASYNC_PROFILER_LOG_DIR = Path.of(LOG_DIR.getString(), "profiler").toAbsolutePath().toString();

    public static final String ASYNC_PROFILER_START_EVENTS_PARAM = "events";
    public static final String ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM = "outputFormat";
    public static final String ASYNC_PROFILER_START_DURATION_PARAM = "duration";
    public static final String ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM = "outputFileName";
    public static final Set<String> ASYNC_PROFILER_START_PARAMS = Set.of(ASYNC_PROFILER_START_EVENTS_PARAM,
                                                                          ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM,
                                                                          ASYNC_PROFILER_START_DURATION_PARAM,
                                                                          ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM);

    public static final String ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM = "outputFileName";
    public static final Set<String> ASYNC_PROFILER_STOP_PARAMS = Set.of(ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM);

    private static AsyncProfilerService instance;
    private static AsyncProfiler asyncProfiler;
    private final boolean unsafeMode;
    private static String logDir;
    private final AtomicReference<File> currentResultFile = new AtomicReference<>();
    private final StartupChecks.AsyncProfilerKernelParamsCheck kernelParamsCheck;

    @VisibleForTesting
    public static synchronized AsyncProfilerService instance(String logDir, boolean registerMBean)
    {
        return instance(logDir, registerMBean, new StartupChecks.AsyncProfilerKernelParamsCheck());
    }

    @VisibleForTesting
    public static synchronized AsyncProfilerService instance(String logDir, boolean registerMBean, StartupChecks.AsyncProfilerKernelParamsCheck kernelParamsCheck)
    {
        AsyncProfilerService.logDir = logDir;
        if (instance == null)
        {
            try
            {
                instance = new AsyncProfilerService(ASYNC_PROFILER_UNSAFE_MODE.getBoolean(), kernelParamsCheck);

                if (registerMBean)
                {
                    MBeanWrapper.instance.registerMBean(new StandardMBean(AsyncProfilerService.instance, AsyncProfilerMBean.class),
                                                        AsyncProfilerService.MBEAN_NAME,
                                                        MBeanWrapper.OnException.LOG);
                }

                try
                {
                    maybeCreateProfilesLogDir();
                }
                catch (Throwable t)
                {
                    throw new ConfigurationException(t.getMessage());
                }

                if (ASYNC_PROFILER_ENABLED.getBoolean())
                    asyncProfiler = instance.getProfiler();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }
        return AsyncProfilerService.instance;
    }

    public static synchronized AsyncProfilerService instance()
    {
        if (instance == null)
            return instance(ASYNC_PROFILER_LOG_DIR, true);
        else
            return instance;
    }

    public AsyncProfilerService(boolean unsafeMode)
    {
        this(unsafeMode, null);
    }

    public AsyncProfilerService(boolean unsafeMode, StartupChecks.AsyncProfilerKernelParamsCheck kernelParamsCheck)
    {
        this.unsafeMode = unsafeMode;
        this.kernelParamsCheck = kernelParamsCheck;
    }

    public enum AsyncProfilerEvent
    {
        cpu("cpu"),
        alloc("alloc"),
        lock("lock"),
        wall("wall"),
        nativemem("nativemem"),
        cache_misses("cache-misses");

        private final String name;

        AsyncProfilerEvent(String name)
        {
            this.name = name;
        }

        public String getEvent()
        {
            return name;
        }

        public static String parseEvents(String rawString)
        {
            if (rawString == null || rawString.isBlank())
                throw new IllegalArgumentException("Event can not be null nor blank string.");

            try
            {
                List<String> processedEvents = new ArrayList<>();
                for (String rawEvent : rawString.split(","))
                    processedEvents.add(AsyncProfilerEvent.valueOf(rawEvent).getEvent());

                return String.join(",", processedEvents);
            }
            catch (IllegalArgumentException ex)
            {
                throw new IllegalArgumentException(format("Event must be one or a combination of %s", VALID_EVENTS));
            }
        }
    }

    public enum AsyncProfilerFormat
    {
        flat, traces, collapsed, flamegraph, tree, jfr;

        public static String parseFormat(String rawFormat)
        {
            if (rawFormat == null || rawFormat.isBlank())
                throw new IllegalArgumentException("Event can not be null nor blank string.");

            try
            {
                return AsyncProfilerFormat.valueOf(rawFormat).name();
            }
            catch (IllegalArgumentException ex)
            {
                throw new IllegalArgumentException(format("Format must be one of %s", VALID_FORMATS));
            }
        }
    }

    @Override
    public synchronized boolean start(Map<String, String> parameters)
    {
        if (isRunning())
            return false;

        validateStartParameters(parameters);

        try
        {
            run(new ThrowingFunction<>()
            {
                @Override
                public Object apply(AsyncProfiler profiler) throws Throwable
                {
                    maybeCreateProfilesLogDir();
                    kernelParamsCheck.execute(null, true);

                    String parsedFormat = AsyncProfilerFormat.parseFormat(parameters.get(ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM));
                    String parsedEvents = AsyncProfilerEvent.parseEvents(parameters.get(ASYNC_PROFILER_START_EVENTS_PARAM));
                    File file = new File(logDir, validateOutputFileName(parameters.get(ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM)));

                    String cmd = format("start,%s,event=%s,timeout=%s,file=%s",
                                        parsedFormat,
                                        parsedEvents,
                                        parseDuration(parameters.get(ASYNC_PROFILER_START_DURATION_PARAM)),
                                        file);

                    currentResultFile.set(file);

                    String result = profiler.execute(cmd);
                    logger.info("Started Async-Profiler: result={}, cmd={}", result, cmd);

                    return null;
                }
            });
            return true;
        }
        catch (IllegalStateException | IllegalArgumentException ex)
        {
            throw ex;
        }
        catch (Throwable t)
        {
            logger.error("Failed to start Async-Profiler", t);
            return false;
        }
    }

    @Override
    public synchronized boolean stop(Map<String, String> parameters)
    {
        if (!isRunning())
            return false;

        validateStopParameters(parameters);

        try
        {
            run(new ThrowingFunction<>()
            {
                @Override
                public Object apply(AsyncProfiler profiler) throws Throwable
                {
                    maybeCreateProfilesLogDir();
                    File resolvedOutputFile;
                    String cmd = "stop";
                    String outputFileName = parameters.get(ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM);
                    if (outputFileName != null)
                        resolvedOutputFile = new File(logDir, validateOutputFileName(outputFileName));
                    else
                        resolvedOutputFile = currentResultFile.get();

                    cmd += ",file=" + resolvedOutputFile.absolutePath();

                    String result = profiler.execute(cmd);
                    logger.debug("Stopped Async-Profiler: result={}, cmd={}", result, cmd);

                    currentResultFile.set(null);

                    return null;
                }
            });
            return true;
        }
        catch (IllegalStateException | IllegalArgumentException e)
        {
            throw e;
        }
        catch (Throwable e)
        {
            logger.error("Failed to stop Async-Profiler", e);
            return false;
        }
    }

    @Override
    public String execute(String command)
    {
        if (!unsafeMode)
        {
            throw new SecurityException(String.format("The arbitrary command execution is not permitted " +
                                                      "with %s MBean. If unsafe command execution is required, " +
                                                      "start Cassandra with %s property set to true. " +
                                                      "Rejected command: %s",
                                                      AsyncProfilerService.MBEAN_NAME,
                                                      CassandraRelevantProperties.ASYNC_PROFILER_UNSAFE_MODE.getKey(), command));
        }

        return run(new ThrowingFunction<>()
        {
            @Override
            public String apply(AsyncProfiler profiler) throws Throwable
            {
                return profiler.execute(validateCommand(command));
            }
        });
    }

    @Override
    public List<String> list()
    {
        try
        {
            maybeCreateProfilesLogDir();
            return Arrays.stream(new File(logDir).list()).map(File::name).sorted().collect(toList());
        }
        catch (Throwable t)
        {
            return List.of();
        }
    }

    @Override
    public byte[] fetch(String resultFile) throws IOException
    {
        try
        {
            if (!Path.of(logDir, resultFile).toAbsolutePath().getParent().equals(Path.of(logDir)))
            {
                throw new IllegalArgumentException("Illegal file to fetch: " + resultFile);
            }
            maybeCreateProfilesLogDir();
            return Files.readAllBytes(new File(logDir, resultFile).toPath());
        }
        catch (NoSuchFileException t)
        {
            logger.error("Result file " + resultFile + " not found or error occurred while returning it.", t);
            throw t;
        }
    }

    @Override
    public void purge()
    {
        maybeCreateProfilesLogDir();
        new File(logDir).deleteRecursive();
    }

    @Override
    public String status()
    {
        return run(new ThrowingFunction<>()
        {
            @Override
            public String apply(AsyncProfiler asyncProfiler) throws Throwable
            {
                return asyncProfiler.execute("status");
            }
        });
    }

    @Override
    public synchronized boolean isEnabled()
    {
        return instance != null && asyncProfiler != null;
    }

    public static String validateOutputFileName(String outputFile)
    {
        if (outputFile == null || outputFile.trim().isEmpty())
            throw new IllegalArgumentException("Output file name must not be null or empty.");

        if (!VALID_FILENAME_REGEX_PATTERN.matcher(outputFile).matches())
            throw new IllegalArgumentException(format("Output file name must match pattern %s.", VALID_FILENAME_REGEX_PATTERN));

        return outputFile;
    }

    public static String validateCommand(String command)
    {
        if (command == null || command.isBlank())
            throw new IllegalArgumentException("Command can not be null or blank string.");

        return command;
    }

    /**
     * @param duration duration of profiling
     * @return converted string representation of duration to seconds
     */
    public static int parseDuration(String duration)
    {
        int durationSeconds = new DurationSpec.IntSecondsBound(duration).toSeconds();
        if (durationSeconds > MAX_SAFE_PROFILING_DURATION)
            throw new IllegalArgumentException(format("Max profiling duration is %s seconds. If you need longer profiling, use execute command instead.",
                                                      MAX_SAFE_PROFILING_DURATION));
        return durationSeconds;
    }

    private static void maybeCreateProfilesLogDir()
    {
        String dir = new File(logDir).toAbsolute().toString();

        if ((DatabaseDescriptor.getCommitLogLocation() != null && dir.startsWith(DatabaseDescriptor.getCommitLogLocation())) ||
            (DatabaseDescriptor.getAccordJournalDirectory() != null && dir.startsWith(DatabaseDescriptor.getAccordJournalDirectory())) ||
            dir.startsWith(DatabaseDescriptor.getHintsDirectory().absolutePath()) ||
            (DatabaseDescriptor.getCDCLogLocation() != null && dir.startsWith(DatabaseDescriptor.getCDCLogLocation())) ||
            (DatabaseDescriptor.getSavedCachesLocation() != null && dir.startsWith(DatabaseDescriptor.getSavedCachesLocation())))
        {
            throw new RuntimeException("You can not store Async-Profiler results into system Cassandra directory.");
        }

        for (String location : StorageService.instance.getAllDataFileLocations())
        {
            if (dir.startsWith(location))
                throw new RuntimeException("You can not store Async-Profiler results into a data directory of Cassandra.");
        }

        try
        {
            new File(logDir).createDirectoriesIfNotExists();
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Unable to create directory " + logDir);
        }
    }

    private void validateStartParameters(Map<String, String> parameters)
    {
        if (!ASYNC_PROFILER_START_PARAMS.equals(parameters.keySet()))
        {
            throw new IllegalArgumentException("Wrong parameters passed to start async profiler method. Passed parameters" +
                                               " should be: " + ASYNC_PROFILER_START_PARAMS);
        }
    }

    private void validateStopParameters(Map<String, String> parameters)
    {
        if (!ASYNC_PROFILER_STOP_PARAMS.containsAll(parameters.keySet()))
        {
            throw new IllegalArgumentException("Wrong parameters passed to stop async profiler method. Passed parameters" +
                                               " should be: " + ASYNC_PROFILER_STOP_PARAMS);
        }
    }

    private boolean isRunning()
    {
        if (!isEnabled())
            return false;

        try
        {
            String status = status();
            return status != null && status.contains("Profiling is running");
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private <T> T run(ThrowingFunction<AsyncProfiler, T> f)
    {
        if (!ASYNC_PROFILER_ENABLED.getBoolean())
            throw new IllegalStateException("Async Profiler is not enabled. Enable it by setting " + ASYNC_PROFILER_ENABLED.getKey() +
                                            " property to true.");
        if (asyncProfiler != null)
        {
            try
            {
                return f.apply(asyncProfiler);
            }
            catch (IllegalStateException | IllegalArgumentException t)
            {
                throw t;
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }
        else
            throw new IllegalStateException("Async profiler not available.");
    }

    public abstract static class ThrowingFunction<A, B>
    {
        public abstract B apply(AsyncProfiler a) throws Throwable;
    }

    private AsyncProfiler getProfiler()
    {
        if (!ASYNC_PROFILER_ENABLED.getBoolean())
            return null;

        if (asyncProfiler != null)
            return asyncProfiler;

        try
        {
            asyncProfiler = AsyncProfiler.getInstance();
            return asyncProfiler;
        }
        catch (Throwable t)
        {
            throw new IllegalStateException("Unable to get an instance of Async-Profiler", t);
        }
    }

    @VisibleForTesting
    public static void reset()
    {
        if (instance != null)
            asyncProfiler = null;

        instance = null;
    }
}
