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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.profiler.AsyncProfilerMBean;
import org.apache.cassandra.service.AsyncProfilerService.AsyncProfilerEvent;
import org.apache.cassandra.service.AsyncProfilerService.AsyncProfilerFormat;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.FBUtilities;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.joining;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_DURATION_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_EVENTS_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.parseDuration;
import static org.apache.cassandra.service.AsyncProfilerService.validateCommand;
import static org.apache.cassandra.service.AsyncProfilerService.validateOutputFileName;

@Command(name = "profile", description = "Manage Async-Profiler on a Cassandra process",
subcommands = {
AsyncProfileCommandGroup.AsyncProfileStartCommand.class,
AsyncProfileCommandGroup.AsyncProfileStopCommand.class,
AsyncProfileCommandGroup.AsyncProfileExecuteCommand.class,
AsyncProfileCommandGroup.AsyncProfilePurgeCommand.class,
AsyncProfileCommandGroup.AsyncProfileListCommand.class,
AsyncProfileCommandGroup.AsyncProfileFetchCommand.class,
AsyncProfileCommandGroup.AsyncProfileStatusCommand.class
})
public class AsyncProfileCommandGroup extends AbstractCommand
{
    @Override
    public void execute(NodeProbe probe)
    {
        AbstractCommand cmd = new AsyncProfileStartCommand();
        cmd.probe(probe);
        cmd.logger(output);
        cmd.run();
    }

    private static void doWithProfiler(NodeProbe probe, Consumer<AsyncProfilerMBean> consumer, boolean requiresEnabledProfiler)
    {
        AsyncProfilerMBean profiler = probe.getAsyncProfilerProxy();

        if (requiresEnabledProfiler && !profiler.isEnabled())
        {
            probe.output().err.println("Async-profiler native library is not enabled or not possible to load.");
            System.exit(1);
        }

        consumer.accept(profiler);
    }

    private static String getOutputFileName(AsyncProfilerFormat outputFormat)
    {
        String filename = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")
                                           .withZone(ZoneId.systemDefault()).format(FBUtilities.now());

        if (outputFormat == AsyncProfilerFormat.jfr)
            filename += ".jfr";
        else
            filename += ".html";

        return filename;
    }

    public static void doWithProfiler(NodeProbe probe, Consumer<AsyncProfilerMBean> consumer)
    {
        doWithProfiler(probe, consumer, true);
    }

    @Command(name = "start", description = "Run Async-Profiler on a Cassandra process")
    public static class AsyncProfileStartCommand extends AbstractCommand
    {
        @Option(names = { "-e", "--event" },
        description = "Event(s) to profile, one of or combination of 'cpu', 'alloc', " +
                      "'lock', 'wall', 'nativemem', 'cache_misses', delimited by comma, defaults to 'cpu'")
        public List<AsyncProfilerEvent> event = List.of(AsyncProfilerEvent.cpu);

        @Option(names = { "-o", "--output" }, description = "File name to save profiling results into, defaults to a " +
                                                            "file of name 'yyyy-MM-dd-HH-mm-ss.html'")
        public String filename;

        @Option(names = { "-d", "--duration" }, description = "Duration of profiling, defaults to '60s'. Accepts string values " +
                                                              "in the form of '5m', '30s' and similar.")
        public String duration = "60s";

        @Option(names = { "-f", "--format" },
        description = "Output format, one of 'flat', 'traces', 'collapsed', 'flamegraph', 'tree', 'jfr', defaults to 'flamegraph'")
        public AsyncProfilerFormat outputFormat = AsyncProfilerFormat.flamegraph;

        @Override
        public void execute(NodeProbe probe)
        {
            // make sure it is valid
            parseDuration(duration);

            if (filename == null)
                filename = AsyncProfileCommandGroup.getOutputFileName(outputFormat);

            doWithProfiler(probe, profiler -> {
                Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, event.stream().map(Enum::name).collect(joining(",")),
                                                             ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, outputFormat.name(),
                                                             ASYNC_PROFILER_START_DURATION_PARAM, duration,
                                                             ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, validateOutputFileName(filename));
                if (!profiler.start(startParameters))
                {
                    output.err.println("Profiler has already started or there was a failure to start it.");
                    System.exit(1);
                }
            });
        }
    }

    @Command(name = "stop", description = "Stop Async-Profiler on a Cassandra process")
    public static class AsyncProfileStopCommand extends AbstractCommand
    {
        @Option(names = { "-o", "--output" }, description = "File name to save profiling results into, defaults to a " +
                                                            "file of name 'yyyy-MM-dd-HH-mm-ss.html'")
        public String filename;

        @Override
        public void execute(NodeProbe probe)
        {
            doWithProfiler(probe, profiler -> {
                String file = filename != null ? validateOutputFileName(filename) : null;
                Map<String, String> stopParameters = file == null ? Map.of() : Map.of(ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM, file);
                if (!profiler.stop(stopParameters))
                {
                    output.err.println("Profiler has already stopped or there was a failure to stop it.");
                    System.exit(1);
                }
            });
        }
    }

    @Command(name = "execute", description = "Execute an arbitrary command on Async-Profiler on a Cassandra process.")
    public static class AsyncProfileExecuteCommand extends AbstractCommand
    {
        @Parameters(index = "0", description = "Raw command to execute. There has to be 'unsafe' profiler configured " +
                                               "in Cassandra, driven by cassandra.async_profiler.unsafe_mode property set" +
                                               " to true, to be able to do this.", arity = "1")
        public String command;

        @Override
        public void execute(NodeProbe probe)
        {
            doWithProfiler(probe, profiler -> {
                try
                {
                    output.out.print(profiler.execute(validateCommand(command)));
                }
                catch (Throwable ex)
                {
                    output.err.print(ex.getMessage());
                    System.exit(1);
                }
            });
        }
    }

    @Command(name = "purge", description = "Remove all profiling results from node's disk")
    public static class AsyncProfilePurgeCommand extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            doWithProfiler(probe, AsyncProfilerMBean::purge, false);
        }
    }

    @Command(name = "list", description = "List profiling result files of a node")
    public static class AsyncProfileListCommand extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            doWithProfiler(probe, profiler -> {
                for (String resultFile : profiler.list())
                    output.out.println(resultFile);
            }, false);
        }
    }

    @Command(name = "fetch", description = "Copy profiler result file from a node to a local file")
    public static class AsyncProfileFetchCommand extends AbstractCommand
    {

        @Parameters(index = "0", description = "Remote profiler file name", arity = "1")
        private String remoteFile;

        @Parameters(index = "1", description = "Local file name", arity = "1")
        private String localFile;

        @Override
        protected void execute(NodeProbe probe)
        {
            doWithProfiler(probe, profiler -> {
                try
                {
                    Files.write(new File(localFile).toPath(), profiler.fetch(remoteFile), CREATE, TRUNCATE_EXISTING, WRITE);
                }
                catch (NoSuchFileException e)
                {
                    probe.output().err.println("Remote file " + remoteFile + " was not found.");
                    System.exit(1);
                }
                catch (IllegalArgumentException e)
                {
                    probe.output().err.println(e.getMessage());
                    System.exit(1);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }, false);
        }
    }

    @Command(name = "status", description = "Get status of profiling")
    public static class AsyncProfileStatusCommand extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            doWithProfiler(probe, profiler -> output.out.print(profiler.status()));
        }
    }
}