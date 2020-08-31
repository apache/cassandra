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

package org.apache.cassandra.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.OfflineToolUtils.SystemExitException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ToolRunner implements AutoCloseable
{
    protected static final Logger logger = LoggerFactory.getLogger(ToolRunner.class);

    private static final ImmutableList<String> DEFAULT_CLEANERS = ImmutableList.of("(?im)^picked up.*\\R");
    
    private final List<String> allArgs = new ArrayList<>();
    private Process process;
    private final CompletableFuture<ByteArrayOutputStream> errBufferFuture = new CompletableFuture<>();
    private final CompletableFuture<ByteArrayOutputStream> outBufferFuture = new CompletableFuture<>();
    private boolean stdinAutoClose;
    private long defaultTimeoutMillis = TimeUnit.SECONDS.toMillis(30);
    private Thread ioWatcherStdErr;
    private Thread ioWatcherStdOut;
    private Map<String, String> envs;
    private boolean runOutOfProcess = true;

    public ToolRunner(List<String> args)
    {
        this.allArgs.addAll(args);
    }
    
    public ToolRunner(List<String> args, boolean runOutOfProcess)
    {
        this.allArgs.addAll(args);
        this.runOutOfProcess = runOutOfProcess;
    }

    public ToolRunner withEnvs(Map<String, String> envs)
    {
        Preconditions.checkArgument(runOutOfProcess, "Not supported");
        this.envs = envs;
        return this;
    }

    public ToolRunner start()
    {
        if (process != null)
            throw new IllegalStateException("Process already started. Create a new ToolRunner instance for each invocation.");

        logger.debug("Starting {} with args {}", runOutOfProcess ? "process" : "class" , argsToLogString());

        try
        {
            if (runOutOfProcess)
            {
                ProcessBuilder pb = new ProcessBuilder(allArgs);
                if (envs != null)
                    pb.environment().putAll(envs);
                process = pb.start();
            }
            else
            {
                PrintStream originalSysOut = System.out;
                PrintStream originalSysErr = System.err;
                InputStream originalSysIn = System.in;
                originalSysOut.flush();
                originalSysErr.flush();
                ByteArrayOutputStream toolOut = new ByteArrayOutputStream();
                ByteArrayOutputStream toolErr = new ByteArrayOutputStream();

                int exit = 0;
                try (PrintStream newOut = new PrintStream(toolOut); PrintStream newErr = new PrintStream(toolErr);)
                {
                    System.setOut(newOut);
                    System.setErr(newErr);
                    String clazz = allArgs.get(0);
                    String[] clazzArgs = allArgs.subList(1, allArgs.size()).toArray(new String[0]);
                    exit = runClassAsTool(clazz, clazzArgs);
                }
                
                final int exitCode = exit;
                System.setOut(originalSysOut);
                System.setErr(originalSysErr);
                System.setIn(originalSysIn);
                
                process = new Process() {

                    @Override
                    public void destroy()
                    {
                    }

                    @Override
                    public int exitValue()
                    {
                        return exitCode;
                    }

                    @Override
                    public InputStream getErrorStream()
                    {
                        return new ByteArrayInputStream(toolErr.toByteArray());
                    }

                    @Override
                    public InputStream getInputStream()
                    {
                        return new ByteArrayInputStream(toolOut.toByteArray());
                    }

                    @Override
                    public OutputStream getOutputStream()
                    {
                        return null;
                    }

                    @Override
                    public int waitFor() throws InterruptedException
                    {
                        return exitValue();
                    }
                    
                };
            }

            // each stream tends to use a bounded buffer, so need to process each stream in its own thread else we
            // might block on an idle stream, not consuming the other stream which is blocked in the other process
            // as nothing is consuming
            ioWatcherStdErr = new Thread(new StreamGobbler(process.getErrorStream(), errBufferFuture));
            ioWatcherStdErr.setDaemon(true);
            ioWatcherStdErr.setName("IO Watcher stderr for " + allArgs);
            ioWatcherStdErr.start();

            ioWatcherStdOut = new Thread(new StreamGobbler(process.getInputStream(), outBufferFuture));
            ioWatcherStdOut.setDaemon(true);
            ioWatcherStdOut.setName("IO Watcher stdout for " + allArgs);
            ioWatcherStdOut.start();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to start " + allArgs, e);
        }

        return this;
    }

    public int runClassAsTool(String clazz, String... args)
    {
        try
        {
            // install security manager to get informed about the exit-code
            System.setSecurityManager(new SecurityManager()
            {
                public void checkExit(int status)
                {
                    throw new SystemExitException(status);
                }

                public void checkPermission(Permission perm)
                {
                }

                public void checkPermission(Permission perm, Object context)
                {
                }
            });

            try
            {
                Class.forName(clazz).getDeclaredMethod("main", String[].class).invoke(null, (Object) args);
            }
            catch (InvocationTargetException e)
            {
                Throwable cause = e.getCause();
                if (cause instanceof Error)
                    throw (Error) cause;
                if (cause instanceof RuntimeException)
                    throw (RuntimeException) cause;
                throw e;
            }

            return 0;
        }
        catch (SystemExitException e)
        {
            return e.status;
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e.getTargetException());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // uninstall security manager
            System.setSecurityManager(null);
        }
    }

    public boolean isRunning()
    {
        return process != null && process.isAlive();
    }

    public boolean waitFor()
    {
        try
        {
            process.waitFor();
            ioWatcherStdOut.join();
            ioWatcherStdErr.join();
            return true;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ToolRunner waitAndAssertOnExitCode()
    {
        assertTrue(String.format("Tool %s didn't terminate",
                           argsToLogString()),
                   waitFor());
        return assertOnExitCode();
    }
    
    public ToolRunner waitAndAssertOnCleanExit()
    {
        return waitAndAssertOnExitCode().assertCleanStdErr();
    }
    
    /**
     * Checks if the stdErr is empty after removing any potential JVM env info output and other noise
     * 
     * Some JVM configs may output env info on stdErr. We need to remove those to see what was the tool's actual stdErr
     * @return The ToolRunner instance
     */
    public ToolRunner assertCleanStdErr()
    {
        assertTrue("Failed because cleaned stdErr wasn't empty: " + getCleanedStderr(), getCleanedStderr().isEmpty());
        return this;
    }

    public ToolRunner assertOnExitCode()
    {
        int code = getExitCode();
        if (code != 0)
            fail(String.format("%s%nexited with code %d%nstderr:%n%s%nstdout:%n%s",
                               argsToLogString(),
                               code,
                               getStderr(),
                               getStdout()));
        return this;
    }

    public String argsToLogString()
    {
        return allArgs.stream().collect(Collectors.joining(",\n    ", "[", "]"));
    }

    public int getExitCode()
    {
        return process.exitValue();
    }

    public String getStdout()
    {
        return Futures.getUnchecked(outBufferFuture).toString();
    }

    public String getStderr()
    {
        return Futures.getUnchecked(errBufferFuture).toString();
    }

    /**
     * Checks if the stdErr is empty after removing any potential JVM env info output and other noise
     * 
     * Some JVM configs may output env info on stdErr. We need to remove those to see what was the tool's actual stdErr
     * 
     * @param regExpCleaners List of regExps to remove from stdErr
     * @return The stdErr with all excludes removed
     */
    public String getCleanedStderr(List<String> regExpCleaners)
    {
        String sanitizedStderr = getStderr();
        for (String regExp: regExpCleaners)
            sanitizedStderr = sanitizedStderr.replaceAll(regExp, "");
        return sanitizedStderr;
    }

    /**
     * Checks if the stdErr is empty after removing any potential JVM env info output. Uses default list of excludes
     * 
     * {@link #getCleanedStderr(List)}
     */
    public String getCleanedStderr()
    {
        return getCleanedStderr(DEFAULT_CLEANERS);
    }

    public void forceKill()
    {
        try
        {
            process.exitValue();
            // process no longer alive - just ignore that fact
        }
        catch (IllegalThreadStateException e)
        {
            process.destroyForcibly();
        }
    }

    @Override
    public void close()
    {
        forceKill();
    }

    private static final class StreamGobbler implements Runnable
    {
        private static final int BUFFER_SIZE = 8_192;

        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final InputStream input;
        private final CompletableFuture<ByteArrayOutputStream> whenComplete;

        private StreamGobbler(InputStream input, CompletableFuture<ByteArrayOutputStream> whenComplete)
        {
            this.input = input;
            this.whenComplete = whenComplete;
        }

        public void run()
        {
            byte[] buffer = new byte[BUFFER_SIZE];
            while (true)
            {
                try
                {
                    int read = input.read(buffer);
                    if (read == -1)
                    {
                        whenComplete.complete(out);
                        return;
                    }
                    out.write(buffer, 0, read);
                }
                catch (IOException e)
                {
                    logger.error("Unexpected IO Error while reading stream", e);
                    whenComplete.complete(out);
                    return;
                }
                catch (Throwable t)
                {
                    whenComplete.complete(out);
                    throw t;
                }
            }
        }
    }

    public static class Runners
    {
        public static ToolRunner invokeNodetool(String... args)
        {
            return invokeNodetool(Arrays.asList(args));
        }

        public static ToolRunner invokeNodetool(List<String> args)
        {
            return invokeTool(buildNodetoolArgs(args), true);
        }

        private static List<String> buildNodetoolArgs(List<String> args)
        {
            return CQLTester.buildNodetoolArgs(args);
        }

        public static ToolRunner invokeClassAsTool(String... args)
        {
            return invokeClassAsTool(Arrays.asList(args));
        }

        public static ToolRunner invokeClassAsTool(List<String> args)
        {
            return invokeTool(args, false);
        }

        public static ToolRunner invokeTool(String... args)
        {
            return invokeTool(Arrays.asList(args));
        }

        public static ToolRunner invokeTool(List<String> args)
        {
            return invokeTool(args, true);
        }

        public static ToolRunner invokeTool(List<String> args, boolean runOutOfProcess)
        {
            ToolRunner runner = new ToolRunner(args, runOutOfProcess);
            runner.start().waitFor();
            return runner;
        }
    }
}
