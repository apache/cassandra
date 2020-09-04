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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

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
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final CompletableFuture<Void> onComplete = new CompletableFuture<>();
    private InputStream stdin;
    private Thread[] ioWatchers;
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

    public ToolRunner withStdin(InputStream stdin)
    {
        this.stdin = stdin;
        return this;
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

                System.setIn(stdin == null ? originalSysIn : stdin);

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
                        if (stdin == null)
                            return null;

                        ByteArrayOutputStream out;
                        try
                        {
                            out = new ByteArrayOutputStream(stdin.available());
                            IOUtils.copy(stdin, out);
                        }
                        catch(IOException e)
                        {
                            throw new RuntimeException("Failed to get stdin", e);
                        }
                        return out;
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
            int numWatchers = 2;
            // only need a stdin watcher when forking
            boolean includeStdinWatcher = runOutOfProcess && stdin != null;
            if (includeStdinWatcher)
                numWatchers = 3;
            ioWatchers = new Thread[numWatchers];
            ioWatchers[0] = new Thread(new StreamGobbler<>(process.getErrorStream(), err));
            ioWatchers[0].setDaemon(true);
            ioWatchers[0].setName("IO Watcher stderr for " + allArgs);
            ioWatchers[0].start();

            ioWatchers[1] = new Thread(new StreamGobbler<>(process.getInputStream(), out));
            ioWatchers[1].setDaemon(true);
            ioWatchers[1].setName("IO Watcher stdout for " + allArgs);
            ioWatchers[1].start();

            if (includeStdinWatcher)
            {
                ioWatchers[2] = new Thread(new StreamGobbler<>(stdin, process.getOutputStream()));
                ioWatchers[2].setDaemon(true);
                ioWatchers[2].setName("IO Watcher stdin for " + allArgs);
                ioWatchers[2].start();
                // since stdin might not close the thread would block, so add logic to try to close stdin when the process exits
                onComplete.whenComplete((i1, i2) -> {
                    try
                    {
                        stdin.close();
                    }
                    catch (IOException e)
                    {
                        logger.warn("Error closing stdin for {}", allArgs, e);
                    }
                });
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to start " + allArgs, e);
        }

        return this;
    }

    public static int runClassAsTool(String clazz, String... args)
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
            onComplete.complete(null); // safe to call multiple times as it will just start returning false
            for (Thread t : ioWatchers)
                t.join();
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
        return out.toString();
    }

    public String getStderr()
    {
        return err.toString();
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

    private static final class StreamGobbler<T extends OutputStream> implements Runnable
    {
        private static final int BUFFER_SIZE = 8_192;

        private final InputStream input;
        private final T out;

        private StreamGobbler(InputStream input, T out)
        {
            this.input = input;
            this.out = out;
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
                        return;
                    }
                    out.write(buffer, 0, read);
                }
                catch (IOException e)
                {
                    logger.error("Unexpected IO Error while reading stream", e);
                    return;
                }
                catch (Throwable t)
                {
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

    public static ToolResult invoke(String... args) throws InterruptedException
    {
        try (ObservableTool tool = invokeAsync(args))
        {
            return tool.waitComplete();
        }
    }

    public static ObservableTool invokeAsync(String... args)
    {
        return invokeAsync(Collections.emptyMap(), null, Arrays.asList(args));
    }

    public static ToolResult invoke(Map<String, String> env, InputStream stdin, List<String> args) throws InterruptedException
    {
        try (ObservableTool tool = invokeAsync(env, stdin, args))
        {
            return tool.waitComplete();
        }
    }

    public static ObservableTool invokeAsync(Map<String, String> env, InputStream stdin, List<String> args)
    {
        ProcessBuilder pb = new ProcessBuilder(args);
        if (env != null && !env.isEmpty())
            pb.environment().putAll(env);
        try
        {
            return new ForkedObservableTool(pb.start(), stdin);
        }
        catch (IOException e)
        {
            return new FailedObservableTool(e);
        }
    }

    public static ToolResult invokeClass(Class<?> klass, InputStream stdin, String... args)
    {
        PrintStream originalSysOut = System.out;
        PrintStream originalSysErr = System.err;
        InputStream originalSysIn = System.in;
        originalSysOut.flush();
        originalSysErr.flush();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        System.setIn(stdin == null ? originalSysIn : stdin);

        try (PrintStream newOut = new PrintStream(out);
             PrintStream newErr = new PrintStream(err))
        {
            System.setOut(newOut);
            System.setErr(newErr);
            int rc = runClassAsTool(klass.getName(), args);
            return new ToolResult(rc, out.toString(), err.toString());
        }
        catch (Exception e)
        {
            return new ToolResult(-1, "", Throwables.getStackTraceAsString(e));
        }
        finally
        {
            System.setOut(originalSysOut);
            System.setErr(originalSysErr);
            System.setIn(originalSysIn);
        }
    }

    public static Builder builder(List<String> args)
    {
        return new Builder(args);
    }

    public static final class ToolResult
    {
        private final int exitCode;
        private final String stdout;
        private final String stderr;

        private ToolResult(int exitCode, String stdout, String stderr)
        {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        public int getExitCode()
        {
            return exitCode;
        }

        public String getStdout()
        {
            return stdout;
        }

        public String getStderr()
        {
            return stderr;
        }
    }

    public interface ObservableTool extends AutoCloseable
    {
        String getPartialStdout();

        String getPartialStderr();

        boolean isDone();

        ToolResult waitComplete() throws InterruptedException;

        @Override
        void close();
    }

    private static final class FailedObservableTool implements ObservableTool
    {
        private final IOException error;

        private FailedObservableTool(IOException error)
        {
            this.error = error;
        }

        @Override
        public String getPartialStdout()
        {
            return "";
        }

        @Override
        public String getPartialStderr()
        {
            return error.getMessage();
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public ToolResult waitComplete() throws InterruptedException
        {
            return new ToolResult(-1, getPartialStdout(), getPartialStderr());
        }

        @Override
        public void close()
        {

        }
    }

    private static final class ForkedObservableTool implements ObservableTool
    {
        private final CompletableFuture<Void> onComplete = new CompletableFuture<>();
        private final ByteArrayOutputStream err = new ByteArrayOutputStream();
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final Process process;
        private final Thread[] ioWatchers;

        private ForkedObservableTool(Process process, InputStream stdin)
        {
            this.process = process;

            // each stream tends to use a bounded buffer, so need to process each stream in its own thread else we
            // might block on an idle stream, not consuming the other stream which is blocked in the other process
            // as nothing is consuming
            int numWatchers = 2;
            // only need a stdin watcher when forking
            boolean includeStdinWatcher = stdin != null;
            if (includeStdinWatcher)
                numWatchers = 3;
            ioWatchers = new Thread[numWatchers];
            ioWatchers[0] = new Thread(new StreamGobbler<>(process.getErrorStream(), err));
            ioWatchers[0].setDaemon(true);
            ioWatchers[0].setName("IO Watcher stderr");
            ioWatchers[0].start();

            ioWatchers[1] = new Thread(new StreamGobbler<>(process.getInputStream(), out));
            ioWatchers[1].setDaemon(true);
            ioWatchers[1].setName("IO Watcher stdout");
            ioWatchers[1].start();

            if (includeStdinWatcher)
            {
                ioWatchers[2] = new Thread(new StreamGobbler<>(stdin, process.getOutputStream()));
                ioWatchers[2].setDaemon(true);
                ioWatchers[2].setName("IO Watcher stdin");
                ioWatchers[2].start();
                // since stdin might not close the thread would block, so add logic to try to close stdin when the process exits
                onComplete.whenComplete((i1, i2) -> {
                    try
                    {
                        stdin.close();
                    }
                    catch (IOException e)
                    {
                        logger.warn("Error closing stdin", e);
                    }
                });
            }
        }

        @Override
        public String getPartialStdout()
        {
            return out.toString();
        }

        @Override
        public String getPartialStderr()
        {
            return err.toString();
        }

        @Override
        public boolean isDone()
        {
            return !process.isAlive();
        }

        @Override
        public ToolResult waitComplete() throws InterruptedException
        {
            int rc = process.waitFor();
            onComplete();
            return new ToolResult(rc, out.toString(), err.toString());
        }

        private void onComplete() throws InterruptedException
        {
            onComplete.complete(null);
            for (Thread t : ioWatchers)
                t.join();
        }

        @Override
        public void close()
        {
            if (!process.isAlive())
                return;
            process.destroyForcibly();
        }
    }

    public static final class Builder
    {
        private final Map<String, String> env = new HashMap<>();
        private final List<String> args;
        private InputStream stdin;

        public Builder(List<String> args)
        {
            this.args = Objects.requireNonNull(args);
        }

        public Builder withEnv(String key, String value)
        {
            env.put(key, value);
            return this;
        }

        public Builder withEnvs(Map<String, String> map)
        {
            env.putAll(map);
            return this;
        }

        public Builder withStdin(InputStream input)
        {
            this.stdin = input;
            return this;
        }

        public ObservableTool invokeAsync()
        {
            return ToolRunner.invokeAsync(env, stdin, args);
        }

        public ToolResult invoke() throws InterruptedException
        {
            return ToolRunner.invoke(env, stdin, args);
        }
    }
}
