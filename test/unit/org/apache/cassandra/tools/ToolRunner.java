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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.OfflineToolUtils.SystemExitException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ToolRunner
{
    protected static final Logger logger = LoggerFactory.getLogger(ToolRunner.class);

    private static final ImmutableList<String> DEFAULT_CLEANERS = ImmutableList.of("(?im)^picked up.*\\R");

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
            }
        }
    }

    /**
     * Invokes Cqlsh. The first arg is the cql to execute
     */
    public static ToolResult invokeCqlsh(String... args)
    {
        return invokeCqlsh(Arrays.asList(args));
    }

    /**
     * Invokes Cqlsh. The first arg is the cql to execute
     */
    public static ToolResult invokeCqlsh(List<String> args)
    {
        return invoke(CQLTester.buildCqlshArgs(args));
    }

    public static ToolResult invokeCassandraStress(String... args)
    {
        return invokeCassandraStress(Arrays.asList(args));
    }

    public static ToolResult invokeCassandraStress(List<String> args)
    {
        return invoke(CQLTester.buildCassandraStressArgs(args));
    }

    public static ToolResult invokeNodetool(String... args)
    {
        return invokeNodetool(Arrays.asList(args));
    }

    public static ToolResult invokeNodetool(List<String> args)
    {
        return invoke(CQLTester.buildNodetoolArgs(args));
    }

    public static ToolResult invoke(List<String> args)
    {
        return invoke(args.toArray(new String[args.size()]));
    }

    public static ToolResult invoke(String... args) 
    {
        try (ObservableTool  t = invokeAsync(args))
        {
            return t.waitComplete();
        }
    }

    public static ObservableTool invokeAsync(String... args)
    {
        return invokeAsync(Collections.emptyMap(), null, Arrays.asList(args));
    }

    public static ToolResult invoke(Map<String, String> env, InputStream stdin, List<String> args)
    {
        try (ObservableTool  t = invokeAsync(env, stdin, args))
        {
            return t.waitComplete();
        }
    }

    public static ObservableTool invokeAsync(Map<String, String> env, InputStream stdin, List<String> args)
    {
        ProcessBuilder pb = new ProcessBuilder(args);
        if (env != null && !env.isEmpty())
            pb.environment().putAll(env);
        try
        {
            return new ForkedObservableTool(pb.start(), stdin, args);
        }
        catch (IOException e)
        {
            return new FailedObservableTool(e, args);
        }
    }

    public static ToolResult invokeClass(String klass,  String... args)
    {
        return invokeClass(klass, null, args);
    }

    public static ToolResult invokeClass(Class<?> klass,  String... args)
    {
        return invokeClass(klass.getName(), null, args);
    }

    public static ToolResult invokeClass(String klass, InputStream stdin, String... args)
    {
        List<String> allArgs = new ArrayList<>();
        allArgs.add(klass);
        allArgs.addAll(Arrays.asList(args));
        
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
            int rc = runClassAsTool(klass, args);
            out.flush();
            err.flush();
            return new ToolResult(allArgs, rc, out.toString(), err.toString(), null);
        }
        catch (Exception e)
        {
            return new ToolResult(allArgs,
                                  -1,
                                  out.toString(),
                                  err.toString() + "\n" + Throwables.getStackTraceAsString(e),
                                  e);
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
        private final List<String> allArgs;
        private final int exitCode;
        private final String stdout;
        private final String stderr;
        private final Exception e;

        private ToolResult(List<String> allArgs, int exitCode, String stdout, String stderr, Exception e)
        {
            this.allArgs = allArgs;
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
            this.e = e;
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
        
        public Exception getException()
        {
            return e;
        }
        
        /**
         * Checks if the stdErr is empty after removing any potential JVM env info output and other noise
         * 
         * Some JVM configs may output env info on stdErr. We need to remove those to see what was the tool's actual
         * stdErr
         * 
         * @return The ToolRunner instance
         */
        public void assertCleanStdErr()
        {
            assertTrue("Failed because cleaned stdErr wasn't empty: " + getCleanedStderr(),
                       getCleanedStderr().isEmpty());
        }

        public void assertOnExitCode()
        {
            assertExitCode(getExitCode());
        }

        private void assertExitCode(int code)
        {
            if (code != 0)
                fail(String.format("%s%nexited with code %d%nstderr:%n%s%nstdout:%n%s",
                                   argsToLogString(),
                                   code,
                                   getStderr(),
                                   getStdout()));
        }

        public String argsToLogString()
        {
            return allArgs.stream().collect(Collectors.joining(",\n    ", "[", "]"));
        }

        /**
         * Returns stdErr after removing any potential JVM env info output through the provided cleaners
         * 
         * Some JVM configs may output env info on stdErr. We need to remove those to see what was the tool's actual
         * stdErr
         * 
         * @param regExpCleaners
         *            List of regExps to remove from stdErr
         * @return The stdErr with all excludes removed
         */
        public String getCleanedStderr(List<String> regExpCleaners)
        {
            String sanitizedStderr = getStderr();
            for (String regExp : regExpCleaners)
                sanitizedStderr = sanitizedStderr.replaceAll(regExp, "");
            return sanitizedStderr;
        }

        /**
         * Returns stdErr after removing any potential JVM env info output. Uses default list of excludes
         * 
         * {@link #getCleanedStderr(List)}
         */
        public String getCleanedStderr()
        {
            return getCleanedStderr(DEFAULT_CLEANERS);
        }
        
        public void assertOnCleanExit()
        {
            assertOnExitCode();
            assertCleanStdErr();
        }
    }

    public interface ObservableTool extends AutoCloseable
    {
        String getPartialStdout();

        String getPartialStderr();

        boolean isDone();

        ToolResult waitComplete();

        @Override
        void close();
    }

    private static final class FailedObservableTool implements ObservableTool
    {
        private final List<String> args;
        private final IOException error;

        private FailedObservableTool(IOException error, List<String> args)
        {
            this.args = args;
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
        public ToolResult waitComplete()
        {
            return new ToolResult(args, -1, getPartialStdout(), getPartialStderr(), error);
        }

        @Override
        public void close()
        {

        }
    }

    private static final class ForkedObservableTool implements ObservableTool
    {
        @SuppressWarnings("resource")
        private final ByteArrayOutputStream err = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        private final InputStream stdin;
        private final Process process;
        private final Thread[] ioWatchers;
        private final List<String> args;

        private ForkedObservableTool(Process process, InputStream stdin, List<String> args)
        {
            this.process = process;
            this.args = args;
            this.stdin = stdin;

            // Each stream tends to use a bounded buffer, so need to process each stream in its own thread else we
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
        public ToolResult waitComplete()
        {
            try
            {
                int rc = process.waitFor();
                onComplete();
                return new ToolResult(args, rc, out.toString(), err.toString(), null);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private void onComplete() throws InterruptedException
        {
            try
            {
                if (stdin != null)
                    stdin.close();
            }
            catch (IOException e)
            {
                logger.warn("Error closing stdin", e);
            }
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

        public ToolResult invoke()
        {
            return ToolRunner.invoke(env, stdin, args);
        }
    }
}
