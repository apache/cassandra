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
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
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
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private final List<String> allArgs = new ArrayList<>();
    private Process process;
    @SuppressWarnings("resource")
    private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
    @SuppressWarnings("resource")
    private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private InputStream stdin;
    private boolean stdinAutoClose;
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

                int exit;
                try (PrintStream newOut = new PrintStream(toolOut); PrintStream newErr = new PrintStream(toolErr))
                {
                    System.setOut(newOut);
                    System.setErr(newErr);
                    String clazz = allArgs.get(0);
                    String[] clazzArgs = allArgs.subList(1, allArgs.size()).toArray(EMPTY_STRING_ARRAY);
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
                    public int waitFor()
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
            ioWatchers[0] = new Thread(new StreamGobbler<>(process.getErrorStream(), errBuffer));
            ioWatchers[0].setDaemon(true);
            ioWatchers[0].setName("IO Watcher stderr for " + allArgs);
            ioWatchers[0].start();

            ioWatchers[1] = new Thread(new StreamGobbler<>(process.getInputStream(), outBuffer));
            ioWatchers[1].setDaemon(true);
            ioWatchers[1].setName("IO Watcher stdout for " + allArgs);
            ioWatchers[1].start();

            if (includeStdinWatcher)
            {
                ioWatchers[2] = new Thread(new StreamGobbler<>(stdin, process.getOutputStream()));
                ioWatchers[2].setDaemon(true);
                ioWatchers[2].setName("IO Watcher stdin for " + allArgs);
                ioWatchers[2].start();
            }
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

    public int waitFor()
    {
        try
        {
            int rc = process.waitFor();
            // must call first in order to make sure the stdin ioWatcher will exit
            onComplete();
            for (Thread t : ioWatchers)
                t.join();
            return rc;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ToolRunner waitAndAssertOnExitCode()
    {
        assertExitCode(waitFor());
        return this;
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
        assertExitCode(getExitCode());
        return this;
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

    public int getExitCode()
    {
        return process.exitValue();
    }

    public String getStdout()
    {
        return outBuffer.toString();
    }

    public String getStderr()
    {
        return errBuffer.toString();
    }

    /**
     * Returns stdErr after removing any potential JVM env info output through the provided cleaners
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
     * Returns stdErr after removing any potential JVM env info output. Uses default list of excludes
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
        onComplete();
    }

    private void onComplete()
    {
        if (stdin != null)
        {
            try
            {
                stdin.close();
            }
            catch (IOException e)
            {
                logger.warn("Error closing stdin for {}", allArgs, e);
            }
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

    private void watchIO()
    {
        OutputStream in = process.getOutputStream();
        InputStream err = process.getErrorStream();
        InputStream out = process.getInputStream();
        while (true)
        {
            boolean errHandled;
            boolean outHandled;
            try
            {
                if (stdin != null)
                {
                    IOUtils.copy(stdin, in);
                    if (stdinAutoClose)
                    {
                        in.close();
                        stdin = null;
                    }
                }
                errHandled = IOUtils.copy(err, errBuffer) > 0;
                outHandled = IOUtils.copy(out, outBuffer) > 0;
            }
            catch(IOException e1)
            {
                logger.error("Error trying to use in/err/out from process");
                Thread.currentThread().interrupt();
                break;
            }
            if (!errHandled && !outHandled)
            {
                if (!process.isAlive())
                    return;
                try
                {
                    Thread.sleep(50L);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
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
            return invokeTool(CQLTester.buildNodetoolArgs(args), true, true);
        }

        /**
         * Invokes Cqlsh. The first arg is the cql to execute
         */
        public ToolRunner invokeCqlsh(String... args)
        {
            return invokeCqlsh(Arrays.asList(args));
        }

        /**
         * Invokes Cqlsh. The first arg is the cql to execute
         */
        public ToolRunner invokeCqlsh(List<String> args)
        {
            return invokeTool(CQLTester.buildCqlshArgs(args), true, true);
        }

        public static ToolRunner invokeClassAsTool(String... args)
        {
            return invokeClassAsTool(Arrays.asList(args));
        }

        public static ToolRunner invokeClassAsTool(List<String> args)
        {
            return invokeTool(args, false, true);
        }

        public ToolRunner invokeCassandraStress(String... args)
        {
            return invokeCassandraStress(Arrays.asList(args));
        }

        public ToolRunner invokeCassandraStress(List<String> args)
        {
            return invokeTool(CQLTester.buildCassandraStressArgs(args), true, true);
        }

        public static ToolRunner invokeTool(String... args)
        {
            return invokeTool(Arrays.asList(args));
        }

        public static ToolRunner invokeTool(List<String> args)
        {
            return invokeTool(args, true, true);
        }

        public static ToolRunner invokeToolNoWait(List<String> args)
        {
            return invokeTool(args, true, false);
        }

        public static ToolRunner invokeTool(List<String> args, boolean runOutOfProcess, boolean wait)
        {
            ToolRunner runner = new ToolRunner(args, runOutOfProcess);
            if (wait)
                runner.start().waitFor();
            else
                runner.start();

            return runner;
        }

    }
}
