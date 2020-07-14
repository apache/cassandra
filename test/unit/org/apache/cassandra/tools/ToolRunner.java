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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

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
    
    private final List<String> allArgs = new ArrayList<>();
    private Process process;
    private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
    private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private InputStream stdin;
    private boolean stdinAutoClose;
    private long defaultTimeoutMillis = TimeUnit.SECONDS.toMillis(30);
    private Thread ioWatcher;
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

    public ToolRunner withStdin(InputStream stdin, boolean autoClose)
    {
        this.stdin = stdin;
        this.stdinAutoClose = autoClose;
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
                        
                        ByteArrayOutputStream out = null;
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
            
            ioWatcher = new Thread(this::watchIO);
            ioWatcher.setDaemon(true);
            ioWatcher.start();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to start " + allArgs, e);
        }

        return this;
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
        return waitFor(defaultTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    public boolean waitFor(long time, TimeUnit timeUnit)
    {
        try
        {
            if (!process.waitFor(time, timeUnit))
                return false;
            ioWatcher.join();
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
        return waitAndAssertOnExitCode().assertEmptyStdErr();
    }
    
    public ToolRunner assertEmptyStdErr()
    {
        assertTrue(getStderr().isEmpty());
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
        return outBuffer.toString();
    }

    public String getStderr()
    {
        return errBuffer.toString();
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
    
    static class Runners
    {
        protected ToolRunner invokeNodetool(String... args)
        {
            return invokeNodetool(Arrays.asList(args));
        }

        protected ToolRunner invokeNodetool(List<String> args)
        {
            return invokeTool(buildNodetoolArgs(args), true);
        }

        private static List<String> buildNodetoolArgs(List<String> args)
        {
            return CQLTester.buildNodetoolArgs(args);
        }
        
        protected ToolRunner invokeClassAsTool(String... args)
        {
            return invokeClassAsTool(Arrays.asList(args));
        }
        
        protected ToolRunner invokeClassAsTool(List<String> args)
        {
            return invokeTool(args, false);
        }

        protected ToolRunner invokeTool(String... args)
        {
            return invokeTool(Arrays.asList(args));
        }

        protected ToolRunner invokeTool(List<String> args)
        {
            return invokeTool(args, true);
        }

        protected ToolRunner invokeTool(List<String> args, boolean runOutOfProcess)
        {
            ToolRunner runner = new ToolRunner(args, runOutOfProcess);
            runner.start().waitFor();
            return runner;
        }
    }
}
