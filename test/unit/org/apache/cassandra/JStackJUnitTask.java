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
package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.ExecuteWatchdog;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTask;
import org.apache.tools.ant.util.Watchdog;

public class JStackJUnitTask extends JUnitTask
{
    private Integer timeout;

    public JStackJUnitTask() throws Exception
    {
    }

    @Override
    public void setTimeout(Integer timeout)
    {
        this.timeout = timeout;
        super.setTimeout(timeout);
    }

    @Override
    public ExecuteWatchdog createWatchdog() throws BuildException
    {
        return new JStackWatchDog(timeout);
    }

    private static class JStackWatchDog extends ExecuteWatchdog
    {
        private long pid;

        public JStackWatchDog(long timeout)
        {
            super(timeout);
        }

        public JStackWatchDog(int timeout)
        {
            super(timeout);
        }

        @Override
        public synchronized void start(Process process)
        {
            this.pid = getPid(process);
            super.start(process);
        }

        @Override
        public synchronized void timeoutOccured(Watchdog w)
        {
            if (pid > 0)
            {
                ProcessBuilder pb = new ProcessBuilder("jstack","-l", String.valueOf(pid));
                try
                {
                    Process p = pb.start();
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream())))
                    {
                        StringBuilder sb = new StringBuilder();
                        String line;
                        while((line = br.readLine()) != null)
                        {
                            sb.append(line).append("\n");
                        }
                        System.out.println(sb.toString());
                    }
                }
                catch (IOException e)
                {
                    System.err.println("Could not get stack for "+pid);
                    e.printStackTrace();
                }
            }
            super.timeoutOccured(w);
        }

        private long getPid(Process process)
        {
            if (process.getClass().getName().equals("java.lang.UNIXProcess"))
            {
                try
                {
                    Field f = process.getClass().getDeclaredField("pid");
                    f.setAccessible(true);
                    long pid = f.getLong(process);
                    f.setAccessible(false);
                    return pid;
                }
                catch (IllegalAccessException | NoSuchFieldException e)
                {
                    System.err.println("Could not get PID");
                }
            }
            return -1;
        }
    }

}
