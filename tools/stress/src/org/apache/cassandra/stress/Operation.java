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

package org.apache.cassandra.stress;

import java.io.IOException;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.stress.settings.SettingsLog;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.transport.SimpleClient;

public abstract class Operation
{
    public final StressSettings settings;
    public final Timer timer;

    public Operation(Timer timer, StressSettings settings)
    {
        this.timer = timer;
        this.settings = settings;
    }

    public static interface RunOp
    {
        public boolean run() throws Exception;
        public int partitionCount();
        public int rowCount();
    }

    public abstract boolean ready(WorkManager permits, RateLimiter rateLimiter);

    public boolean isWrite()
    {
        return false;
    }

    /**
     * Run operation
     * @param client Cassandra Thrift client connection
     * @throws IOException on any I/O error.
     */
    public abstract void run(ThriftClient client) throws IOException;

    public void run(SimpleClient client) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public void run(JavaDriverClient client) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public void timeWithRetry(RunOp run) throws IOException
    {
        timer.start();

        boolean success = false;
        String exceptionMessage = null;

        int tries = 0;
        for (; tries < settings.errors.tries; tries++)
        {
            try
            {
                success = run.run();
                break;
            }
            catch (Exception e)
            {
                switch (settings.log.level)
                {
                    case MINIMAL:
                        break;

                    case NORMAL:
                        System.err.println(e);
                        break;

                    case VERBOSE:
                        e.printStackTrace(System.err);
                        break;

                    default:
                        throw new AssertionError();
                }
                exceptionMessage = getExceptionMessage(e);
            }
        }

        timer.stop(run.partitionCount(), run.rowCount(), !success);

        if (!success)
        {
            error(String.format("Operation x%d on key(s) %s: %s%n",
                    tries,
                    key(),
                    (exceptionMessage == null)
                        ? "Data returned was not validated"
                        : "Error executing: " + exceptionMessage));
        }

    }

    public abstract String key();

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

    protected void error(String message) throws IOException
    {
        if (!settings.errors.ignore)
            throw new IOException(message);
        else if (settings.log.level.compareTo(SettingsLog.Level.MINIMAL) > 0)
            System.err.println(message);
    }

}
