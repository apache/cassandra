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

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.Condition;

import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;

public class BootstrapMonitor extends JMXNotificationProgressListener
{
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    private final PrintStream out;
    private final Condition condition = new SimpleCondition();

    public BootstrapMonitor(PrintStream out)
    {
        this.out = out;
    }

    public void awaitCompletion() throws InterruptedException
    {
        condition.await();
    }

    @Override
    public boolean isInterestedIn(String tag)
    {
        return "bootstrap".equals(tag);
    }

    @Override
    public void handleNotificationLost(long timestamp, String message)
    {
        super.handleNotificationLost(timestamp, message);
    }

    @Override
    public void handleConnectionClosed(long timestamp, String message)
    {
        handleConnectionFailed(timestamp, message);
    }

    @Override
    public void handleConnectionFailed(long timestamp, String message)
    {
        Exception error = new IOException(String.format("[%s] JMX connection closed. (%s)",
                                              format.format(timestamp), message));
        out.println(error.getMessage());
        condition.signalAll();
    }

    @Override
    public void progress(String tag, ProgressEvent event)
    {
        ProgressEventType type = event.getType();
        String message = String.format("[%s] %s", format.format(System.currentTimeMillis()), event.getMessage());
        if (type == ProgressEventType.PROGRESS)
        {
            message = message + " (progress: " + (int)event.getProgressPercentage() + "%)";
        }
        out.println(message);
        if (type == ProgressEventType.COMPLETE)
        {
            condition.signalAll();
        }
    }
}
