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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.google.common.base.Throwables;

import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;

public class RepairRunner extends JMXNotificationProgressListener
{
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    private final PrintStream out;
    private final StorageServiceMBean ssProxy;
    private final String keyspace;
    private final Map<String, String> options;
    private final SimpleCondition condition = new SimpleCondition();

    private int cmd;
    private volatile Exception error;

    public RepairRunner(PrintStream out, StorageServiceMBean ssProxy, String keyspace, Map<String, String> options)
    {
        this.out = out;
        this.ssProxy = ssProxy;
        this.keyspace = keyspace;
        this.options = options;
    }

    public void run() throws Exception
    {
        cmd = ssProxy.repairAsync(keyspace, options);
        if (cmd <= 0)
        {
            // repairAsync can only return 0 for replication factor 1.
            String message = String.format("Replication factor is 1. No repair is needed for keyspace '%s'", keyspace);
            printMessage(message);
        }
        else
        {
            while (!condition.await(NodeProbe.JMX_NOTIFICATION_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS))
            {
                queryForCompletedRepair(String.format("After waiting for poll interval of %s seconds",
                                                      NodeProbe.JMX_NOTIFICATION_POLL_INTERVAL_SECONDS));
            }
            Exception error = this.error;
            if (error == null)
            {
                // notifications are lossy so its possible to see complete and not error; request latest state
                // from the server
                queryForCompletedRepair("condition satisfied");
                error = this.error;
            }
            if (error != null)
            {
                throw error;
            }
        }
    }

    @Override
    public boolean isInterestedIn(String tag)
    {
        return tag.equals("repair:" + cmd);
    }

    @Override
    public void handleNotificationLost(long timestamp, String message)
    {
        if (cmd > 0)
        {
            // Check to see if the lost notification was a completion message
            queryForCompletedRepair("After receiving lost notification");
        }
    }

    @Override
    public void handleConnectionClosed(long timestamp, String message)
    {
        handleConnectionFailed(timestamp, message);
    }

    @Override
    public void handleConnectionFailed(long timestamp, String message)
    {
        error = new IOException(String.format("[%s] JMX connection closed. You should check server log for repair status of keyspace %s"
                                              + "(Subsequent keyspaces are not going to be repaired).",
                                              format.format(timestamp), keyspace));
        condition.signalAll();
    }

    @Override
    public void progress(String tag, ProgressEvent event)
    {
        ProgressEventType type = event.getType();
        String message = event.getMessage();
        if (type == ProgressEventType.PROGRESS)
        {
            message = message + " (progress: " + (int) event.getProgressPercentage() + "%)";
        }
        printMessage(message);
        if (type == ProgressEventType.ERROR)
        {
            error = new RuntimeException(String.format("Repair job has failed with the error message: %s. " +
                                                       "Check the logs on the repair participants for further details",
                                                       message));
        }
        if (type == ProgressEventType.COMPLETE)
        {
            condition.signalAll();
        }
    }


    private void queryForCompletedRepair(String triggeringCondition)
    {
        List<String> status = ssProxy.getParentRepairStatus(cmd);
        String queriedString = "queried for parent session status and";
        if (status == null)
        {
            String message = String.format("%s %s couldn't find repair status for cmd: %s", triggeringCondition,
                                           queriedString, cmd);
            printMessage(message);
        }
        else
        {
            ActiveRepairService.ParentRepairStatus parentRepairStatus = ActiveRepairService.ParentRepairStatus.valueOf(status.get(0));
            List<String> messages = status.subList(1, status.size());
            switch (parentRepairStatus)
            {
                case COMPLETED:
                case FAILED:
                    printMessage(String.format("%s %s discovered repair %s.",
                                              triggeringCondition,
                                              queriedString, parentRepairStatus.name().toLowerCase()));
                    if (parentRepairStatus == ActiveRepairService.ParentRepairStatus.FAILED)
                    {
                        error = new IOException(messages.get(0));
                    }
                    printMessages(messages);
                    condition.signalAll();
                    break;
                case IN_PROGRESS:
                    break;
                default:
                    printMessage(String.format("WARNING Encountered unexpected RepairRunnable.ParentRepairStatus: %s", parentRepairStatus));
                    printMessages(messages);
                    break;
            }
        }
    }

    private void printMessages(List<String> messages)
    {
        for (String message : messages)
        {
            printMessage(message);
        }
    }

    private void printMessage(String message)
    {
        out.println(String.format("[%s] %s", this.format.format(System.currentTimeMillis()), message));
    }
}
