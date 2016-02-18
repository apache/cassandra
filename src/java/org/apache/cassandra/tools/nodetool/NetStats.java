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

import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.Set;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "netstats", description = "Print network information on provided host (connecting node by default)")
public class NetStats extends NodeToolCmd
{
    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Override
    public void execute(NodeProbe probe)
    {
        System.out.printf("Mode: %s%n", probe.getOperationMode());
        Set<StreamState> statuses = probe.getStreamStatus();
        if (statuses.isEmpty())
            System.out.println("Not sending any streams.");
        for (StreamState status : statuses)
        {
            System.out.printf("%s %s%n", status.description, status.planId.toString());
            for (SessionInfo info : status.sessions)
            {
                System.out.printf("    %s", info.peer.toString());
                // print private IP when it is used
                if (!info.peer.equals(info.connecting))
                {
                    System.out.printf(" (using %s)", info.connecting.toString());
                }
                System.out.printf("%n");
                if (!info.receivingSummaries.isEmpty())
                {
                    if (humanReadable)
                        System.out.printf("        Receiving %d files, %s total. Already received %d files, %s total%n", info.getTotalFilesToReceive(), FileUtils.stringifyFileSize(info.getTotalSizeToReceive()), info.getTotalFilesReceived(), FileUtils.stringifyFileSize(info.getTotalSizeReceived()));
                    else
                        System.out.printf("        Receiving %d files, %d bytes total. Already received %d files, %d bytes total%n", info.getTotalFilesToReceive(), info.getTotalSizeToReceive(), info.getTotalFilesReceived(), info.getTotalSizeReceived());
                    for (ProgressInfo progress : info.getReceivingFiles())
                    {
                        System.out.printf("            %s%n", progress.toString());
                    }
                }
                if (!info.sendingSummaries.isEmpty())
                {
                    if (humanReadable)
                        System.out.printf("        Sending %d files, %s total. Already sent %d files, %s total%n", info.getTotalFilesToSend(), FileUtils.stringifyFileSize(info.getTotalSizeToSend()), info.getTotalFilesSent(), FileUtils.stringifyFileSize(info.getTotalSizeSent()));
                    else
                        System.out.printf("        Sending %d files, %d bytes total. Already sent %d files, %d bytes total%n", info.getTotalFilesToSend(), info.getTotalSizeToSend(), info.getTotalFilesSent(), info.getTotalSizeSent());
                    for (ProgressInfo progress : info.getSendingFiles())
                    {
                        System.out.printf("            %s%n", progress.toString());
                    }
                }
            }
        }

        if (!probe.isStarting())
        {
            System.out.printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", probe.getReadRepairAttempted(), probe.getReadRepairRepairedBlocking(), probe.getReadRepairRepairedBackground());

            MessagingServiceMBean ms = probe.msProxy;
            System.out.printf("%-25s", "Pool Name");
            System.out.printf("%10s", "Active");
            System.out.printf("%10s", "Pending");
            System.out.printf("%15s%n", "Completed");

            int pending;
            long completed;

            pending = 0;
            for (int n : ms.getLargeMessagePendingTasks().values())
                pending += n;
            completed = 0;
            for (long n : ms.getLargeMessageCompletedTasks().values())
                completed += n;
            System.out.printf("%-25s%10s%10s%15s%n", "Large messages", "n/a", pending, completed);

            pending = 0;
            for (int n : ms.getSmallMessagePendingTasks().values())
                pending += n;
            completed = 0;
            for (long n : ms.getSmallMessageCompletedTasks().values())
                completed += n;
            System.out.printf("%-25s%10s%10s%15s%n", "Small messages", "n/a", pending, completed);

            pending = 0;
            for (int n : ms.getGossipMessagePendingTasks().values())
                pending += n;
            completed = 0;
            for (long n : ms.getGossipMessageCompletedTasks().values())
                completed += n;
            System.out.printf("%-25s%10s%10s%15s%n", "Gossip messages", "n/a", pending, completed);
        }
    }
}