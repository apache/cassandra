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

import io.airlift.airline.Command;
import io.airlift.airline.Option;

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
            System.out.printf("%s %s%n", status.streamOperation.getDescription(), status.planId.toString());
            for (SessionInfo info : status.sessions)
            {
                System.out.printf("    %s", info.peer.toString(printPort));
                // print private IP when it is used
                if (!info.peer.equals(info.connecting))
                {
                    System.out.printf(" (using %s)", info.connecting.toString(printPort));
                }
                System.out.printf("%n");
                if (!info.receivingSummaries.isEmpty())
                {
                    long totalFilesToReceive = info.getTotalFilesToReceive();
                    long totalBytesToReceive = info.getTotalBytesToReceive();
                    long totalFilesReceived = info.getTotalFilesReceived();
                    long totalSizeReceived = info.getTotalSizeReceived();
                    double percentageFilesReceived = ((double) totalFilesReceived / totalFilesToReceive) * 100;
                    double percentageSizesReceived = ((double) totalSizeReceived / totalBytesToReceive) * 100;

                    if (humanReadable)
                        System.out.printf("        Receiving %d files, %s total. Already received %d files (%.2f%%), %s total (%.2f%%)%n",
                                          totalFilesToReceive,
                                          FileUtils.stringifyFileSize(totalBytesToReceive),
                                          totalFilesReceived,
                                          percentageFilesReceived,
                                          FileUtils.stringifyFileSize(totalSizeReceived),
                                          percentageSizesReceived);
                    else
                        System.out.printf("        Receiving %d files, %d bytes total. Already received %d files (%.2f%%), %d bytes total (%.2f%%)%n",
                                          totalFilesToReceive,
                                          totalBytesToReceive,
                                          totalFilesReceived,
                                          percentageFilesReceived,
                                          totalSizeReceived,
                                          percentageSizesReceived);
                    for (ProgressInfo progress : info.getReceivingFiles())
                    {
                        System.out.printf("            %s%n", progress.toString(printPort));
                    }
                }
                if (!info.sendingSummaries.isEmpty())
                {
                    long totalFilesToSend = info.getTotalFilesToSend();
                    long totalSizeToSend = info.getTotalSizeToSend();
                    long totalFilesSent = info.getTotalFilesSent();
                    long totalSizeSent = info.getTotalSizeSent();
                    double percentageFilesSent = ((double) totalFilesSent / totalFilesToSend) * 100;
                    double percentageSizeSent = ((double) totalSizeSent / totalSizeToSend) * 100;

                    if (humanReadable)
                        System.out.printf("        Sending %d files, %s total. Already sent %d files (%.2f%%), %s total (%.2f%%)%n",
                                          totalFilesToSend,
                                          FileUtils.stringifyFileSize(totalSizeToSend),
                                          totalFilesSent,
                                          percentageFilesSent,
                                          FileUtils.stringifyFileSize(totalSizeSent),
                                          percentageSizeSent);
                    else
                        System.out.printf("        Sending %d files, %d bytes total. Already sent %d files (%.2f%%), %d bytes total (%.2f%%) %n",
                                          totalFilesToSend,
                                          totalSizeToSend,
                                          totalFilesSent,
                                          percentageFilesSent,
                                          totalSizeSent,
                                          percentageSizeSent);
                    for (ProgressInfo progress : info.getSendingFiles())
                    {
                        System.out.printf("            %s%n", progress.toString(printPort));
                    }
                }
            }
        }

        if (!probe.isStarting())
        {
            System.out.printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", probe.getReadRepairAttempted(), probe.getReadRepairRepairedBlocking(), probe.getReadRepairRepairedBackground());

            MessagingServiceMBean ms = probe.getMessagingServiceProxy();
            System.out.printf("%-25s", "Pool Name");
            System.out.printf("%10s", "Active");
            System.out.printf("%10s", "Pending");
            System.out.printf("%15s", "Completed");
            System.out.printf("%10s%n", "Dropped");

            int pending;
            long completed;
            long dropped;

            pending = 0;
            for (int n : ms.getLargeMessagePendingTasksWithPort().values())
                pending += n;
            completed = 0;
            for (long n : ms.getLargeMessageCompletedTasksWithPort().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getLargeMessageDroppedTasksWithPort().values())
                dropped += n;
            System.out.printf("%-25s%10s%10s%15s%10s%n", "Large messages", "n/a", pending, completed, dropped);

            pending = 0;
            for (int n : ms.getSmallMessagePendingTasksWithPort().values())
                pending += n;
            completed = 0;
            for (long n : ms.getSmallMessageCompletedTasksWithPort().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getSmallMessageDroppedTasksWithPort().values())
                dropped += n;
            System.out.printf("%-25s%10s%10s%15s%10s%n", "Small messages", "n/a", pending, completed, dropped);

            pending = 0;
            for (int n : ms.getGossipMessagePendingTasksWithPort().values())
                pending += n;
            completed = 0;
            for (long n : ms.getGossipMessageCompletedTasksWithPort().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getGossipMessageDroppedTasksWithPort().values())
                dropped += n;
            System.out.printf("%-25s%10s%10s%15s%10s%n", "Gossip messages", "n/a", pending, completed, dropped);
        }
    }
}
