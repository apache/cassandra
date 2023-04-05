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

import java.io.PrintStream;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
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
        PrintStream out = probe.output().out;
        out.printf("Mode: %s%n", probe.getOperationMode());
        Set<StreamState> statuses = probe.getStreamStatus();
        if (statuses.isEmpty())
            out.println("Not sending any streams.");
        for (StreamState status : statuses)
        {
            out.printf("%s %s%n", status.streamOperation.getDescription(), status.planId.toString());
            for (SessionInfo info : status.sessions)
            {
                out.printf("    %s", InetAddressAndPort.toString(info.peer, printPort));
                // print private IP when it is used
                if (!info.peer.equals(info.connecting))
                {
                    out.printf(" (using %s)", InetAddressAndPort.toString(info.connecting, printPort));
                }
                out.printf("%n");
                if (!info.receivingSummaries.isEmpty())
                {
                    printReceivingSummaries(out, info, humanReadable);
                }
                if (!info.sendingSummaries.isEmpty())
                {
                    printSendingSummaries(out, info, humanReadable);
                }
            }
        }

        if (!probe.isStarting())
        {
            out.printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", probe.getReadRepairAttempted(), probe.getReadRepairRepairedBlocking(), probe.getReadRepairRepairedBackground());

            MessagingServiceMBean ms = probe.getMessagingServiceProxy();
            out.printf("%-25s", "Pool Name");
            out.printf("%10s", "Active");
            out.printf("%10s", "Pending");
            out.printf("%15s", "Completed");
            out.printf("%10s%n", "Dropped");

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
            out.printf("%-25s%10s%10s%15s%10s%n", "Large messages", "n/a", pending, completed, dropped);

            pending = 0;
            for (int n : ms.getSmallMessagePendingTasksWithPort().values())
                pending += n;
            completed = 0;
            for (long n : ms.getSmallMessageCompletedTasksWithPort().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getSmallMessageDroppedTasksWithPort().values())
                dropped += n;
            out.printf("%-25s%10s%10s%15s%10s%n", "Small messages", "n/a", pending, completed, dropped);

            pending = 0;
            for (int n : ms.getGossipMessagePendingTasksWithPort().values())
                pending += n;
            completed = 0;
            for (long n : ms.getGossipMessageCompletedTasksWithPort().values())
                completed += n;
            dropped = 0;
            for (long n : ms.getGossipMessageDroppedTasksWithPort().values())
                dropped += n;
            out.printf("%-25s%10s%10s%15s%10s%n", "Gossip messages", "n/a", pending, completed, dropped);
        }
    }

    @VisibleForTesting
    public void printReceivingSummaries(PrintStream out, SessionInfo info, boolean printHumanReadable)
    {
        long totalFilesToReceive = info.getTotalFilesToReceive();
        long totalBytesToReceive = info.getTotalSizeToReceive();
        long totalFilesReceived = info.getTotalFilesReceived();
        long totalSizeReceived = info.getTotalSizeReceived();
        double percentageFilesReceived = ((double) totalFilesReceived / totalFilesToReceive) * 100;
        double percentageSizesReceived = ((double) totalSizeReceived / totalBytesToReceive) * 100;

        out.printf("        Receiving %d files, %s total. Already received %d files (%.2f%%), %s total (%.2f%%)%n",
                   totalFilesToReceive,
                   printHumanReadable ? FileUtils.stringifyFileSize(totalBytesToReceive) : Long.toString(totalBytesToReceive) + " bytes",
                   totalFilesReceived,
                   percentageFilesReceived,
                   printHumanReadable ? FileUtils.stringifyFileSize(totalSizeReceived) : Long.toString(totalSizeReceived) + " bytes",
                   percentageSizesReceived);

        for (ProgressInfo progress : info.getReceivingFiles())
        {
            out.printf("            %s%n", progress.toString(printPort));
        }
    }

    @VisibleForTesting
    public void printSendingSummaries(PrintStream out, SessionInfo info, boolean printHumanReadable)
    {
        long totalFilesToSend = info.getTotalFilesToSend();
        long totalSizeToSend = info.getTotalSizeToSend();
        long totalFilesSent = info.getTotalFilesSent();
        long totalSizeSent = info.getTotalSizeSent();
        double percentageFilesSent = ((double) totalFilesSent / totalFilesToSend) * 100;
        double percentageSizeSent = ((double) totalSizeSent / totalSizeToSend) * 100;

        out.printf("        Sending %d files, %s total. Already sent %d files (%.2f%%), %s total (%.2f%%)%n",
                   totalFilesToSend,
                   printHumanReadable ? FileUtils.stringifyFileSize(totalSizeToSend) : Long.toString(totalSizeToSend) + " bytes",
                   totalFilesSent,
                   percentageFilesSent,
                   printHumanReadable ? FileUtils.stringifyFileSize(totalSizeSent) : Long.toString(totalSizeSent) + " bytes",
                   percentageSizeSent);

        for (ProgressInfo progress : info.getSendingFiles())
        {
            out.printf("            %s%n", progress.toString(printPort));
        }
    }
}
