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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

public abstract class CMSAdmin extends NodeTool.NodeToolCmd
{
    @Command(name = "describe", description = "Describe the current Cluster Metadata Service")
    public static class DescribeCMS extends NodeTool.NodeToolCmd
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            Map<String, String> info = probe.getCMSOperationsProxy().describeCMS();
            output.out.printf("Cluster Metadata Service:%n");
            output.out.printf("Members: %s%n", info.get("MEMBERS"));
            output.out.printf("Is Member: %s%n", info.get("IS_MEMBER"));
            output.out.printf("Service State: %s%n", info.get("SERVICE_STATE"));
            output.out.printf("Is Migrating: %s%n", info.get("IS_MIGRATING"));
            output.out.printf("Epoch: %s%n", info.get("EPOCH"));
            output.out.printf("Local Pending Count: %s%n", info.get("LOCAL_PENDING"));
            output.out.printf("Commits Paused: %s%n", info.get("COMMITS_PAUSED"));
            output.out.printf("Replication factor: %s%n", info.get("REPLICATION_FACTOR"));
        }
    }

    @Command(name = "initialize", description = "Upgrade from gossip and initialize CMS")
    public static class InitializeCMS extends NodeTool.NodeToolCmd
    {
        @Option(title = "ignored endpoints", name = { "-i", "--ignore"}, description = "Hosts to ignore due to them being down")
        private List<String> endpoint = new ArrayList<>();

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().initializeCMS(endpoint);
        }
    }

    @Command(name = "reconfigure", description = "Reconfigure replication factor of CMS")
    public static class ReconfigureCMS extends NodeTool.NodeToolCmd
    {
        @Option(title = "status",
        name = {"--status"},
        description = "Poll status of the reconfigure command. All other flags and arguments are ignored when this one is used.")
        private boolean status = false;

        @Option(title = "resume",
        name = {"-r", "--resume"},
        description = "Whether or not a previously interrupted sequence should be resumed")
        private boolean resume = false;

        @Option(title = "cancel",
        name = {"-c", "--cancel"},
        description = "Cancels any in progress CMS reconfiguration")
        private boolean cancel = false;

        @Arguments(usage = "[<replication factor>] or <datacenter>:<replication_factor> ... ", description = "Replication factor of new CMS")
        private List<String> args = new ArrayList<>();

        @Override
        protected void execute(NodeProbe probe)
        {
            if (status)
            {
                Map<String, List<String>> status = probe.getCMSOperationsProxy().reconfigureCMSStatus();
                if (status == null)
                {
                    output.out.println("No active reconfiguration");
                }
                else
                {
                    for (Map.Entry<String, List<String>> e : status.entrySet())
                        output.out.printf("%s: %s%n", e.getKey(), e.getValue());
                }
                return;
            }
            if (resume)
            {
                if (!args.isEmpty())
                    throw new IllegalArgumentException("Replication factor should not be set if previous operation is resumed");

                probe.getCMSOperationsProxy().resumeReconfigureCms();
                return;
            }

            if (cancel)
            {
                probe.getCMSOperationsProxy().cancelReconfigureCms();
                return;
            }

            if (args.isEmpty())
                throw new IllegalArgumentException("Replication factor is empty");

            Map<String, Integer> parsedRfs = new HashMap<>(args.size());
            for (String rf : args)
            {
                if (!rf.contains(":"))
                {
                    if (args.size() > 1)
                        throw new IllegalArgumentException("Simple placement can only specify a single replication factor accross all data centers");
                    int parsedRf;
                    try
                    {
                        parsedRf = Integer.parseInt(args.get(0));
                    }
                    catch (Throwable t)
                    {
                        throw new IllegalArgumentException(String.format("Can not parse replication factor from %s", args.get(0)));
                    }
                    probe.getCMSOperationsProxy().reconfigureCMS(parsedRf);
                    return;
                }
                else
                {
                    String[] splits = rf.split(":");
                    if (splits.length > 2)
                        throw new IllegalArgumentException(String.format("Can not parse replication factor %s", rf));
                    String dc = splits[0];
                    int parsedRf;
                    try
                    {
                        parsedRf = Integer.parseInt(splits[1]);
                    }
                    catch (Throwable t)
                    {
                        throw new IllegalArgumentException(String.format("Can not parse replication factor from %s", args.get(0)));
                    }
                    parsedRfs.put(dc, parsedRf);
                }
            }

            probe.getCMSOperationsProxy().reconfigureCMS(parsedRfs);
        }
    }

    @Command(name = "snapshot", description = "Request a checkpointing snapshot of cluster metadata")
    public static class Snapshot extends NodeTool.NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().snapshotClusterMetadata();
        }
    }

    @Command(name = "unregister", description = "Unregister nodes in LEFT state")
    public static class Unregister extends NodeTool.NodeToolCmd
    {
        @Arguments(required = true, title = "Unregister nodes in LEFT state", description = "One or more nodeIds to unregister, they all need to be in LEFT state", usage = "<nodeId>+")
        public List<String> nodeIds;

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().unregisterLeftNodes(nodeIds);
        }
    }
}
