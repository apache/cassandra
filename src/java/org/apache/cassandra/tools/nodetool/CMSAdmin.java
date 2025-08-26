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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tcm.CMSOperations.COMMITS_PAUSED;
import static org.apache.cassandra.tcm.CMSOperations.EPOCH;
import static org.apache.cassandra.tcm.CMSOperations.IS_MEMBER;
import static org.apache.cassandra.tcm.CMSOperations.IS_MIGRATING;
import static org.apache.cassandra.tcm.CMSOperations.LOCAL_PENDING;
import static org.apache.cassandra.tcm.CMSOperations.MEMBERS;
import static org.apache.cassandra.tcm.CMSOperations.NEEDS_RECONFIGURATION;
import static org.apache.cassandra.tcm.CMSOperations.REPLICATION_FACTOR;
import static org.apache.cassandra.tcm.CMSOperations.SERVICE_STATE;

@Command(name = "cms", description = "Manage cluster metadata",
         subcommands = { CMSAdmin.DescribeCMS.class,
                         CMSAdmin.InitializeCMS.class,
                         CMSAdmin.ReconfigureCMS.class,
                         CMSAdmin.Snapshot.class,
                         CMSAdmin.Unregister.class,
                         CMSAdmin.AbortInitialization.class,
                         CMSAdmin.DumpDirectory.class,
                         CMSAdmin.DumpLog.class,
                         CMSAdmin.ResumeDropAccordTable.class })
public class CMSAdmin extends AbstractCommand
{
    @Override
    protected void execute(NodeProbe probe)
    {
        AbstractCommand cmd = new DescribeCMS();
        cmd.probe(probe);
        cmd.logger(output);
        cmd.run();
    }

    @Command(name = "describe", description = "Describe the current Cluster Metadata Service")
    public static class DescribeCMS extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            Map<String, String> info = probe.getCMSOperationsProxy().describeCMS();
            output.out.printf("Cluster Metadata Service:%n");
            output.out.printf("Members: %s%n", info.get(MEMBERS));
            output.out.printf("Needs reconfiguration: %s%n", info.get(NEEDS_RECONFIGURATION));
            output.out.printf("Is Member: %s%n", info.get(IS_MEMBER));
            output.out.printf("Service State: %s%n", info.get(SERVICE_STATE));
            output.out.printf("Is Migrating: %s%n", info.get(IS_MIGRATING));
            output.out.printf("Epoch: %s%n", info.get(EPOCH));
            output.out.printf("Local Pending Count: %s%n", info.get(LOCAL_PENDING));
            output.out.printf("Commits Paused: %s%n", info.get(COMMITS_PAUSED));
            output.out.printf("Replication factor: %s%n", info.get(REPLICATION_FACTOR));
        }
    }

    @Command(name = "initialize", description = "Upgrade from gossip and initialize CMS")
    public static class InitializeCMS extends AbstractCommand
    {
        @Option(paramLabel = "ignored_endpoints", names = { "-i", "--ignore" }, description = "Hosts to ignore due to them being down")
        private List<String> endpoint = new ArrayList<>();

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().initializeCMS(endpoint);
        }
    }

    @Command(name = "reconfigure", description = "Reconfigure replication factor of CMS")
    public static class ReconfigureCMS extends AbstractCommand
    {
        @Option(paramLabel = "status",
                names = { "--status" },
                description = "Poll status of the reconfigure command. All other flags and arguments are ignored when this one is used.")
        private boolean status = false;

        @Option(paramLabel = "resume",
                names = { "-r", "--resume" },
                description = "Whether or not a previously interrupted sequence should be resumed")
        private boolean resume = false;

        @Option(paramLabel = "cancel",
                names = { "-c", "--cancel" },
                description = "Cancels any in progress CMS reconfiguration")
        private boolean cancel = false;

        @CassandraUsage(usage = "[<replication factor>] or <datacenter>:<replication_factor> ... ", description = "Replication factor of new CMS")
        @Parameters(paramLabel = "replication_factor", description = "Replication factors of new CMS in format <replication factor> or <datacenter>:<replication_factor>")
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
    public static class Snapshot extends AbstractCommand
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().snapshotClusterMetadata();
        }
    }

    @Command(name = "unregister", description = "Unregister nodes in LEFT state")
    public static class Unregister extends AbstractCommand
    {
        @Parameters(paramLabel = "nodeId", description = "One or more nodeIds to unregister, they all need to be in LEFT state", arity = "1..*")
        private List<String> nodeIds;

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().unregisterLeftNodes(nodeIds);
        }
    }

    @Command(name = "abortinitialization", description = "Abort an incomplete initialization")
    public static class AbortInitialization extends AbstractCommand
    {
        @Option(required = true, names = { "--initiator" }, description = "The address of the node where `cms initialize` was run.")
        public String initiator;

        @Override
        protected void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().abortInitialization(initiator);
        }
    }

    @Command(name = "dumpdirectory", description = "Dump the directory from the current ClusterMetadata")
    public static class DumpDirectory extends AbstractCommand
    {
        @Option(names = { "--tokens" }, description = "Include tokens in output")
        public boolean tokens = false;

        @Override
        protected void execute(NodeProbe probe)
        {
            output(probe.output().out, "NodeId", probe.getCMSOperationsProxy().dumpDirectory(tokens));
        }
    }

    @Command(name = "dumplog", description = "Dump the metadata log")
    public static class DumpLog extends AbstractCommand
    {
        @Option(names = { "--start" }, description = "Start epoch")
        long startEpoch = Epoch.FIRST.getEpoch();
        @Option(names = { "--end" }, description = "End epoch")
        long endEpoch = Long.MAX_VALUE;

        @Override
        protected void execute(NodeProbe probe)
        {
            output(probe.output().out, "Epoch", probe.getCMSOperationsProxy().dumpLog(startEpoch, endEpoch));
        }
    }

    private static void output(PrintStream out, String title, Map<Long, Map<String, String>> map)
    {
        if (map.isEmpty())
            return;
        int keywidth = keywidth(map);
        for (Long key : ImmutableList.sortedCopyOf(map.keySet()))
        {
            out.println(title + ": " + key);
            for (Map.Entry<String, String> nodeEntry : map.get(key).entrySet())
                out.printf("  %-" + keywidth + "s%s%n", nodeEntry.getKey(), nodeEntry.getValue());
        }
    }

    private static int keywidth(Map<?, Map<String, String>> map)
    {
        assert !map.isEmpty();
        return map.entrySet().iterator().next().getValue().keySet().stream().max(Comparator.comparingInt(String::length)).get().length() + 1;
    }

    @Command(name = "resumedropaccordtable", description = "Resume a drop accord table operation which has stalled")
    public static class ResumeDropAccordTable extends AbstractCommand
    {
        @Parameters(description = "Table id of the table being dropped")
        private String tableId;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.getCMSOperationsProxy().resumeDropAccordTable(tableId);
        }
    }
}
