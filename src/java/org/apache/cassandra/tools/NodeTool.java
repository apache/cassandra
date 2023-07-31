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

import java.io.Console;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.SortedMap;

import javax.management.InstanceNotFoundException;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.nodetool.Assassinate;
import org.apache.cassandra.tools.nodetool.BootstrapResume;
import org.apache.cassandra.tools.nodetool.CIDRFilteringStats;
import org.apache.cassandra.tools.nodetool.CfHistograms;
import org.apache.cassandra.tools.nodetool.CfStats;
import org.apache.cassandra.tools.nodetool.Cleanup;
import org.apache.cassandra.tools.nodetool.ClearSnapshot;
import org.apache.cassandra.tools.nodetool.ClientStats;
import org.apache.cassandra.tools.nodetool.Compact;
import org.apache.cassandra.tools.nodetool.CompactionHistory;
import org.apache.cassandra.tools.nodetool.CompactionStats;
import org.apache.cassandra.tools.nodetool.DataPaths;
import org.apache.cassandra.tools.nodetool.Decommission;
import org.apache.cassandra.tools.nodetool.DescribeCluster;
import org.apache.cassandra.tools.nodetool.DescribeRing;
import org.apache.cassandra.tools.nodetool.DisableAuditLog;
import org.apache.cassandra.tools.nodetool.DisableAutoCompaction;
import org.apache.cassandra.tools.nodetool.DisableBackup;
import org.apache.cassandra.tools.nodetool.DisableBinary;
import org.apache.cassandra.tools.nodetool.DisableFullQueryLog;
import org.apache.cassandra.tools.nodetool.DisableGossip;
import org.apache.cassandra.tools.nodetool.DisableHandoff;
import org.apache.cassandra.tools.nodetool.DisableHintsForDC;
import org.apache.cassandra.tools.nodetool.DisableOldProtocolVersions;
import org.apache.cassandra.tools.nodetool.Drain;
import org.apache.cassandra.tools.nodetool.DropCIDRGroup;
import org.apache.cassandra.tools.nodetool.EnableAuditLog;
import org.apache.cassandra.tools.nodetool.EnableAutoCompaction;
import org.apache.cassandra.tools.nodetool.EnableBackup;
import org.apache.cassandra.tools.nodetool.EnableBinary;
import org.apache.cassandra.tools.nodetool.EnableFullQueryLog;
import org.apache.cassandra.tools.nodetool.EnableGossip;
import org.apache.cassandra.tools.nodetool.EnableHandoff;
import org.apache.cassandra.tools.nodetool.EnableHintsForDC;
import org.apache.cassandra.tools.nodetool.EnableOldProtocolVersions;
import org.apache.cassandra.tools.nodetool.FailureDetectorInfo;
import org.apache.cassandra.tools.nodetool.Flush;
import org.apache.cassandra.tools.nodetool.ForceCompact;
import org.apache.cassandra.tools.nodetool.GarbageCollect;
import org.apache.cassandra.tools.nodetool.GcStats;
import org.apache.cassandra.tools.nodetool.GetAuditLog;
import org.apache.cassandra.tools.nodetool.GetAuthCacheConfig;
import org.apache.cassandra.tools.nodetool.GetBatchlogReplayTrottle;
import org.apache.cassandra.tools.nodetool.GetCIDRGroupsOfIP;
import org.apache.cassandra.tools.nodetool.GetColumnIndexSize;
import org.apache.cassandra.tools.nodetool.GetCompactionThreshold;
import org.apache.cassandra.tools.nodetool.GetCompactionThroughput;
import org.apache.cassandra.tools.nodetool.GetConcurrency;
import org.apache.cassandra.tools.nodetool.GetConcurrentCompactors;
import org.apache.cassandra.tools.nodetool.GetConcurrentViewBuilders;
import org.apache.cassandra.tools.nodetool.GetDefaultKeyspaceRF;
import org.apache.cassandra.tools.nodetool.GetEndpoints;
import org.apache.cassandra.tools.nodetool.GetFullQueryLog;
import org.apache.cassandra.tools.nodetool.GetInterDCStreamThroughput;
import org.apache.cassandra.tools.nodetool.GetLoggingLevels;
import org.apache.cassandra.tools.nodetool.GetMaxHintWindow;
import org.apache.cassandra.tools.nodetool.GetSSTables;
import org.apache.cassandra.tools.nodetool.GetSeeds;
import org.apache.cassandra.tools.nodetool.GetSnapshotThrottle;
import org.apache.cassandra.tools.nodetool.GetStreamThroughput;
import org.apache.cassandra.tools.nodetool.GetTimeout;
import org.apache.cassandra.tools.nodetool.GetTraceProbability;
import org.apache.cassandra.tools.nodetool.GossipInfo;
import org.apache.cassandra.tools.nodetool.Import;
import org.apache.cassandra.tools.nodetool.Info;
import org.apache.cassandra.tools.nodetool.InvalidateCIDRPermissionsCache;
import org.apache.cassandra.tools.nodetool.InvalidateCounterCache;
import org.apache.cassandra.tools.nodetool.InvalidateCredentialsCache;
import org.apache.cassandra.tools.nodetool.InvalidateJmxPermissionsCache;
import org.apache.cassandra.tools.nodetool.InvalidateKeyCache;
import org.apache.cassandra.tools.nodetool.InvalidateNetworkPermissionsCache;
import org.apache.cassandra.tools.nodetool.InvalidatePermissionsCache;
import org.apache.cassandra.tools.nodetool.InvalidateRolesCache;
import org.apache.cassandra.tools.nodetool.InvalidateRowCache;
import org.apache.cassandra.tools.nodetool.Join;
import org.apache.cassandra.tools.nodetool.ListPendingHints;
import org.apache.cassandra.tools.nodetool.ListSnapshots;
import org.apache.cassandra.tools.nodetool.Move;
import org.apache.cassandra.tools.nodetool.NetStats;
import org.apache.cassandra.tools.nodetool.PauseHandoff;
import org.apache.cassandra.tools.nodetool.ProfileLoad;
import org.apache.cassandra.tools.nodetool.ProxyHistograms;
import org.apache.cassandra.tools.nodetool.RangeKeySample;
import org.apache.cassandra.tools.nodetool.Rebuild;
import org.apache.cassandra.tools.nodetool.RebuildIndex;
import org.apache.cassandra.tools.nodetool.RecompressSSTables;
import org.apache.cassandra.tools.nodetool.Refresh;
import org.apache.cassandra.tools.nodetool.RefreshSizeEstimates;
import org.apache.cassandra.tools.nodetool.ReloadCIDRGroupsCache;
import org.apache.cassandra.tools.nodetool.ReloadLocalSchema;
import org.apache.cassandra.tools.nodetool.ReloadSeeds;
import org.apache.cassandra.tools.nodetool.ReloadTriggers;
import org.apache.cassandra.tools.nodetool.RelocateSSTables;
import org.apache.cassandra.tools.nodetool.RemoveNode;
import org.apache.cassandra.tools.nodetool.Repair;
import org.apache.cassandra.tools.nodetool.RepairAdmin;
import org.apache.cassandra.tools.nodetool.ReplayBatchlog;
import org.apache.cassandra.tools.nodetool.ResetFullQueryLog;
import org.apache.cassandra.tools.nodetool.ResetLocalSchema;
import org.apache.cassandra.tools.nodetool.ResumeHandoff;
import org.apache.cassandra.tools.nodetool.Ring;
import org.apache.cassandra.tools.nodetool.Scrub;
import org.apache.cassandra.tools.nodetool.SetAuthCacheConfig;
import org.apache.cassandra.tools.nodetool.SetBatchlogReplayThrottle;
import org.apache.cassandra.tools.nodetool.SetCacheCapacity;
import org.apache.cassandra.tools.nodetool.SetCacheKeysToSave;
import org.apache.cassandra.tools.nodetool.SetColumnIndexSize;
import org.apache.cassandra.tools.nodetool.SetCompactionThreshold;
import org.apache.cassandra.tools.nodetool.SetCompactionThroughput;
import org.apache.cassandra.tools.nodetool.SetConcurrency;
import org.apache.cassandra.tools.nodetool.SetConcurrentCompactors;
import org.apache.cassandra.tools.nodetool.SetConcurrentViewBuilders;
import org.apache.cassandra.tools.nodetool.SetDefaultKeyspaceRF;
import org.apache.cassandra.tools.nodetool.SetHintedHandoffThrottleInKB;
import org.apache.cassandra.tools.nodetool.SetHostStatWithPort;
import org.apache.cassandra.tools.nodetool.SetInterDCStreamThroughput;
import org.apache.cassandra.tools.nodetool.SetLoggingLevel;
import org.apache.cassandra.tools.nodetool.SetMaxHintWindow;
import org.apache.cassandra.tools.nodetool.SetSnapshotThrottle;
import org.apache.cassandra.tools.nodetool.SetStreamThroughput;
import org.apache.cassandra.tools.nodetool.SetTimeout;
import org.apache.cassandra.tools.nodetool.SetTraceProbability;
import org.apache.cassandra.tools.nodetool.Sjk;
import org.apache.cassandra.tools.nodetool.Snapshot;
import org.apache.cassandra.tools.nodetool.Status;
import org.apache.cassandra.tools.nodetool.StatusAutoCompaction;
import org.apache.cassandra.tools.nodetool.StatusBackup;
import org.apache.cassandra.tools.nodetool.StatusBinary;
import org.apache.cassandra.tools.nodetool.StatusGossip;
import org.apache.cassandra.tools.nodetool.StatusHandoff;
import org.apache.cassandra.tools.nodetool.Stop;
import org.apache.cassandra.tools.nodetool.StopDaemon;
import org.apache.cassandra.tools.nodetool.TableHistograms;
import org.apache.cassandra.tools.nodetool.TableStats;
import org.apache.cassandra.tools.nodetool.TopPartitions;
import org.apache.cassandra.tools.nodetool.TpStats;
import org.apache.cassandra.tools.nodetool.TruncateHints;
import org.apache.cassandra.tools.nodetool.UpdateCIDRGroup;
import org.apache.cassandra.tools.nodetool.UpgradeSSTable;
import org.apache.cassandra.tools.nodetool.Verify;
import org.apache.cassandra.tools.nodetool.Version;
import org.apache.cassandra.tools.nodetool.ViewBuildStatus;
import org.apache.cassandra.utils.FBUtilities;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.cassandra.io.util.File.WriteMode.APPEND;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class NodeTool
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final String HISTORYFILE = "nodetool.history";

    private final INodeProbeFactory nodeProbeFactory;
    private final Output output;

    public static void main(String... args)
    {
        System.exit(new NodeTool(new NodeProbeFactory(), Output.CONSOLE).execute(args));
    }

    public NodeTool(INodeProbeFactory nodeProbeFactory, Output output)
    {
        this.nodeProbeFactory = nodeProbeFactory;
        this.output = output;
    }

    public int execute(String... args)
    {
        List<Class<? extends NodeToolCmdRunnable>> commands = newArrayList(
                Assassinate.class,
                CassHelp.class,
                CfHistograms.class,
                CfStats.class,
                CIDRFilteringStats.class,
                Cleanup.class,
                ClearSnapshot.class,
                ClientStats.class,
                Compact.class,
                CompactionHistory.class,
                CompactionStats.class,
                DataPaths.class,
                Decommission.class,
                DescribeCluster.class,
                DescribeRing.class,
                DisableAuditLog.class,
                DisableAutoCompaction.class,
                DisableBackup.class,
                DisableBinary.class,
                DisableFullQueryLog.class,
                DisableGossip.class,
                DisableHandoff.class,
                DisableHintsForDC.class,
                DisableOldProtocolVersions.class,
                Drain.class,
                DropCIDRGroup.class,
                EnableAuditLog.class,
                EnableAutoCompaction.class,
                EnableBackup.class,
                EnableBinary.class,
                EnableFullQueryLog.class,
                EnableGossip.class,
                EnableHandoff.class,
                EnableHintsForDC.class,
                EnableOldProtocolVersions.class,
                FailureDetectorInfo.class,
                Flush.class,
                GarbageCollect.class,
                GcStats.class,
                GetAuditLog.class,
                GetAuthCacheConfig.class,
                GetBatchlogReplayTrottle.class,
                GetCIDRGroupsOfIP.class,
                GetColumnIndexSize.class,
                GetCompactionThreshold.class,
                GetCompactionThroughput.class,
                GetConcurrency.class,
                GetConcurrentCompactors.class,
                GetConcurrentViewBuilders.class,
                GetDefaultKeyspaceRF.class,
                GetEndpoints.class,
                GetFullQueryLog.class,
                GetInterDCStreamThroughput.class,
                GetLoggingLevels.class,
                GetMaxHintWindow.class,
                GetSSTables.class,
                GetSeeds.class,
                GetSnapshotThrottle.class,
                GetStreamThroughput.class,
                GetTimeout.class,
                GetTraceProbability.class,
                GossipInfo.class,
                Import.class,
                Info.class,
                InvalidateCIDRPermissionsCache.class,
                InvalidateCounterCache.class,
                InvalidateCredentialsCache.class,
                InvalidateJmxPermissionsCache.class,
                ReloadCIDRGroupsCache.class,
                InvalidateKeyCache.class,
                InvalidateNetworkPermissionsCache.class,
                InvalidatePermissionsCache.class,
                InvalidateRolesCache.class,
                InvalidateRowCache.class,
                Join.class,
                ListCIDRGroups.class,
                ListPendingHints.class,
                ListSnapshots.class,
                Move.class,
                NetStats.class,
                PauseHandoff.class,
                ProfileLoad.class,
                ProxyHistograms.class,
                RangeKeySample.class,
                Rebuild.class,
                RebuildIndex.class,
                RecompressSSTables.class,
                Refresh.class,
                RefreshSizeEstimates.class,
                ReloadLocalSchema.class,
                ReloadSeeds.class,
                ReloadSslCertificates.class,
                ReloadTriggers.class,
                RelocateSSTables.class,
                RemoveNode.class,
                Repair.class,
                ReplayBatchlog.class,
                ResetFullQueryLog.class,
                ResetLocalSchema.class,
                ResumeHandoff.class,
                Ring.class,
                Scrub.class,
                SetAuthCacheConfig.class,
                SetBatchlogReplayThrottle.class,
                SetCacheCapacity.class,
                SetCacheKeysToSave.class,
                SetColumnIndexSize.class,
                SetCompactionThreshold.class,
                SetCompactionThroughput.class,
                SetConcurrency.class,
                SetConcurrentCompactors.class,
                SetConcurrentViewBuilders.class,
                SetDefaultKeyspaceRF.class,
                SetHintedHandoffThrottleInKB.class,
                SetInterDCStreamThroughput.class,
                SetLoggingLevel.class,
                SetMaxHintWindow.class,
                SetSnapshotThrottle.class,
                SetStreamThroughput.class,
                SetTimeout.class,
                SetTraceProbability.class,
                Sjk.class,
                Snapshot.class,
                Status.class,
                StatusAutoCompaction.class,
                StatusBackup.class,
                StatusBinary.class,
                StatusGossip.class,
                StatusHandoff.class,
                Stop.class,
                StopDaemon.class,
                TableHistograms.class,
                TableStats.class,
                TopPartitions.class,
                TpStats.class,
                TruncateHints.class,
                UpdateCIDRGroup.class,
                UpgradeSSTable.class,
                Verify.class,
                Version.class,
                ViewBuildStatus.class,
                ForceCompact.class
        );

        Cli.CliBuilder<NodeToolCmdRunnable> builder = Cli.builder("nodetool");

        builder.withDescription("Manage your Cassandra cluster")
                 .withDefaultCommand(CassHelp.class)
                 .withCommands(commands);

        // bootstrap commands
        builder.withGroup("bootstrap")
                .withDescription("Monitor/manage node's bootstrap process")
                .withDefaultCommand(CassHelp.class)
                .withCommand(BootstrapResume.class);

        builder.withGroup("repair_admin")
               .withDescription("list and fail incremental repair sessions")
               .withDefaultCommand(RepairAdmin.ListCmd.class)
               .withCommand(RepairAdmin.ListCmd.class)
               .withCommand(RepairAdmin.CancelCmd.class)
               .withCommand(RepairAdmin.CleanupDataCmd.class)
               .withCommand(RepairAdmin.SummarizePendingCmd.class)
               .withCommand(RepairAdmin.SummarizeRepairedCmd.class);

        Cli<NodeToolCmdRunnable> parser = builder.build();

        int status = 0;
        try
        {
            NodeToolCmdRunnable parse = parser.parse(args);
            printHistory(args);
            parse.run(nodeProbeFactory, output);
        } catch (IllegalArgumentException |
                IllegalStateException |
                ParseArgumentsMissingException |
                ParseArgumentsUnexpectedException |
                ParseOptionConversionException |
                ParseOptionMissingException |
                ParseOptionMissingValueException |
                ParseCommandMissingException |
                ParseCommandUnrecognizedException e)
        {
            badUse(e);
            status = 1;
        } catch (Throwable throwable)
        {
            err(Throwables.getRootCause(throwable));
            status = 2;
        }

        return status;
    }

    private static void printHistory(String... args)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;

        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

        try (FileWriter writer = new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE).newWriter(APPEND))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    protected void badUse(Exception e)
    {
        output.out.println("nodetool: " + e.getMessage());
        output.out.println("See 'nodetool help' or 'nodetool help <command>'.");
    }

    protected void err(Throwable e)
    {
        // CASSANDRA-11537: friendly error message when server is not ready
        if (e instanceof InstanceNotFoundException)
            throw new IllegalArgumentException("Server is not initialized yet, cannot run nodetool.");

        output.err.println("error: " + e.getMessage());
        output.err.println("-- StackTrace --");
        output.err.println(getStackTraceAsString(e));
    }

    public static class CassHelp extends Help implements NodeToolCmdRunnable
    {
        public void run(INodeProbeFactory nodeProbeFactory, Output output)
        {
            run();
        }
    }

    interface NodeToolCmdRunnable
    {
        void run(INodeProbeFactory nodeProbeFactory, Output output);
    }

    public static abstract class NodeToolCmd implements NodeToolCmdRunnable
    {

        @Option(type = OptionType.GLOBAL, name = {"-h", "--host"}, description = "Node hostname or ip address")
        private String host = "127.0.0.1";

        @Option(type = OptionType.GLOBAL, name = {"-p", "--port"}, description = "Remote jmx agent port number")
        private String port = "7199";

        @Option(type = OptionType.GLOBAL, name = {"-u", "--username"}, description = "Remote jmx agent username")
        private String username = EMPTY;

        @Option(type = OptionType.GLOBAL, name = {"-pw", "--password"}, description = "Remote jmx agent password")
        private String password = EMPTY;

        @Option(type = OptionType.GLOBAL, name = {"-pwf", "--password-file"}, description = "Path to the JMX password file")
        private String passwordFilePath = EMPTY;

        @Option(type = OptionType.GLOBAL, name = { "-pp", "--print-port"}, description = "Operate in 4.0 mode with hosts disambiguated by port number", arity = 0)
        protected boolean printPort = false;

        private INodeProbeFactory nodeProbeFactory;
        protected Output output;

        @Override
        public void run(INodeProbeFactory nodeProbeFactory, Output output)
        {
            this.nodeProbeFactory = nodeProbeFactory;
            this.output = output;
            runInternal();
        }

        public void runInternal()
        {
            if (isNotEmpty(username)) {
                if (isNotEmpty(passwordFilePath))
                    password = readUserPasswordFromFile(username, passwordFilePath);

                if (isEmpty(password))
                    password = promptAndReadPassword();
            }

            try (NodeProbe probe = connect())
            {
                execute(probe);
                if (probe.isFailed())
                    throw new RuntimeException("nodetool failed, check server logs");
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error while closing JMX connection", e);
            }

        }

        private String readUserPasswordFromFile(String username, String passwordFilePath) {
            String password = EMPTY;

            File passwordFile = new File(passwordFilePath);
            try (Scanner scanner = new Scanner(passwordFile.toJavaIOFile()).useDelimiter("\\s+"))
            {
                while (scanner.hasNextLine())
                {
                    if (scanner.hasNext())
                    {
                        String jmxRole = scanner.next();
                        if (jmxRole.equals(username) && scanner.hasNext())
                        {
                            password = scanner.next();
                            break;
                        }
                    }
                    scanner.nextLine();
                }
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }

            return password;
        }

        private String promptAndReadPassword()
        {
            String password = EMPTY;

            Console console = System.console();
            if (console != null)
                password = String.valueOf(console.readPassword("Password:"));

            return password;
        }

        protected abstract void execute(NodeProbe probe);

        private NodeProbe connect()
        {
            NodeProbe nodeClient = null;

            try
            {
                if (username.isEmpty())
                    nodeClient = nodeProbeFactory.create(host, parseInt(port));
                else
                    nodeClient = nodeProbeFactory.create(host, parseInt(port), username, password);

                nodeClient.setOutput(output);
            } catch (IOException | SecurityException e)
            {
                Throwable rootCause = Throwables.getRootCause(e);
                output.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
                System.exit(1);
            }

            return nodeClient;
        }

        protected enum KeyspaceSet
        {
            ALL, NON_SYSTEM, NON_LOCAL_STRATEGY
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe)
        {
            return parseOptionalKeyspace(cmdArgs, nodeProbe, KeyspaceSet.ALL);
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe, KeyspaceSet defaultKeyspaceSet)
        {
            List<String> keyspaces = new ArrayList<>();


            if (cmdArgs == null || cmdArgs.isEmpty())
            {
                if (defaultKeyspaceSet == KeyspaceSet.NON_LOCAL_STRATEGY)
                    keyspaces.addAll(keyspaces = nodeProbe.getNonLocalStrategyKeyspaces());
                else if (defaultKeyspaceSet == KeyspaceSet.NON_SYSTEM)
                    keyspaces.addAll(keyspaces = nodeProbe.getNonSystemKeyspaces());
                else
                    keyspaces.addAll(nodeProbe.getKeyspaces());
            }
            else
            {
                keyspaces.add(cmdArgs.get(0));
            }

            for (String keyspace : keyspaces)
            {
                if (!nodeProbe.getKeyspaces().contains(keyspace))
                    throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
            }

            return Collections.unmodifiableList(keyspaces);
        }

        protected String[] parseOptionalTables(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
        }

        protected String[] parsePartitionKeys(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 2 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(2, cmdArgs.size()), String.class);
        }
    }

    public static SortedMap<String, SetHostStatWithPort> getOwnershipByDcWithPort(NodeProbe probe, boolean resolveIp,
                                                                  Map<String, String> tokenToEndpoint,
                                                                  Map<String, Float> ownerships)
    {
        SortedMap<String, SetHostStatWithPort> ownershipByDc = Maps.newTreeMap();
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        try
        {
            for (Entry<String, String> tokenAndEndPoint : tokenToEndpoint.entrySet())
            {
                String dc = epSnitchInfo.getDatacenter(tokenAndEndPoint.getValue());
                if (!ownershipByDc.containsKey(dc))
                    ownershipByDc.put(dc, new SetHostStatWithPort(resolveIp));
                ownershipByDc.get(dc).add(tokenAndEndPoint.getKey(), tokenAndEndPoint.getValue(), ownerships);
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        return ownershipByDc;
    }
}
