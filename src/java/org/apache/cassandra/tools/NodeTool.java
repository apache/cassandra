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

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
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
import java.util.function.Consumer;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.nodetool.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.tools.nodetool.Sjk;

import com.google.common.collect.Maps;

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

public class NodeTool
{
    static
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9608
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final String HISTORYFILE = "nodetool.history";

    private final INodeProbeFactory nodeProbeFactory;

    public static void main(String... args)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15429
        System.exit(new NodeTool(new NodeProbeFactory()).execute(args));
    }

    public NodeTool(INodeProbeFactory nodeProbeFactory)
    {
        this.nodeProbeFactory = nodeProbeFactory;
    }

    public int execute(String... args)
    {
        List<Class<? extends Consumer<INodeProbeFactory>>> commands = newArrayList(
                CassHelp.class,
                Info.class,
                Ring.class,
                NetStats.class,
                CfStats.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8872
                TableStats.class,
                CfHistograms.class,
                TableHistograms.class,
                Cleanup.class,
                ClearSnapshot.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13665
                ClientStats.class,
                Compact.class,
                Scrub.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5791
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5791
                Verify.class,
                Flush.class,
                UpgradeSSTable.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7019
                GarbageCollect.class,
                DisableAutoCompaction.class,
                EnableAutoCompaction.class,
                CompactionStats.class,
                CompactionHistory.class,
                Decommission.class,
                DescribeCluster.class,
                DisableBinary.class,
                EnableBinary.class,
                EnableGossip.class,
                DisableGossip.class,
                EnableHandoff.class,
                EnableFullQueryLog.class,
                DisableFullQueryLog.class,
                GcStats.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13614
                GetBatchlogReplayTrottle.class,
                GetCompactionThreshold.class,
                GetCompactionThroughput.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5044
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15277
                GetConcurrency.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10953
                GetTimeout.class,
                GetStreamThroughput.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10234
                GetTraceProbability.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9708
                GetInterDCStreamThroughput.class,
                GetEndpoints.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14190
                GetSeeds.class,
                GetSSTables.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11720
                GetMaxHintWindow.class,
                GossipInfo.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6719
                Import.class,
                InvalidateKeyCache.class,
                InvalidateRowCache.class,
                InvalidateCounterCache.class,
                Join.class,
                Move.class,
                PauseHandoff.class,
                ResumeHandoff.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14436
                ProfileLoad.class,
                ProxyHistograms.class,
                Rebuild.class,
                Refresh.class,
                RemoveNode.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7935
                Assassinate.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14190
                ReloadSeeds.class,
                ResetFullQueryLog.class,
                Repair.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9143
                RepairAdmin.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9547
                ReplayBatchlog.class,
                SetCacheCapacity.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5044
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15277
                SetConcurrency.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7635
                SetHintedHandoffThrottleInKB.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13614
                SetBatchlogReplayThrottle.class,
                SetCompactionThreshold.class,
                SetCompactionThroughput.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12248
                GetConcurrentCompactors.class,
                SetConcurrentCompactors.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12245
                GetConcurrentViewBuilders.class,
                SetConcurrentViewBuilders.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5044
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15277
                SetConcurrency.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10953
                SetTimeout.class,
                SetStreamThroughput.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9708
                SetInterDCStreamThroughput.class,
                SetTraceProbability.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11720
                SetMaxHintWindow.class,
                Snapshot.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5742
                ListSnapshots.class,
                Status.class,
                StatusBinary.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8125
                StatusGossip.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8912
                StatusBackup.class,
                StatusHandoff.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8727
                StatusAutoCompaction.class,
                Stop.class,
                StopDaemon.class,
                Version.class,
                DescribeRing.class,
                RebuildIndex.class,
                RangeKeySample.class,
                EnableBackup.class,
                DisableBackup.class,
                ResetLocalSchema.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13954
                ReloadLocalSchema.class,
                ReloadTriggers.class,
                SetCacheKeysToSave.class,
                DisableHandoff.class,
                Drain.class,
                TruncateHints.class,
                TpStats.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7974
                TopPartitions.class,
                SetLoggingLevel.class,
                GetLoggingLevels.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12197
                Sjk.class,
                DisableHintsForDC.class,
                EnableHintsForDC.class,
                FailureDetectorInfo.class,
                RefreshSizeEstimates.class,
                RelocateSSTables.class,
                ViewBuildStatus.class,
                ReloadSslCertificates.class,
                EnableAuditLog.class,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14659
                DisableAuditLog.class,
                EnableOldProtocolVersions.class,
                DisableOldProtocolVersions.class
        );

        Cli.CliBuilder<Consumer<INodeProbeFactory>> builder = Cli.builder("nodetool");
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15429

        builder.withDescription("Manage your Cassandra cluster")
                 .withDefaultCommand(CassHelp.class)
                 .withCommands(commands);

        // bootstrap commands
        builder.withGroup("bootstrap")
                .withDescription("Monitor/manage node's bootstrap process")
                .withDefaultCommand(CassHelp.class)
                .withCommand(BootstrapResume.class);

        Cli<Consumer<INodeProbeFactory>> parser = builder.build();

        int status = 0;
        try
        {
            Consumer<INodeProbeFactory> parse = parser.parse(args);
            printHistory(args);
            parse.accept(nodeProbeFactory);
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

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15429
        return status;
    }

    private static void printHistory(String... args)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;

        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

        try (FileWriter writer = new FileWriter(new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE), true))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6449
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    protected void badUse(Exception e)
    {
        System.out.println("nodetool: " + e.getMessage());
        System.out.println("See 'nodetool help' or 'nodetool help <command>'.");
    }

    protected void err(Throwable e)
    {
        System.err.println("error: " + e.getMessage());
        System.err.println("-- StackTrace --");
        System.err.println(getStackTraceAsString(e));
    }

    public static class CassHelp extends Help implements Consumer<INodeProbeFactory>
    {
        public void accept(INodeProbeFactory nodeProbeFactory)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15429
            run();
        }
    }

    public static abstract class NodeToolCmd implements Consumer<INodeProbeFactory>
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

        public void accept(INodeProbeFactory nodeProbeFactory)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15429
            this.nodeProbeFactory = nodeProbeFactory;
            run();
        }

        public void run()
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6660
            if (isNotEmpty(username)) {
                if (isNotEmpty(passwordFilePath))
                    password = readUserPasswordFromFile(username, passwordFilePath);

                if (isEmpty(password))
                    password = promptAndReadPassword();
            }

            try (NodeProbe probe = connect())
            {
                execute(probe);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9569
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6660

            File passwordFile = new File(passwordFilePath);
            try (Scanner scanner = new Scanner(passwordFile).useDelimiter("\\s+"))
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
            } catch (FileNotFoundException e)
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15429
                    nodeClient = nodeProbeFactory.create(host, parseInt(port));
                else
                    nodeClient = nodeProbeFactory.create(host, parseInt(port), username, password);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10091
            } catch (IOException | SecurityException e)
            {
                Throwable rootCause = Throwables.getRootCause(e);
                System.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8776
                System.exit(1);
            }

            return nodeClient;
        }

        protected enum KeyspaceSet
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11627
            ALL, NON_SYSTEM, NON_LOCAL_STRATEGY
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13410
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
    }

    public static SortedMap<String, SetHostStat> getOwnershipByDc(NodeProbe probe, boolean resolveIp,
                                                                  Map<String, String> tokenToEndpoint,
                                                                  Map<InetAddress, Float> ownerships)
    {
        SortedMap<String, SetHostStat> ownershipByDc = Maps.newTreeMap();
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        try
        {
            for (Entry<String, String> tokenAndEndPoint : tokenToEndpoint.entrySet())
            {
                String dc = epSnitchInfo.getDatacenter(tokenAndEndPoint.getValue());
                if (!ownershipByDc.containsKey(dc))
                    ownershipByDc.put(dc, new SetHostStat(resolveIp));
                ownershipByDc.get(dc).add(tokenAndEndPoint.getKey(), tokenAndEndPoint.getValue(), ownerships);
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        return ownershipByDc;
    }

    public static SortedMap<String, SetHostStatWithPort> getOwnershipByDcWithPort(NodeProbe probe, boolean resolveIp,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7544
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
