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

import java.io.*;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import javax.management.InstanceNotFoundException;
import javax.management.openmbean.*;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.*;

import io.airlift.command.*;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.metrics.ColumnFamilyMetrics.Sampler;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.*;

public class NodeTool
{
    private static final String HISTORYFILE = "nodetool.history";

    public static void main(String... args)
    {
        List<Class<? extends Runnable>> commands = newArrayList(
                Help.class,
                Info.class,
                Ring.class,
                NetStats.class,
                CfStats.class,
                CfHistograms.class,
                Cleanup.class,
                ClearSnapshot.class,
                Compact.class,
                Scrub.class,
                Flush.class,
                UpgradeSSTable.class,
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
                EnableThrift.class,
                GcStats.class,
                GetCompactionThreshold.class,
                GetCompactionThroughput.class,
                GetStreamThroughput.class,
                GetEndpoints.class,
                GetSSTables.class,
                GossipInfo.class,
                InvalidateKeyCache.class,
                InvalidateRowCache.class,
                InvalidateCounterCache.class,
                Join.class,
                Move.class,
                PauseHandoff.class,
                ResumeHandoff.class,
                ProxyHistograms.class,
                Rebuild.class,
                Refresh.class,
                RemoveNode.class,
                Assassinate.class,
                Repair.class,
                SetCacheCapacity.class,
                SetHintedHandoffThrottleInKB.class,
                SetCompactionThreshold.class,
                SetCompactionThroughput.class,
                SetStreamThroughput.class,
                SetTraceProbability.class,
                Snapshot.class,
                ListSnapshots.class,
                Status.class,
                StatusBinary.class,
                StatusGossip.class,
                StatusThrift.class,
                StatusBackup.class,
                StatusHandoff.class,
                Stop.class,
                StopDaemon.class,
                Version.class,
                DescribeRing.class,
                RebuildIndex.class,
                RangeKeySample.class,
                EnableBackup.class,
                DisableBackup.class,
                ResetLocalSchema.class,
                ReloadTriggers.class,
                SetCacheKeysToSave.class,
                DisableThrift.class,
                DisableHandoff.class,
                Drain.class,
                TruncateHints.class,
                TpStats.class,
                TopPartitions.class,
                SetLoggingLevel.class,
                GetLoggingLevels.class
        );

        Cli<Runnable> parser = Cli.<Runnable>builder("nodetool")
                .withDescription("Manage your Cassandra cluster")
                .withDefaultCommand(Help.class)
                .withCommands(commands)
                .build();

        int status = 0;
        try
        {
            Runnable parse = parser.parse(args);
            printHistory(args);
            parse.run();
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

        System.exit(status);
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
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    private static void badUse(Exception e)
    {
        System.out.println("nodetool: " + e.getMessage());
        System.out.println("See 'nodetool help' or 'nodetool help <command>'.");
    }

    private static void err(Throwable e)
    {
        System.err.println("error: " + e.getMessage());
        System.err.println("-- StackTrace --");
        System.err.println(getStackTraceAsString(e));
    }

    public static abstract class NodeToolCmd implements Runnable
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

        @Override
        public void run()
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
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error while closing JMX connection", e);
            }

        }

        private String readUserPasswordFromFile(String username, String passwordFilePath) {
            String password = EMPTY;

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
                    nodeClient = new NodeProbe(host, parseInt(port));
                else
                    nodeClient = new NodeProbe(host, parseInt(port), username, password);
            } catch (IOException e)
            {
                Throwable rootCause = Throwables.getRootCause(e);
                System.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
                System.exit(1);
            }

            return nodeClient;
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe)
        {
            return parseOptionalKeyspace(cmdArgs, nodeProbe, false);
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe, boolean includeSystemKS)
        {
            List<String> keyspaces = new ArrayList<>();

            if (cmdArgs == null || cmdArgs.isEmpty())
                keyspaces.addAll(includeSystemKS ? nodeProbe.getKeyspaces() : nodeProbe.getNonSystemKeyspaces());
            else
                keyspaces.add(cmdArgs.get(0));

            for (String keyspace : keyspaces)
            {
                if (!nodeProbe.getKeyspaces().contains(keyspace))
                    throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
            }

            return Collections.unmodifiableList(keyspaces);
        }

        protected String[] parseOptionalColumnFamilies(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
        }
    }

    @Command(name = "info", description = "Print node information (uptime, load, ...)")
    public static class Info extends NodeToolCmd
    {
        @Option(name = {"-T", "--tokens"}, description = "Display all tokens")
        private boolean tokens = false;

        @Override
        public void execute(NodeProbe probe)
        {
            boolean gossipInitialized = probe.isInitialized();

            System.out.printf("%-23s: %s%n", "ID", probe.getLocalHostId());
            System.out.printf("%-23s: %s%n", "Gossip active", gossipInitialized);
            System.out.printf("%-23s: %s%n", "Thrift active", probe.isThriftServerRunning());
            System.out.printf("%-23s: %s%n", "Native Transport active", probe.isNativeTransportRunning());
            System.out.printf("%-23s: %s%n", "Load", probe.getLoadString());
            if (gossipInitialized)
                System.out.printf("%-23s: %s%n", "Generation No", probe.getCurrentGenerationNumber());
            else
                System.out.printf("%-23s: %s%n", "Generation No", 0);

            // Uptime
            long secondsUp = probe.getUptime() / 1000;
            System.out.printf("%-23s: %d%n", "Uptime (seconds)", secondsUp);

            // Memory usage
            MemoryUsage heapUsage = probe.getHeapMemoryUsage();
            double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
            double memMax = (double) heapUsage.getMax() / (1024 * 1024);
            System.out.printf("%-23s: %.2f / %.2f%n", "Heap Memory (MB)", memUsed, memMax);
            try
            {
                System.out.printf("%-23s: %.2f%n", "Off Heap Memory (MB)", getOffHeapMemoryUsed(probe));
            }
            catch (RuntimeException e)
            {
                // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
                if (!(e.getCause() instanceof InstanceNotFoundException))
                    throw e;
            }

            // Data Center/Rack
            System.out.printf("%-23s: %s%n", "Data Center", probe.getDataCenter());
            System.out.printf("%-23s: %s%n", "Rack", probe.getRack());

            // Exceptions
            System.out.printf("%-23s: %s%n", "Exceptions", probe.getStorageMetric("Exceptions"));

            CacheServiceMBean cacheService = probe.getCacheServiceMBean();

            // Key Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
            System.out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                    "Key Cache",
                    probe.getCacheMetric("KeyCache", "Entries"),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("KeyCache", "Size")),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("KeyCache", "Capacity")),
                    probe.getCacheMetric("KeyCache", "Hits"),
                    probe.getCacheMetric("KeyCache", "Requests"),
                    probe.getCacheMetric("KeyCache", "HitRate"),
                    cacheService.getKeyCacheSavePeriodInSeconds());

            // Row Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
            System.out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                    "Row Cache",
                    probe.getCacheMetric("RowCache", "Entries"),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("RowCache", "Size")),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("RowCache", "Capacity")),
                    probe.getCacheMetric("RowCache", "Hits"),
                    probe.getCacheMetric("RowCache", "Requests"),
                    probe.getCacheMetric("RowCache", "HitRate"),
                    cacheService.getRowCacheSavePeriodInSeconds());

            // Counter Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
            System.out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                    "Counter Cache",
                    probe.getCacheMetric("CounterCache", "Entries"),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("CounterCache", "Size")),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("CounterCache", "Capacity")),
                    probe.getCacheMetric("CounterCache", "Hits"),
                    probe.getCacheMetric("CounterCache", "Requests"),
                    probe.getCacheMetric("CounterCache", "HitRate"),
                    cacheService.getCounterCacheSavePeriodInSeconds());

            // Tokens
            List<String> tokens = probe.getTokens();
            if (tokens.size() == 1 || this.tokens)
                for (String token : tokens)
                    System.out.printf("%-23s: %s%n", "Token", token);
            else
                System.out.printf("%-23s: (invoke with -T/--tokens to see all %d tokens)%n", "Token", tokens.size());
        }

        /**
         * Returns the total off heap memory used in MB.
         * @return the total off heap memory used in MB.
         */
        private static double getOffHeapMemoryUsed(NodeProbe probe)
        {
            long offHeapMemUsedInBytes = 0;
            // get a list of column family stores
            Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

            while (cfamilies.hasNext())
            {
                Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
                String keyspaceName = entry.getKey();
                String cfName = entry.getValue().getColumnFamilyName();

                offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableOffHeapSize");
                offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterOffHeapMemoryUsed");
                offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "IndexSummaryOffHeapMemoryUsed");
                offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionMetadataOffHeapMemoryUsed");
            }

            return offHeapMemUsedInBytes / (1024d * 1024);
        }
    }

    @Command(name = "ring", description = "Print information about the token ring")
    public static class Ring extends NodeToolCmd
    {
        @Arguments(description = "Specify a keyspace for accurate ownership information (topology awareness)")
        private String keyspace = null;

        @Option(title = "resolve_ip", name = {"-r", "--resolve-ip"}, description = "Show node domain names instead of IPs")
        private boolean resolveIp = false;

        @Override
        public void execute(NodeProbe probe)
        {
            Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
            LinkedHashMultimap<String, String> endpointsToTokens = LinkedHashMultimap.create();
            boolean haveVnodes = false;
            for (Map.Entry<String, String> entry : tokensToEndpoints.entrySet())
            {
                haveVnodes |= endpointsToTokens.containsKey(entry.getValue());
                endpointsToTokens.put(entry.getValue(), entry.getKey());
            }

            int maxAddressLength = Collections.max(endpointsToTokens.keys(), new Comparator<String>()
            {
                @Override
                public int compare(String first, String second)
                {
                    return ((Integer) first.length()).compareTo(second.length());
                }
            }).length();

            String formatPlaceholder = "%%-%ds  %%-12s%%-7s%%-8s%%-16s%%-20s%%-44s%%n";
            String format = format(formatPlaceholder, maxAddressLength);

            StringBuffer errors = new StringBuffer();
            boolean showEffectiveOwnership = true;
            // Calculate per-token ownership of the ring
            Map<InetAddress, Float> ownerships;
            try
            {
                ownerships = probe.effectiveOwnership(keyspace);
            }
            catch (IllegalStateException ex)
            {
                ownerships = probe.getOwnership();
                errors.append("Note: " + ex.getMessage() + "%n");
                showEffectiveOwnership = false;
            }
            catch (IllegalArgumentException ex)
            {
                System.out.printf("%nError: " + ex.getMessage() + "%n");
                return;
            }


            System.out.println();
            for (Entry<String, SetHostStat> entry : getOwnershipByDc(probe, resolveIp, tokensToEndpoints, ownerships).entrySet())
                printDc(probe, format, entry.getKey(), endpointsToTokens, entry.getValue(),showEffectiveOwnership);

            if (haveVnodes)
            {
                System.out.println("  Warning: \"nodetool ring\" is used to output all the tokens of a node.");
                System.out.println("  To view status related info of a node use \"nodetool status\" instead.\n");
            }

            System.out.printf("%n  " + errors.toString());
        }

        private void printDc(NodeProbe probe, String format,
                             String dc,
                             LinkedHashMultimap<String, String> endpointsToTokens,
                             SetHostStat hoststats,boolean showEffectiveOwnership)
        {
            Collection<String> liveNodes = probe.getLiveNodes();
            Collection<String> deadNodes = probe.getUnreachableNodes();
            Collection<String> joiningNodes = probe.getJoiningNodes();
            Collection<String> leavingNodes = probe.getLeavingNodes();
            Collection<String> movingNodes = probe.getMovingNodes();
            Map<String, String> loadMap = probe.getLoadMap();

            System.out.println("Datacenter: " + dc);
            System.out.println("==========");

            // get the total amount of replicas for this dc and the last token in this dc's ring
            List<String> tokens = new ArrayList<>();
            String lastToken = "";

            for (HostStat stat : hoststats)
            {
                tokens.addAll(endpointsToTokens.get(stat.endpoint.getHostAddress()));
                lastToken = tokens.get(tokens.size() - 1);
            }

            System.out.printf(format, "Address", "Rack", "Status", "State", "Load", "Owns", "Token");

            if (hoststats.size() > 1)
                System.out.printf(format, "", "", "", "", "", "", lastToken);
            else
                System.out.println();

            for (HostStat stat : hoststats)
            {
                String endpoint = stat.endpoint.getHostAddress();
                String rack;
                try
                {
                    rack = probe.getEndpointSnitchInfoProxy().getRack(endpoint);
                }
                catch (UnknownHostException e)
                {
                    rack = "Unknown";
                }

                String status = liveNodes.contains(endpoint)
                        ? "Up"
                        : deadNodes.contains(endpoint)
                                ? "Down"
                                : "?";

                String state = "Normal";

                if (joiningNodes.contains(endpoint))
                    state = "Joining";
                else if (leavingNodes.contains(endpoint))
                    state = "Leaving";
                else if (movingNodes.contains(endpoint))
                    state = "Moving";

                String load = loadMap.containsKey(endpoint)
                        ? loadMap.get(endpoint)
                        : "?";
                String owns = stat.owns != null && showEffectiveOwnership? new DecimalFormat("##0.00%").format(stat.owns) : "?";
                System.out.printf(format, stat.ipOrDns(), rack, status, state, load, owns, stat.token);
            }
            System.out.println();
        }
    }

    @Command(name = "netstats", description = "Print network information on provided host (connecting node by default)")
    public static class NetStats extends NodeToolCmd
    {
        @Option(title = "human_readable",
                name = {"-H", "--human-readable"},
                description = "Display bytes in human readable form, i.e. KB, MB, GB, TB")
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
            }
        }
    }

    @Command(name = "cfstats", description = "Print statistics on tables")
    public static class CfStats extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace.table>...]", description = "List of tables (or keyspace) names")
        private List<String> cfnames = new ArrayList<>();

        @Option(name = "-i", description = "Ignore the list of tables and display the remaining cfs")
        private boolean ignore = false;

        @Option(title = "human_readable",
                name = {"-H", "--human-readable"},
                description = "Display bytes in human readable form, i.e. KB, MB, GB, TB")
        private boolean humanReadable = false;

        @Override
        public void execute(NodeProbe probe)
        {
            OptionFilter filter = new OptionFilter(ignore, cfnames);
            Map<String, List<ColumnFamilyStoreMBean>> cfstoreMap = new HashMap<>();

            // get a list of column family stores
            Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

            while (cfamilies.hasNext())
            {
                Map.Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
                String keyspaceName = entry.getKey();
                ColumnFamilyStoreMBean cfsProxy = entry.getValue();

                if (!cfstoreMap.containsKey(keyspaceName) && filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
                {
                    List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<>();
                    columnFamilies.add(cfsProxy);
                    cfstoreMap.put(keyspaceName, columnFamilies);
                } else if (filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
                {
                    cfstoreMap.get(keyspaceName).add(cfsProxy);
                }
            }

            // make sure all specified kss and cfs exist
            filter.verifyKeyspaces(probe.getKeyspaces());
            filter.verifyColumnFamilies();

            // print out the table statistics
            for (Map.Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
            {
                String keyspaceName = entry.getKey();
                List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
                long keyspaceReadCount = 0;
                long keyspaceWriteCount = 0;
                int keyspacePendingFlushes = 0;
                double keyspaceTotalReadTime = 0.0f;
                double keyspaceTotalWriteTime = 0.0f;

                System.out.println("Keyspace: " + keyspaceName);
                for (ColumnFamilyStoreMBean cfstore : columnFamilies)
                {
                    String cfName = cfstore.getColumnFamilyName();
                    long writeCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getCount();
                    long readCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getCount();

                    if (readCount > 0)
                    {
                        keyspaceReadCount += readCount;
                        keyspaceTotalReadTime += (long) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadTotalLatency");
                    }
                    if (writeCount > 0)
                    {
                        keyspaceWriteCount += writeCount;
                        keyspaceTotalWriteTime += (long) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteTotalLatency");
                    }
                    keyspacePendingFlushes += (long) probe.getColumnFamilyMetric(keyspaceName, cfName, "PendingFlushes");
                }

                double keyspaceReadLatency = keyspaceReadCount > 0
                                             ? keyspaceTotalReadTime / keyspaceReadCount / 1000
                                             : Double.NaN;
                double keyspaceWriteLatency = keyspaceWriteCount > 0
                                              ? keyspaceTotalWriteTime / keyspaceWriteCount / 1000
                                              : Double.NaN;

                System.out.println("\tRead Count: " + keyspaceReadCount);
                System.out.println("\tRead Latency: " + String.format("%s", keyspaceReadLatency) + " ms.");
                System.out.println("\tWrite Count: " + keyspaceWriteCount);
                System.out.println("\tWrite Latency: " + String.format("%s", keyspaceWriteLatency) + " ms.");
                System.out.println("\tPending Flushes: " + keyspacePendingFlushes);

                // print out column family statistics for this keyspace
                for (ColumnFamilyStoreMBean cfstore : columnFamilies)
                {
                    String cfName = cfstore.getColumnFamilyName();
                    if (cfName.contains("."))
                        System.out.println("\t\tTable (index): " + cfName);
                    else
                        System.out.println("\t\tTable: " + cfName);

                    System.out.println("\t\tSSTable count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveSSTableCount"));

                    int[] leveledSStables = cfstore.getSSTableCountPerLevel();
                    if (leveledSStables != null)
                    {
                        System.out.print("\t\tSSTables in each level: [");
                        for (int level = 0; level < leveledSStables.length; level++)
                        {
                            int count = leveledSStables[level];
                            System.out.print(count);
                            long maxCount = 4L; // for L0
                            if (level > 0)
                                maxCount = (long) Math.pow(10, level);
                            //  show max threshold for level when exceeded
                            if (count > maxCount)
                                System.out.print("/" + maxCount);

                            if (level < leveledSStables.length - 1)
                                System.out.print(", ");
                            else
                                System.out.println("]");
                        }
                    }

                    Long memtableOffHeapSize = null;
                    Long bloomFilterOffHeapSize = null;
                    Long indexSummaryOffHeapSize = null;
                    Long compressionMetadataOffHeapSize = null;

                    Long offHeapSize = null;

                    try
                    {
                        memtableOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableOffHeapSize");
                        bloomFilterOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterOffHeapMemoryUsed");
                        indexSummaryOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "IndexSummaryOffHeapMemoryUsed");
                        compressionMetadataOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionMetadataOffHeapMemoryUsed");

                        offHeapSize = memtableOffHeapSize + bloomFilterOffHeapSize + indexSummaryOffHeapSize + compressionMetadataOffHeapSize;
                    }
                    catch (RuntimeException e)
                    {
                        // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
                        if (!(e.getCause() instanceof InstanceNotFoundException))
                            throw e;
                    }

                    System.out.println("\t\tSpace used (live): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveDiskSpaceUsed"), humanReadable));
                    System.out.println("\t\tSpace used (total): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "TotalDiskSpaceUsed"), humanReadable));
                    System.out.println("\t\tSpace used by snapshots (total): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "SnapshotsSize"), humanReadable));
                    if (offHeapSize != null)
                        System.out.println("\t\tOff heap memory used (total): " + format(offHeapSize, humanReadable));
                    System.out.println("\t\tSSTable Compression Ratio: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionRatio"));
                    long numberOfKeys = 0;
                    for (long keys : (long[]) probe.getColumnFamilyMetric(keyspaceName, cfName, "EstimatedColumnCountHistogram"))
                        numberOfKeys += keys;
                    System.out.println("\t\tNumber of keys (estimate): " + numberOfKeys);
                    System.out.println("\t\tMemtable cell count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableColumnsCount"));
                    System.out.println("\t\tMemtable data size: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableLiveDataSize"), humanReadable));
                    if (memtableOffHeapSize != null)
                        System.out.println("\t\tMemtable off heap memory used: " + format(memtableOffHeapSize, humanReadable));
                    System.out.println("\t\tMemtable switch count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableSwitchCount"));
                    System.out.println("\t\tLocal read count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getCount());
                    double localReadLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getMean() / 1000;
                    double localRLatency = localReadLatency > 0 ? localReadLatency : Double.NaN;
                    System.out.printf("\t\tLocal read latency: %01.3f ms%n", localRLatency);
                    System.out.println("\t\tLocal write count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getCount());
                    double localWriteLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getMean() / 1000;
                    double localWLatency = localWriteLatency > 0 ? localWriteLatency : Double.NaN;
                    System.out.printf("\t\tLocal write latency: %01.3f ms%n", localWLatency);
                    System.out.println("\t\tPending flushes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "PendingFlushes"));
                    System.out.println("\t\tBloom filter false positives: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterFalsePositives"));
                    System.out.printf("\t\tBloom filter false ratio: %s%n", String.format("%01.5f", probe.getColumnFamilyMetric(keyspaceName, cfName, "RecentBloomFilterFalseRatio")));
                    System.out.println("\t\tBloom filter space used: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterDiskSpaceUsed"), humanReadable));
                    if (bloomFilterOffHeapSize != null)
                        System.out.println("\t\tBloom filter off heap memory used: " + format(bloomFilterOffHeapSize, humanReadable));
                    if (indexSummaryOffHeapSize != null)
                        System.out.println("\t\tIndex summary off heap memory used: " + format(indexSummaryOffHeapSize, humanReadable));
                    if (compressionMetadataOffHeapSize != null)
                        System.out.println("\t\tCompression metadata off heap memory used: " + format(compressionMetadataOffHeapSize, humanReadable));

                    System.out.println("\t\tCompacted partition minimum bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MinRowSize"), humanReadable));
                    System.out.println("\t\tCompacted partition maximum bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MaxRowSize"), humanReadable));
                    System.out.println("\t\tCompacted partition mean bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MeanRowSize"), humanReadable));
                    CassandraMetricsRegistry.JmxHistogramMBean histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveScannedHistogram");
                    System.out.println("\t\tAverage live cells per slice (last five minutes): " + histogram.getMean());
                    System.out.println("\t\tMaximum live cells per slice (last five minutes): " + histogram.getMax());
                    histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "TombstoneScannedHistogram");
                    System.out.println("\t\tAverage tombstones per slice (last five minutes): " + histogram.getMean());
                    System.out.println("\t\tMaximum tombstones per slice (last five minutes): " + histogram.getMax());

                    System.out.println("");
                }
                System.out.println("----------------");
            }
        }

        private String format(long bytes, boolean humanReadable) {
            return humanReadable ? FileUtils.stringifyFileSize(bytes) : Long.toString(bytes);
        }

        /**
         * Used for filtering keyspaces and columnfamilies to be displayed using the cfstats command.
         */
        private static class OptionFilter
        {
            private Map<String, List<String>> filter = new HashMap<>();
            private Map<String, List<String>> verifier = new HashMap<>();
            private List<String> filterList = new ArrayList<>();
            private boolean ignoreMode;

            public OptionFilter(boolean ignoreMode, List<String> filterList)
            {
                this.filterList.addAll(filterList);
                this.ignoreMode = ignoreMode;

                for (String s : filterList)
                {
                    String[] keyValues = s.split("\\.", 2);

                    // build the map that stores the ks' and cfs to use
                    if (!filter.containsKey(keyValues[0]))
                    {
                        filter.put(keyValues[0], new ArrayList<String>());
                        verifier.put(keyValues[0], new ArrayList<String>());

                        if (keyValues.length == 2)
                        {
                            filter.get(keyValues[0]).add(keyValues[1]);
                            verifier.get(keyValues[0]).add(keyValues[1]);
                        }
                    } else
                    {
                        if (keyValues.length == 2)
                        {
                            filter.get(keyValues[0]).add(keyValues[1]);
                            verifier.get(keyValues[0]).add(keyValues[1]);
                        }
                    }
                }
            }

            public boolean isColumnFamilyIncluded(String keyspace, String columnFamily)
            {
                // supplying empty params list is treated as wanting to display all kss & cfs
                if (filterList.isEmpty())
                    return !ignoreMode;

                List<String> cfs = filter.get(keyspace);

                // no such keyspace is in the map
                if (cfs == null)
                    return ignoreMode;
                    // only a keyspace with no cfs was supplied
                    // so ignore or include (based on the flag) every column family in specified keyspace
                else if (cfs.size() == 0)
                    return !ignoreMode;

                // keyspace exists, and it contains specific cfs
                verifier.get(keyspace).remove(columnFamily);
                return ignoreMode ^ cfs.contains(columnFamily);
            }

            public void verifyKeyspaces(List<String> keyspaces)
            {
                for (String ks : verifier.keySet())
                    if (!keyspaces.contains(ks))
                        throw new IllegalArgumentException("Unknown keyspace: " + ks);
            }

            public void verifyColumnFamilies()
            {
                for (String ks : filter.keySet())
                    if (verifier.get(ks).size() > 0)
                        throw new IllegalArgumentException("Unknown tables: " + verifier.get(ks) + " in keyspace: " + ks);
            }
        }
    }

    @Command(name = "toppartitions", description = "Sample and print the most active partitions for a given column family")
    public static class TopPartitions extends NodeToolCmd
    {
        @Arguments(usage = "<keyspace> <cfname> <duration>", description = "The keyspace, column family name, and duration in milliseconds")
        private List<String> args = new ArrayList<>();
        @Option(name = "-s", description = "Capacity of stream summary, closer to the actual cardinality of partitions will yield more accurate results (Default: 256)")
        private int size = 256;
        @Option(name = "-k", description = "Number of the top partitions to list (Default: 10)")
        private int topCount = 10;
        @Option(name = "-a", description = "Comma separated list of samplers to use (Default: all)")
        private String samplers = join(ColumnFamilyMetrics.Sampler.values(), ',');
        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 3, "toppartitions requires keyspace, column family name, and duration");
            checkArgument(topCount < size, "TopK count (-k) option must be smaller then the summary capacity (-s)");
            String keyspace = args.get(0);
            String cfname = args.get(1);
            Integer duration = Integer.parseInt(args.get(2));
            // generate the list of samplers
            List<Sampler> targets = Lists.newArrayList();
            for (String s : samplers.split(","))
            {
                try
                {
                    targets.add(Sampler.valueOf(s.toUpperCase()));
                } catch (Exception e)
                {
                    throw new IllegalArgumentException(s + " is not a valid sampler, choose one of: " + join(Sampler.values(), ", "));
                }
            }

            Map<Sampler, CompositeData> results;
            try
            {
                results = probe.getPartitionSample(keyspace, cfname, size, duration, topCount, targets);
            } catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
            boolean first = true;
            for(Entry<Sampler, CompositeData> result : results.entrySet())
            {
                CompositeData sampling = result.getValue();
                // weird casting for http://bugs.sun.com/view_bug.do?bug_id=6548436
                List<CompositeData> topk = (List<CompositeData>) (Object) Lists.newArrayList(((TabularDataSupport) sampling.get("partitions")).values());
                Collections.sort(topk, new Ordering<CompositeData>()
                {
                    public int compare(CompositeData left, CompositeData right)
                    {
                        return Long.compare((long) right.get("count"), (long) left.get("count"));
                    }
                });
                if(!first)
                    System.out.println();
                System.out.println(result.getKey().toString()+ " Sampler:");
                System.out.printf("  Cardinality: ~%d (%d capacity)%n", (long) sampling.get("cardinality"), size);
                System.out.printf("  Top %d partitions:%n", topCount);
                if (topk.size() == 0)
                {
                    System.out.println("\tNothing recorded during sampling period...");
                } else
                {
                    int offset = 0;
                    for (CompositeData entry : topk)
                        offset = Math.max(offset, entry.get("string").toString().length());
                    System.out.printf("\t%-" + offset + "s%10s%10s%n", "Partition", "Count", "+/-");
                    for (CompositeData entry : topk)
                        System.out.printf("\t%-" + offset + "s%10d%10d%n", entry.get("string").toString(), entry.get("count"), entry.get("error"));
                }
                first = false;
            }
        }
    }

    @Command(name = "cfhistograms", description = "Print statistic histograms for a given column family")
    public static class CfHistograms extends NodeToolCmd
    {
        @Arguments(usage = "<keyspace> <table>", description = "The keyspace and table name")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 2, "cfhistograms requires ks and cf args");

            String keyspace = args.get(0);
            String cfname = args.get(1);

            // calculate percentile of row size and column count
            long[] estimatedRowSize = (long[]) probe.getColumnFamilyMetric(keyspace, cfname, "EstimatedRowSizeHistogram");
            long[] estimatedColumnCount = (long[]) probe.getColumnFamilyMetric(keyspace, cfname, "EstimatedColumnCountHistogram");

            // build arrays to store percentile values
            double[] estimatedRowSizePercentiles = new double[7];
            double[] estimatedColumnCountPercentiles = new double[7];
            double[] offsetPercentiles = new double[]{0.5, 0.75, 0.95, 0.98, 0.99};

            if (ArrayUtils.isEmpty(estimatedRowSize) || ArrayUtils.isEmpty(estimatedColumnCount))
            {
                System.err.println("No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles");

                for (int i = 0; i < 7; i++)
                {
                    estimatedRowSizePercentiles[i] = Double.NaN;
                    estimatedColumnCountPercentiles[i] = Double.NaN;
                }
            }
            else
            {
                long[] rowSizeBucketOffsets = new EstimatedHistogram(estimatedRowSize.length).getBucketOffsets();
                long[] columnCountBucketOffsets = new EstimatedHistogram(estimatedColumnCount.length).getBucketOffsets();
                EstimatedHistogram rowSizeHist = new EstimatedHistogram(rowSizeBucketOffsets, estimatedRowSize);
                EstimatedHistogram columnCountHist = new EstimatedHistogram(columnCountBucketOffsets, estimatedColumnCount);

                if (rowSizeHist.isOverflowed())
                {
                    System.err.println(String.format("Row sizes are larger than %s, unable to calculate percentiles", rowSizeBucketOffsets[rowSizeBucketOffsets.length - 1]));
                    for (int i = 0; i < offsetPercentiles.length; i++)
                            estimatedRowSizePercentiles[i] = Double.NaN;
                }
                else
                {
                    for (int i = 0; i < offsetPercentiles.length; i++)
                        estimatedRowSizePercentiles[i] = rowSizeHist.percentile(offsetPercentiles[i]);
                }

                if (columnCountHist.isOverflowed())
                {
                    System.err.println(String.format("Column counts are larger than %s, unable to calculate percentiles", columnCountBucketOffsets[columnCountBucketOffsets.length - 1]));
                    for (int i = 0; i < estimatedColumnCountPercentiles.length; i++)
                        estimatedColumnCountPercentiles[i] = Double.NaN;
                }
                else
                {
                    for (int i = 0; i < offsetPercentiles.length; i++)
                        estimatedColumnCountPercentiles[i] = columnCountHist.percentile(offsetPercentiles[i]);
                }

                // min value
                estimatedRowSizePercentiles[5] = rowSizeHist.min();
                estimatedColumnCountPercentiles[5] = columnCountHist.min();
                // max value
                estimatedRowSizePercentiles[6] = rowSizeHist.max();
                estimatedColumnCountPercentiles[6] = columnCountHist.max();
            }

            String[] percentiles = new String[]{"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
            double[] readLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, cfname, "ReadLatency"));
            double[] writeLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, cfname, "WriteLatency"));
            double[] sstablesPerRead = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspace, cfname, "SSTablesPerReadHistogram"));

            System.out.println(format("%s/%s histograms", keyspace, cfname));
            System.out.println(format("%-10s%10s%18s%18s%18s%18s",
                    "Percentile", "SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count"));
            System.out.println(format("%-10s%10s%18s%18s%18s%18s",
                    "", "", "(micros)", "(micros)", "(bytes)", ""));

            for (int i = 0; i < percentiles.length; i++)
            {
                System.out.println(format("%-10s%10.2f%18.2f%18.2f%18.0f%18.0f",
                        percentiles[i],
                        sstablesPerRead[i],
                        writeLatency[i],
                        readLatency[i],
                        estimatedRowSizePercentiles[i],
                        estimatedColumnCountPercentiles[i]));
            }
            System.out.println();
        }
    }

    @Command(name = "cleanup", description = "Triggers the immediate cleanup of keys no longer belonging to a node. By default, clean all keyspaces")
    public static class Cleanup extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            for (String keyspace : keyspaces)
            {
                if (SystemKeyspace.NAME.equals(keyspace))
                    continue;

                try
                {
                    probe.forceKeyspaceCleanup(System.out, keyspace, cfnames);
                } catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during cleanup", e);
                }
            }
        }
    }

    @Command(name = "clearsnapshot", description = "Remove the snapshot with the given name from the given keyspaces. If no snapshotName is specified we will remove all snapshots")
    public static class ClearSnapshot extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspaces>...] ", description = "Remove snapshots from the given keyspaces")
        private List<String> keyspaces = new ArrayList<>();

        @Option(title = "snapshot_name", name = "-t", description = "Remove the snapshot with a given name")
        private String snapshotName = EMPTY;

        @Override
        public void execute(NodeProbe probe)
        {
            StringBuilder sb = new StringBuilder();

            sb.append("Requested clearing snapshot(s) for ");

            if (keyspaces.isEmpty())
                sb.append("[all keyspaces]");
            else
                sb.append("[").append(join(keyspaces, ", ")).append("]");

            if (!snapshotName.isEmpty())
                sb.append(" with snapshot name [").append(snapshotName).append("]");

            System.out.println(sb.toString());

            try
            {
                probe.clearSnapshot(snapshotName, toArray(keyspaces, String.class));
            } catch (IOException e)
            {
                throw new RuntimeException("Error during clearing snapshots", e);
            }
        }
    }

    @Command(name = "compact", description = "Force a (major) compaction on one or more tables")
    public static class Compact extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            for (String keyspace : keyspaces)
            {
                try
                {
                    probe.forceKeyspaceCompaction(keyspace, cfnames);
                } catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during compaction", e);
                }
            }
        }
    }

    @Command(name = "flush", description = "Flush one or more tables")
    public static class Flush extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            for (String keyspace : keyspaces)
            {
                try
                {
                    probe.forceKeyspaceFlush(keyspace, cfnames);
                } catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during flushing", e);
                }
            }
        }
    }

    @Command(name = "scrub", description = "Scrub (rebuild sstables for) one or more tables")
    public static class Scrub extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Option(title = "disable_snapshot",
                name = {"-ns", "--no-snapshot"},
                description = "Scrubbed CFs will be snapshotted first, if disableSnapshot is false. (default false)")
        private boolean disableSnapshot = false;

        @Option(title = "skip_corrupted",
                name = {"-s", "--skip-corrupted"},
                description = "Skip corrupted partitions even when scrubbing counter tables. (default false)")
        private boolean skipCorrupted = false;

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            for (String keyspace : keyspaces)
            {
                try
                {
                    probe.scrub(System.out, disableSnapshot, skipCorrupted, keyspace, cfnames);
                } catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during flushing", e);
                }
            }
        }
    }

    @Command(name = "disableautocompaction", description = "Disable autocompaction for the given keyspace and table")
    public static class DisableAutoCompaction extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            for (String keyspace : keyspaces)
            {
                try
                {
                    probe.disableAutoCompaction(keyspace, cfnames);
                } catch (IOException e)
                {
                    throw new RuntimeException("Error occurred during disabling auto-compaction", e);
                }
            }
        }
    }

    @Command(name = "enableautocompaction", description = "Enable autocompaction for the given keyspace and table")
    public static class EnableAutoCompaction extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            for (String keyspace : keyspaces)
            {
                try
                {
                    probe.enableAutoCompaction(keyspace, cfnames);
                } catch (IOException e)
                {
                    throw new RuntimeException("Error occurred during enabling auto-compaction", e);
                }
            }
        }
    }

    @Command(name = "upgradesstables", description = "Rewrite sstables (for the requested tables) that are not on the current version (thus upgrading them to said current version)")
    public static class UpgradeSSTable extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Option(title = "include_all", name = {"-a", "--include-all-sstables"}, description = "Use -a to include all sstables, even those already on the current version")
        private boolean includeAll = false;

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            for (String keyspace : keyspaces)
            {
                try
                {
                    probe.upgradeSSTables(System.out, keyspace, !includeAll, cfnames);
                } catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during enabling auto-compaction", e);
                }
            }
        }
    }

    @Command(name = "compactionstats", description = "Print statistics on compactions")
    public static class CompactionStats extends NodeToolCmd
    {
        @Option(title = "human_readable",
                name = {"-H", "--human-readable"},
                description = "Display bytes in human readable form, i.e. KB, MB, GB, TB")
        private boolean humanReadable = false;

        @Override
        public void execute(NodeProbe probe)
        {
            CompactionManagerMBean cm = probe.getCompactionManagerProxy();
            System.out.println("pending tasks: " + probe.getCompactionMetric("PendingTasks"));
            long remainingBytes = 0;
            List<Map<String, String>> compactions = cm.getCompactions();
            if (!compactions.isEmpty())
            {
                int compactionThroughput = probe.getCompactionThroughput();
                List<String[]> lines = new ArrayList<>();
                int[] columnSizes = new int[] { 0, 0, 0, 0, 0, 0, 0 };

                addLine(lines, columnSizes, "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");
                for (Map<String, String> c : compactions)
                {
                    long total = Long.parseLong(c.get("total"));
                    long completed = Long.parseLong(c.get("completed"));
                    String taskType = c.get("taskType");
                    String keyspace = c.get("keyspace");
                    String columnFamily = c.get("columnfamily");
                    String completedStr = humanReadable ? FileUtils.stringifyFileSize(completed) : Long.toString(completed);
                    String totalStr = humanReadable ? FileUtils.stringifyFileSize(total) : Long.toString(total);
                    String unit = c.get("unit");
                    String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + "%";
                    addLine(lines, columnSizes, taskType, keyspace, columnFamily, completedStr, totalStr, unit, percentComplete);
                    if (taskType.equals(OperationType.COMPACTION.toString()))
                        remainingBytes += total - completed;
                }

                StringBuilder buffer = new StringBuilder();
                for (int columnSize : columnSizes) {
                    buffer.append("%");
                    buffer.append(columnSize + 3);
                    buffer.append("s");
                }
                buffer.append("%n");
                String format = buffer.toString();

                for (String[] line : lines)
                {
                    System.out.printf(format, line[0], line[1], line[2], line[3], line[4], line[5], line[6]);
                }

                String remainingTime = "n/a";
                if (compactionThroughput != 0)
                {
                    long remainingTimeInSecs = remainingBytes / (1024L * 1024L * compactionThroughput);
                    remainingTime = format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));
                }
                System.out.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
            }
        }

        private void addLine(List<String[]> lines, int[] columnSizes, String... columns) {
            lines.add(columns);
            for (int i = 0; i < columns.length; i++) {
                columnSizes[i] = Math.max(columnSizes[i], columns[i].length());
            }
        }
    }

    @Command(name = "compactionhistory", description = "Print history of compaction")
    public static class CompactionHistory extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println("Compaction History: ");

            TabularData tabularData = probe.getCompactionHistory();
            if (tabularData.isEmpty())
            {
                System.out.printf("There is no compaction history");
                return;
            }

            String format = "%-41s%-19s%-29s%-26s%-15s%-15s%s%n";
            List<String> indexNames = tabularData.getTabularType().getIndexNames();
            System.out.printf(format, toArray(indexNames, Object.class));

            Set<?> values = tabularData.keySet();
            for (Object eachValue : values)
            {
                List<?> value = (List<?>) eachValue;
                System.out.printf(format, toArray(value, Object.class));
            }
        }
    }

    @Command(name = "decommission", description = "Decommission the *node I am connecting to*")
    public static class Decommission extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                probe.decommission();
            } catch (InterruptedException e)
            {
                throw new RuntimeException("Error decommissioning node", e);
            }
        }
    }

    @Command(name = "describecluster", description = "Print the name, snitch, partitioner and schema version of a cluster")
    public static class DescribeCluster extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            // display cluster name, snitch and partitioner
            System.out.println("Cluster Information:");
            System.out.println("\tName: " + probe.getClusterName());
            System.out.println("\tSnitch: " + probe.getEndpointSnitchInfoProxy().getSnitchName());
            System.out.println("\tPartitioner: " + probe.getPartitioner());

            // display schema version for each node
            System.out.println("\tSchema versions:");
            Map<String, List<String>> schemaVersions = probe.getSpProxy().getSchemaVersions();
            for (String version : schemaVersions.keySet())
            {
                System.out.println(format("\t\t%s: %s%n", version, schemaVersions.get(version)));
            }
        }
    }

    @Command(name = "disablebinary", description = "Disable native transport (binary protocol)")
    public static class DisableBinary extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.stopNativeTransport();
        }
    }

    @Command(name = "enablebinary", description = "Reenable native transport (binary protocol)")
    public static class EnableBinary extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.startNativeTransport();
        }
    }

    @Command(name = "enablegossip", description = "Reenable gossip")
    public static class EnableGossip extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.startGossiping();
        }
    }

    @Command(name = "disablegossip", description = "Disable gossip (effectively marking the node down)")
    public static class DisableGossip extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.stopGossiping();
        }
    }

    @Command(name = "enablehandoff", description = "Reenable the future hints storing on the current node")
    public static class EnableHandoff extends NodeToolCmd
    {
        @Arguments(usage = "<dc-name>,<dc-name>", description = "Enable hinted handoff only for these DCs")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() <= 1, "enablehandoff does not accept two args");
            if(args.size() == 1)
                probe.enableHintedHandoff(args.get(0));
            else
                probe.enableHintedHandoff();
        }
    }

    @Command(name = "enablethrift", description = "Reenable thrift server")
    public static class EnableThrift extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.startThriftServer();
        }
    }

    @Command(name = "getcompactionthreshold", description = "Print min and max compaction thresholds for a given table")
    public static class GetCompactionThreshold extends NodeToolCmd
    {
        @Arguments(usage = "<keyspace> <table>", description = "The keyspace with a table")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 2, "getcompactionthreshold requires ks and cf args");
            String ks = args.get(0);
            String cf = args.get(1);

            ColumnFamilyStoreMBean cfsProxy = probe.getCfsProxy(ks, cf);
            System.out.println("Current compaction thresholds for " + ks + "/" + cf + ": \n" +
                    " min = " + cfsProxy.getMinimumCompactionThreshold() + ", " +
                    " max = " + cfsProxy.getMaximumCompactionThreshold());
        }
    }

    @Command(name = "getcompactionthroughput", description = "Print the MB/s throughput cap for compaction in the system")
    public static class GetCompactionThroughput extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println("Current compaction throughput: " + probe.getCompactionThroughput() + " MB/s");
        }
    }

    @Command(name = "getstreamthroughput", description = "Print the Mb/s throughput cap for streaming in the system")
    public static class GetStreamThroughput extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println("Current stream throughput: " + probe.getStreamThroughput() + " Mb/s");
        }
    }

    @Command(name = "getendpoints", description = "Print the end points that owns the key")
    public static class GetEndpoints extends NodeToolCmd
    {
        @Arguments(usage = "<keyspace> <table> <key>", description = "The keyspace, the table, and the key for which we need to find the endpoint")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 3, "getendpoints requires ks, cf and key args");
            String ks = args.get(0);
            String cf = args.get(1);
            String key = args.get(2);

            List<InetAddress> endpoints = probe.getEndpoints(ks, cf, key);
            for (InetAddress endpoint : endpoints)
            {
                System.out.println(endpoint.getHostAddress());
            }
        }
    }

    @Command(name = "getsstables", description = "Print the sstable filenames that own the key")
    public static class GetSSTables extends NodeToolCmd
    {
        @Arguments(usage = "<keyspace> <table> <key>", description = "The keyspace, the table, and the key")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 3, "getsstables requires ks, cf and key args");
            String ks = args.get(0);
            String cf = args.get(1);
            String key = args.get(2);

            List<String> sstables = probe.getSSTables(ks, cf, key);
            for (String sstable : sstables)
            {
                System.out.println(sstable);
            }
        }
    }

    @Command(name = "gossipinfo", description = "Shows the gossip information for the cluster")
    public static class GossipInfo extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println(probe.getGossipInfo());
        }
    }

    @Command(name = "invalidatekeycache", description = "Invalidate the key cache")
    public static class InvalidateKeyCache extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.invalidateKeyCache();
        }
    }

    @Command(name = "invalidaterowcache", description = "Invalidate the row cache")
    public static class InvalidateRowCache extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.invalidateRowCache();
        }
    }

    @Command(name = "invalidatecountercache", description = "Invalidate the counter cache")
    public static class InvalidateCounterCache extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.invalidateCounterCache();
        }
    }

    @Command(name = "join", description = "Join the ring")
    public static class Join extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            checkState(!probe.isJoined(), "This node has already joined the ring.");

            try
            {
                probe.joinRing();
            } catch (IOException e)
            {
                throw new RuntimeException("Error during joining the ring", e);
            }
        }
    }

    @Command(name = "move", description = "Move node on the token ring to a new token")
    public static class Move extends NodeToolCmd
    {
        @Arguments(usage = "<new token>", description = "The new token.", required = true)
        private String newToken = EMPTY;

        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                probe.move(newToken);
            } catch (IOException e)
            {
                throw new RuntimeException("Error during moving node", e);
            }
        }
    }



    @Command(name = "pausehandoff", description = "Pause hints delivery process")
    public static class PauseHandoff extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.pauseHintsDelivery();
        }
    }

    @Command(name = "resumehandoff", description = "Resume hints delivery process")
    public static class ResumeHandoff extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.resumeHintsDelivery();
        }
    }


    @Command(name = "proxyhistograms", description = "Print statistic histograms for network operations")
    public static class ProxyHistograms extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            String[] percentiles = new String[]{"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
            double[] readLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Read"));
            double[] writeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Write"));
            double[] rangeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("RangeSlice"));

            System.out.println("proxy histograms");
            System.out.println(format("%-10s%18s%18s%18s",
                    "Percentile", "Read Latency", "Write Latency", "Range Latency"));
            System.out.println(format("%-10s%18s%18s%18s",
                    "", "(micros)", "(micros)", "(micros)"));
            for (int i = 0; i < percentiles.length; i++)
            {
                System.out.println(format("%-10s%18.2f%18.2f%18.2f",
                        percentiles[i],
                        readLatency[i],
                        writeLatency[i],
                        rangeLatency[i]));
            }
            System.out.println();
        }
    }

    @Command(name = "rebuild", description = "Rebuild data by streaming from other nodes (similarly to bootstrap)")
    public static class Rebuild extends NodeToolCmd
    {
        @Arguments(usage = "<src-dc-name>", description = "Name of DC from which to select sources for streaming. By default, pick any DC")
        private String sourceDataCenterName = null;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.rebuild(sourceDataCenterName);
        }
    }

    @Command(name = "refresh", description = "Load newly placed SSTables to the system without restart")
    public static class Refresh extends NodeToolCmd
    {
        @Arguments(usage = "<keyspace> <table>", description = "The keyspace and table name")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 2, "refresh requires ks and cf args");
            probe.loadNewSSTables(args.get(0), args.get(1));
        }
    }

    @Command(name = "removenode", description = "Show status of current node removal, force completion of pending removal or remove provided ID")
    public static class RemoveNode extends NodeToolCmd
    {
        @Arguments(title = "remove_operation", usage = "<status>|<force>|<ID>", description = "Show status of current node removal, force completion of pending removal, or remove provided ID", required = true)
        private String removeOperation = EMPTY;

        @Override
        public void execute(NodeProbe probe)
        {
            switch (removeOperation)
            {
                case "status":
                    System.out.println("RemovalStatus: " + probe.getRemovalStatus());
                    break;
                case "force":
                    System.out.println("RemovalStatus: " + probe.getRemovalStatus());
                    probe.forceRemoveCompletion();
                    break;
                default:
                    probe.removeNode(removeOperation);
                    break;
            }
        }
    }

    @Command(name = "assassinate", description = "Forcefully remove a dead node without re-replicating any data.  Use as a last resort if you cannot removenode")
    public static class Assassinate extends NodeToolCmd
    {
        @Arguments(title = "ip address", usage = "<ip_address>", description = "IP address of the endpoint to assassinate", required = true)
        private String endpoint = EMPTY;

        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                probe.assassinateEndpoint(endpoint);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Command(name = "repair", description = "Repair one or more tables")
    public static class Repair extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
        private List<String> args = new ArrayList<>();

        @Option(title = "seqential", name = {"-seq", "--sequential"}, description = "Use -seq to carry out a sequential repair")
        private boolean sequential = false;

        @Option(title = "dc parallel", name = {"-dcpar", "--dc-parallel"}, description = "Use -dcpar to repair data centers in parallel.")
        private boolean dcParallel = false;

        @Option(title = "local_dc", name = {"-local", "--in-local-dc"}, description = "Use -local to only repair against nodes in the same datacenter")
        private boolean localDC = false;

        @Option(title = "specific_dc", name = {"-dc", "--in-dc"}, description = "Use -dc to repair specific datacenters")
        private List<String> specificDataCenters = new ArrayList<>();

        @Option(title = "specific_host", name = {"-hosts", "--in-hosts"}, description = "Use -hosts to repair specific hosts")
        private List<String> specificHosts = new ArrayList<>();

        @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts")
        private String startToken = EMPTY;

        @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends")
        private String endToken = EMPTY;

        @Option(title = "primary_range", name = {"-pr", "--partitioner-range"}, description = "Use -pr to repair only the first range returned by the partitioner")
        private boolean primaryRange = false;

        @Option(title = "full", name = {"-full", "--full"}, description = "Use -full to issue a full repair.")
        private boolean fullRepair = false;

        @Option(title = "job_threads", name = {"-j", "--job-threads"}, description = "Number of threads to run repair jobs. " +
                                                                                     "Usually this means number of CFs to repair concurrently. " +
                                                                                     "WARNING: increasing this puts more load on repairing nodes, so be careful. (default: 1, max: 4)")
        private int numJobThreads = 1;

        @Option(title = "trace_repair", name = {"-tr", "--trace"}, description = "Use -tr to trace the repair. Traces are logged to system_traces.events.")
        private boolean trace = false;

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] cfnames = parseOptionalColumnFamilies(args);

            if (primaryRange && (!specificDataCenters.isEmpty() || !specificHosts.isEmpty()))
                throw new RuntimeException("Primary range repair should be performed on all nodes in the cluster.");

            for (String keyspace : keyspaces)
            {
                Map<String, String> options = new HashMap<>();
                RepairParallelism parallelismDegree = RepairParallelism.PARALLEL;
                if (sequential)
                    parallelismDegree = RepairParallelism.SEQUENTIAL;
                else if (dcParallel)
                    parallelismDegree = RepairParallelism.DATACENTER_AWARE;
                options.put(RepairOption.PARALLELISM_KEY, parallelismDegree.getName());
                options.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(primaryRange));
                options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(!fullRepair));
                options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(numJobThreads));
                options.put(RepairOption.TRACE_KEY, Boolean.toString(trace));
                options.put(RepairOption.COLUMNFAMILIES_KEY, StringUtils.join(cfnames, ","));
                if (!startToken.isEmpty() || !endToken.isEmpty())
                {
                    options.put(RepairOption.RANGES_KEY, startToken + ":" + endToken);
                }
                if (localDC)
                {
                    options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(newArrayList(probe.getDataCenter()), ","));
                }
                else
                {
                    options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(specificDataCenters, ","));
                }
                options.put(RepairOption.HOSTS_KEY, StringUtils.join(specificHosts, ","));
                try
                {
                    probe.repairAsync(System.out, keyspace, options);
                } catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during repair", e);
                }
            }
        }
    }

    @Command(name = "setcachecapacity", description = "Set global key, row, and counter cache capacities (in MB units)")
    public static class SetCacheCapacity extends NodeToolCmd
    {
        @Arguments(title = "<key-cache-capacity> <row-cache-capacity> <counter-cache-capacity>",
                   usage = "<key-cache-capacity> <row-cache-capacity> <counter-cache-capacity>",
                   description = "Key cache, row cache, and counter cache (in MB)",
                   required = true)
        private List<Integer> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 3, "setcachecapacity requires key-cache-capacity, row-cache-capacity, and counter-cache-capacity args.");
            probe.setCacheCapacities(args.get(0), args.get(1), args.get(2));
        }
    }

    @Command(name = "setcompactionthreshold", description = "Set min and max compaction thresholds for a given table")
    public static class SetCompactionThreshold extends NodeToolCmd
    {
        @Arguments(title = "<keyspace> <table> <minthreshold> <maxthreshold>", usage = "<keyspace> <table> <minthreshold> <maxthreshold>", description = "The keyspace, the table, min and max threshold", required = true)
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 4, "setcompactionthreshold requires ks, cf, min, and max threshold args.");

            int minthreshold = parseInt(args.get(2));
            int maxthreshold = parseInt(args.get(3));
            checkArgument(minthreshold >= 0 && maxthreshold >= 0, "Thresholds must be positive integers");
            checkArgument(minthreshold <= maxthreshold, "Min threshold cannot be greater than max.");
            checkArgument(minthreshold >= 2 || maxthreshold == 0, "Min threshold must be at least 2");

            probe.setCompactionThreshold(args.get(0), args.get(1), minthreshold, maxthreshold);
        }
    }

    @Command(name = "setcompactionthroughput", description = "Set the MB/s throughput cap for compaction in the system, or 0 to disable throttling")
    public static class SetCompactionThroughput extends NodeToolCmd
    {
        @Arguments(title = "compaction_throughput", usage = "<value_in_mb>", description = "Value in MB, 0 to disable throttling", required = true)
        private Integer compactionThroughput = null;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.setCompactionThroughput(compactionThroughput);
        }
    }

    @Command(name = "sethintedhandoffthrottlekb", description =  "Set hinted handoff throttle in kb per second, per delivery thread.")
    public static class SetHintedHandoffThrottleInKB extends NodeToolCmd
    {
        @Arguments(title = "throttle_in_kb", usage = "<value_in_kb_per_sec>", description = "Value in KB per second", required = true)
        private Integer throttleInKB = null;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.setHintedHandoffThrottleInKB(throttleInKB);
        }
    }

    @Command(name = "setstreamthroughput", description = "Set the Mb/s throughput cap for streaming in the system, or 0 to disable throttling")
    public static class SetStreamThroughput extends NodeToolCmd
    {
        @Arguments(title = "stream_throughput", usage = "<value_in_mb>", description = "Value in Mb, 0 to disable throttling", required = true)
        private Integer streamThroughput = null;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.setStreamThroughput(streamThroughput);
        }
    }

    @Command(name = "settraceprobability", description = "Sets the probability for tracing any given request to value. 0 disables, 1 enables for all requests, 0 is the default")
    public static class SetTraceProbability extends NodeToolCmd
    {
        @Arguments(title = "trace_probability", usage = "<value>", description = "Trace probability between 0 and 1 (ex: 0.2)", required = true)
        private Double traceProbability = null;

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(traceProbability >= 0 && traceProbability <= 1, "Trace probability must be between 0 and 1");
            probe.setTraceProbability(traceProbability);
        }
    }

    @Command(name = "snapshot", description = "Take a snapshot of specified keyspaces or a snapshot of the specified table")
    public static class Snapshot extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspaces...>]", description = "List of keyspaces. By default, all keyspaces")
        private List<String> keyspaces = new ArrayList<>();

        @Option(title = "table", name = {"-cf", "--column-family", "--table"}, description = "The table name (you must specify one and only one keyspace for using this option)")
        private String columnFamily = null;

        @Option(title = "tag", name = {"-t", "--tag"}, description = "The name of the snapshot")
        private String snapshotName = Long.toString(System.currentTimeMillis());

        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                StringBuilder sb = new StringBuilder();

                sb.append("Requested creating snapshot(s) for ");

                if (keyspaces.isEmpty())
                    sb.append("[all keyspaces]");
                else
                    sb.append("[").append(join(keyspaces, ", ")).append("]");

                if (!snapshotName.isEmpty())
                    sb.append(" with snapshot name [").append(snapshotName).append("]");

                System.out.println(sb.toString());

                probe.takeSnapshot(snapshotName, columnFamily, toArray(keyspaces, String.class));
                System.out.println("Snapshot directory: " + snapshotName);
            } catch (IOException e)
            {
                throw new RuntimeException("Error during taking a snapshot", e);
            }
        }
    }

    @Command(name = "listsnapshots", description = "Lists all the snapshots along with the size on disk and true size.")
    public static class ListSnapshots extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                System.out.println("Snapshot Details: ");

                final Map<String,TabularData> snapshotDetails = probe.getSnapshotDetails();
                if (snapshotDetails.isEmpty())
                {
                    System.out.printf("There are no snapshots");
                    return;
                }

                final long trueSnapshotsSize = probe.trueSnapshotsSize();
                final String format = "%-20s%-29s%-29s%-19s%-19s%n";
                // display column names only once
                final List<String> indexNames = snapshotDetails.entrySet().iterator().next().getValue().getTabularType().getIndexNames();
                System.out.printf(format, (Object[]) indexNames.toArray(new String[indexNames.size()]));

                for (final Map.Entry<String, TabularData> snapshotDetail : snapshotDetails.entrySet())
                {
                    Set<?> values = snapshotDetail.getValue().keySet();
                    for (Object eachValue : values)
                    {
                        final List<?> value = (List<?>) eachValue;
                        System.out.printf(format, value.toArray(new Object[value.size()]));
                    }
                }

                System.out.println("\nTotal TrueDiskSpaceUsed: " + FileUtils.stringifyFileSize(trueSnapshotsSize) + "\n");
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error during list snapshot", e);
            }
        }
    }

    @Command(name = "status", description = "Print cluster information (state, load, IDs, ...)")
    public static class Status extends NodeToolCmd
    {
        @Arguments(usage = "[<keyspace>]", description = "The keyspace name")
        private String keyspace = null;

        @Option(title = "resolve_ip", name = {"-r", "--resolve-ip"}, description = "Show node domain names instead of IPs")
        private boolean resolveIp = false;

        private boolean hasEffectiveOwns = false;
        private boolean isTokenPerNode = true;
        private int maxAddressLength = 0;
        private String format = null;
        private Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;
        private Map<String, String> loadMap, hostIDMap, tokensToEndpoints;
        private EndpointSnitchInfoMBean epSnitchInfo;

        @Override
        public void execute(NodeProbe probe)
        {
            joiningNodes = probe.getJoiningNodes();
            leavingNodes = probe.getLeavingNodes();
            movingNodes = probe.getMovingNodes();
            loadMap = probe.getLoadMap();
            tokensToEndpoints = probe.getTokenToEndpointMap();
            liveNodes = probe.getLiveNodes();
            unreachableNodes = probe.getUnreachableNodes();
            hostIDMap = probe.getHostIdMap();
            epSnitchInfo = probe.getEndpointSnitchInfoProxy();

            StringBuffer errors = new StringBuffer();

            Map<InetAddress, Float> ownerships = null;
            try
            {
                ownerships = probe.effectiveOwnership(keyspace);
                hasEffectiveOwns = true;
            }
            catch (IllegalStateException e)
            {
                ownerships = probe.getOwnership();
                errors.append("Note: " + e.getMessage() + "%n");
            }
            catch (IllegalArgumentException ex)
            {
                System.out.printf("%nError: " + ex.getMessage() + "%n");
                System.exit(1);
            }

            Map<String, SetHostStat> dcs = getOwnershipByDc(probe, resolveIp, tokensToEndpoints, ownerships);

            // More tokens than nodes (aka vnodes)?
            if (dcs.values().size() < tokensToEndpoints.keySet().size())
                isTokenPerNode = false;

            findMaxAddressLength(dcs);

            // Datacenters
            for (Map.Entry<String, SetHostStat> dc : dcs.entrySet())
            {
                String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
                System.out.printf(dcHeader);
                for (int i = 0; i < (dcHeader.length() - 1); i++) System.out.print('=');
                System.out.println();

                // Legend
                System.out.println("Status=Up/Down");
                System.out.println("|/ State=Normal/Leaving/Joining/Moving");

                printNodesHeader(hasEffectiveOwns, isTokenPerNode);

                ArrayListMultimap<InetAddress, HostStat> hostToTokens = ArrayListMultimap.create();
                for (HostStat stat : dc.getValue())
                    hostToTokens.put(stat.endpoint, stat);

                for (InetAddress endpoint : hostToTokens.keySet())
                {
                    Float owns = ownerships.get(endpoint);
                    List<HostStat> tokens = hostToTokens.get(endpoint);
                    printNode(endpoint.getHostAddress(), owns, tokens, hasEffectiveOwns, isTokenPerNode);
                }
            }

            System.out.printf("%n" + errors.toString());

        }

        private void findMaxAddressLength(Map<String, SetHostStat> dcs)
        {
            maxAddressLength = 0;
            for (Map.Entry<String, SetHostStat> dc : dcs.entrySet())
            {
                for (HostStat stat : dc.getValue())
                {
                    maxAddressLength = Math.max(maxAddressLength, stat.ipOrDns().length());
                }
            }
        }

        private void printNodesHeader(boolean hasEffectiveOwns, boolean isTokenPerNode)
        {
            String fmt = getFormat(hasEffectiveOwns, isTokenPerNode);
            String owns = hasEffectiveOwns ? "Owns (effective)" : "Owns";

            if (isTokenPerNode)
                System.out.printf(fmt, "-", "-", "Address", "Load", owns, "Host ID", "Token", "Rack");
            else
                System.out.printf(fmt, "-", "-", "Address", "Load", "Tokens", owns, "Host ID", "Rack");
        }

        private void printNode(String endpoint, Float owns, List<HostStat> tokens, boolean hasEffectiveOwns, boolean isTokenPerNode)
        {
            String status, state, load, strOwns, hostID, rack, fmt;
            fmt = getFormat(hasEffectiveOwns, isTokenPerNode);
            if (liveNodes.contains(endpoint)) status = "U";
            else if (unreachableNodes.contains(endpoint)) status = "D";
            else status = "?";
            if (joiningNodes.contains(endpoint)) state = "J";
            else if (leavingNodes.contains(endpoint)) state = "L";
            else if (movingNodes.contains(endpoint)) state = "M";
            else state = "N";

            load = loadMap.containsKey(endpoint) ? loadMap.get(endpoint) : "?";
            strOwns = owns != null && hasEffectiveOwns ? new DecimalFormat("##0.0%").format(owns) : "?";
            hostID = hostIDMap.get(endpoint);

            try
            {
                rack = epSnitchInfo.getRack(endpoint);
            } catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }

            String endpointDns = tokens.get(0).ipOrDns();
            if (isTokenPerNode)
                System.out.printf(fmt, status, state, endpointDns, load, strOwns, hostID, tokens.get(0).token, rack);
            else
                System.out.printf(fmt, status, state, endpointDns, load, tokens.size(), strOwns, hostID, rack);
        }

        private String getFormat(
                boolean hasEffectiveOwns,
                boolean isTokenPerNode)
        {
            if (format == null)
            {
                StringBuilder buf = new StringBuilder();
                String addressPlaceholder = String.format("%%-%ds  ", maxAddressLength);
                buf.append("%s%s  ");                         // status
                buf.append(addressPlaceholder);               // address
                buf.append("%-9s  ");                         // load
                if (!isTokenPerNode)
                    buf.append("%-11s  ");                     // "Tokens"
                if (hasEffectiveOwns)
                    buf.append("%-16s  ");                    // "Owns (effective)"
                else
                    buf.append("%-6s  ");                     // "Owns
                buf.append("%-36s  ");                        // Host ID
                if (isTokenPerNode)
                    buf.append("%-39s  ");                    // token
                buf.append("%s%n");                           // "Rack"

                format = buf.toString();
            }

            return format;
        }
    }

    private static Map<String, SetHostStat> getOwnershipByDc(NodeProbe probe, boolean resolveIp,
                                                             Map<String, String> tokenToEndpoint,
                                                             Map<InetAddress, Float> ownerships)
    {
        Map<String, SetHostStat> ownershipByDc = Maps.newLinkedHashMap();
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

    static class SetHostStat implements Iterable<HostStat>
    {
        final List<HostStat> hostStats = new ArrayList<HostStat>();
        final boolean resolveIp;

        public SetHostStat(boolean resolveIp)
        {
            this.resolveIp = resolveIp;
        }

        public int size()
        {
            return hostStats.size();
        }

        @Override
        public Iterator<HostStat> iterator()
        {
            return hostStats.iterator();
        }

        public void add(String token, String host, Map<InetAddress, Float> ownerships) throws UnknownHostException
        {
            InetAddress endpoint = InetAddress.getByName(host);
            Float owns = ownerships.get(endpoint);
            hostStats.add(new HostStat(token, endpoint, resolveIp, owns));
        }
    }

    static class HostStat
    {
        public final InetAddress endpoint;
        public final boolean resolveIp;
        public final Float owns;
        public final String token;

        public HostStat(String token, InetAddress endpoint, boolean resolveIp, Float owns)
        {
            this.token = token;
            this.endpoint = endpoint;
            this.resolveIp = resolveIp;
            this.owns = owns;
        }

        public String ipOrDns()
        {
            return resolveIp ? endpoint.getHostName() : endpoint.getHostAddress();
        }
    }

    @Command(name = "statusbinary", description = "Status of native transport (binary protocol)")
    public static class StatusBinary extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println(
                    probe.isNativeTransportRunning()
                    ? "running"
                    : "not running");
        }
    }

    @Command(name = "statusgossip", description = "Status of gossip")
    public static class StatusGossip extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println(
                    probe.isGossipRunning()
                    ? "running"
                    : "not running");
        }
    }

    @Command(name = "statusthrift", description = "Status of thrift server")
    public static class StatusThrift extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println(
                    probe.isThriftServerRunning()
                    ? "running"
                    : "not running");
        }
    }

    @Command(name = "statusbackup", description = "Status of incremental backup")
    public static class StatusBackup extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println(
                    probe.isIncrementalBackupsEnabled()
                    ? "running"
                    : "not running");
        }
    }

    @Command(name = "statushandoff", description = "Status of storing future hints on the current node")
    public static class StatusHandoff extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println(
                    probe.isHandoffEnabled()
                    ? "running"
                    : "not running");
        }
    }

    @Command(name = "stop", description = "Stop compaction")
    public static class Stop extends NodeToolCmd
    {
        @Arguments(title = "compaction_type", usage = "<compaction type>", description = "Supported types are COMPACTION, VALIDATION, CLEANUP, SCRUB, INDEX_BUILD", required = true)
        private OperationType compactionType = OperationType.UNKNOWN;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.stop(compactionType.name());
        }
    }

    @Command(name = "stopdaemon", description = "Stop cassandra daemon")
    public static class StopDaemon extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                probe.stopCassandraDaemon();
            } catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // ignored
            }
            System.out.println("Cassandra has shutdown.");
        }
    }

    @Command(name = "version", description = "Print cassandra version")
    public static class Version extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println("ReleaseVersion: " + probe.getReleaseVersion());
        }
    }

    @Command(name = "describering", description = "Shows the token ranges info of a given keyspace")
    public static class DescribeRing extends NodeToolCmd
    {
        @Arguments(description = "The keyspace name", required = true)
        String keyspace = EMPTY;

        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println("Schema Version:" + probe.getSchemaVersion());
            System.out.println("TokenRange: ");
            try
            {
                for (String tokenRangeString : probe.describeRing(keyspace))
                {
                    System.out.println("\t" + tokenRangeString);
                }
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Command(name = "rangekeysample", description = "Shows the sampled keys held across all keyspaces")
    public static class RangeKeySample extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.println("RangeKeySample: ");
            List<String> tokenStrings = probe.sampleKeyRange();
            for (String tokenString : tokenStrings)
            {
                System.out.println("\t" + tokenString);
            }
        }
    }

    @Command(name = "rebuild_index", description = "A full rebuild of native secondary indexes for a given table")
    public static class RebuildIndex extends NodeToolCmd
    {
        @Arguments(usage = "<keyspace> <table> <indexName...>", description = "The keyspace and table name followed by a list of index names (IndexNameExample: Standard3.IdxName Standard3.IdxName1)")
        List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() >= 3, "rebuild_index requires ks, cf and idx args");
            probe.rebuildIndex(args.get(0), args.get(1), toArray(args.subList(2, args.size()), String.class));
        }
    }

    @Command(name = "resetlocalschema", description = "Reset node's local schema and resync")
    public static class ResetLocalSchema extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                probe.resetLocalSchema();
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Command(name = "enablebackup", description = "Enable incremental backup")
    public static class EnableBackup extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.setIncrementalBackupsEnabled(true);
        }
    }

    @Command(name = "disablebackup", description = "Disable incremental backup")
    public static class DisableBackup extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.setIncrementalBackupsEnabled(false);
        }
    }

    @Command(name = "setcachekeystosave", description = "Set number of keys saved by each cache for faster post-restart warmup. 0 to disable")
    public static class SetCacheKeysToSave extends NodeToolCmd
    {
        @Arguments(title = "<key-cache-keys-to-save> <row-cache-keys-to-save> <counter-cache-keys-to-save>",
                   usage = "<key-cache-keys-to-save> <row-cache-keys-to-save> <counter-cache-keys-to-save>",
                   description = "The number of keys saved by each cache. 0 to disable",
                   required = true)
        private List<Integer> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 3, "setcachekeystosave requires key-cache-keys-to-save, row-cache-keys-to-save, and counter-cache-keys-to-save args.");
            probe.setCacheKeysToSave(args.get(0), args.get(1), args.get(2));
        }
    }

    @Command(name = "reloadtriggers", description = "Reload trigger classes")
    public static class ReloadTriggers extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.reloadTriggers();
        }
    }

    @Command(name = "disablehandoff", description = "Disable storing hinted handoffs")
    public static class DisableHandoff extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.disableHintedHandoff();
        }
    }

    @Command(name = "disablethrift", description = "Disable thrift server")
    public static class DisableThrift extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            probe.stopThriftServer();
        }
    }

    @Command(name = "drain", description = "Drain the node (stop accepting writes and flush all tables)")
    public static class Drain extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                probe.drain();
            } catch (IOException | InterruptedException | ExecutionException e)
            {
                throw new RuntimeException("Error occurred during flushing", e);
            }
        }
    }

    @Command(name = "tpstats", description = "Print usage statistics of thread pools")
    public static class TpStats extends NodeTool.NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            System.out.printf("%-25s%10s%10s%15s%10s%18s%n", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked");

            for (Stage stage : Stage.jmxEnabledStages())
            {
                System.out.printf("%-25s%10s%10s%15s%10s%18s%n",
                                  stage.getJmxName(),
                                  probe.getThreadPoolMetric(stage, "ActiveTasks"),
                                  probe.getThreadPoolMetric(stage, "PendingTasks"),
                                  probe.getThreadPoolMetric(stage, "CompletedTasks"),
                                  probe.getThreadPoolMetric(stage, "CurrentlyBlockedTasks"),
                                  probe.getThreadPoolMetric(stage, "TotalBlockedTasks"));
            }

            System.out.printf("%n%-20s%10s%n", "Message type", "Dropped");
            for (Map.Entry<String, Integer> entry : probe.getDroppedMessages().entrySet())
                System.out.printf("%-20s%10s%n", entry.getKey(), entry.getValue());
        }
    }

    @Command(name = "gcstats", description = "Print GC Statistics")
    public static class GcStats extends NodeTool.NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            double[] stats = probe.getAndResetGCStats();
            double mean = stats[2] / stats[5];
            double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));
            System.out.printf("%20s%20s%20s%20s%20s%20s%n", "Interval (ms)", "Max GC Elapsed (ms)", "Total GC Elapsed (ms)", "Stdev GC Elapsed (ms)", "GC Reclaimed (MB)", "Collections");
            System.out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%n", stats[0], stats[1], stats[2], stdev, stats[4], stats[5]);
        }
    }

    @Command(name = "truncatehints", description = "Truncate all hints on the local node, or truncate hints for the endpoint(s) specified.")
    public static class TruncateHints extends NodeToolCmd
    {
        @Arguments(usage = "[endpoint ... ]", description = "Endpoint address(es) to delete hints for, either ip address (\"127.0.0.1\") or hostname")
        private String endpoint = EMPTY;

        @Override
        public void execute(NodeProbe probe)
        {
            if (endpoint.isEmpty())
                probe.truncateHints();
            else
                probe.truncateHints(endpoint);
        }
    }

    @Command(name = "setlogginglevel", description = "Set the log level threshold for a given class. If both class and level are empty/null, it will reset to the initial configuration")
    public static class SetLoggingLevel extends NodeToolCmd
    {
        @Arguments(usage = "<class> <level>", description = "The class to change the level for and the log level threshold to set (can be empty)")
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            String classQualifier = args.size() >= 1 ? args.get(0) : EMPTY;
            String level = args.size() == 2 ? args.get(1) : EMPTY;
            probe.setLoggingLevel(classQualifier, level);
        }
    }

    @Command(name = "getlogginglevels", description = "Get the runtime logging levels")
    public static class GetLoggingLevels extends NodeToolCmd
    {
        @Override
        public void execute(NodeProbe probe)
        {
            // what if some one set a very long logger name? 50 space may not be enough...
            System.out.printf("%n%-50s%10s%n", "Logger Name", "Log Level");
            for (Map.Entry<String, String> entry : probe.getLoggingLevels().entrySet())
                System.out.printf("%-50s%10s%n", entry.getKey(), entry.getValue());
        }
    }

}
