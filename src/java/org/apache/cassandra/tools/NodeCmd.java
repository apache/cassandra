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
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import javax.management.openmbean.TabularData;

import com.google.common.base.Joiner;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.yammer.metrics.reporting.JmxReporter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.utils.Pair;

public class NodeCmd
{
    private static final String HISTORYFILE = "nodetool.history";
    private static final Pair<String, String> SNAPSHOT_COLUMNFAMILY_OPT = Pair.create("cf", "column-family");
    private static final Pair<String, String> HOST_OPT = Pair.create("h", "host");
    private static final Pair<String, String> PORT_OPT = Pair.create("p", "port");
    private static final Pair<String, String> USERNAME_OPT = Pair.create("u", "username");
    private static final Pair<String, String> PASSWORD_OPT = Pair.create("pw", "password");
    private static final Pair<String, String> TAG_OPT = Pair.create("t", "tag");
    private static final Pair<String, String> TOKENS_OPT = Pair.create("T", "tokens");
    private static final Pair<String, String> PRIMARY_RANGE_OPT = Pair.create("pr", "partitioner-range");
    private static final Pair<String, String> PARALLEL_REPAIR_OPT = Pair.create("par", "parallel");
    private static final Pair<String, String> LOCAL_DC_REPAIR_OPT = Pair.create("local", "in-local-dc");
    private static final Pair<String, String> DC_REPAIR_OPT = Pair.create("dc", "in-dc");
    private static final Pair<String, String> START_TOKEN_OPT = Pair.create("st", "start-token");
    private static final Pair<String, String> END_TOKEN_OPT = Pair.create("et", "end-token");
    private static final Pair<String, String> UPGRADE_ALL_SSTABLE_OPT = Pair.create("a", "include-all-sstables");
    private static final Pair<String, String> NO_SNAPSHOT = Pair.create("ns", "no-snapshot");
    private static final Pair<String, String> CFSTATS_IGNORE_OPT = Pair.create("i", "ignore");
    private static final Pair<String, String> RESOLVE_IP = Pair.create("r", "resolve-ip");

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 7199;

    private static final ToolOptions options = new ToolOptions();

    private final NodeProbe probe;

    static
    {
        options.addOption(SNAPSHOT_COLUMNFAMILY_OPT, true, "only take a snapshot of the specified table (column family)");
        options.addOption(HOST_OPT,     true, "node hostname or ip address");
        options.addOption(PORT_OPT,     true, "remote jmx agent port number");
        options.addOption(USERNAME_OPT, true, "remote jmx agent username");
        options.addOption(PASSWORD_OPT, true, "remote jmx agent password");
        options.addOption(TAG_OPT,      true, "optional name to give a snapshot");
        options.addOption(TOKENS_OPT,   false, "display all tokens");
        options.addOption(PRIMARY_RANGE_OPT, false, "only repair the first range returned by the partitioner for the node");
        options.addOption(PARALLEL_REPAIR_OPT, false, "repair nodes in parallel.");
        options.addOption(LOCAL_DC_REPAIR_OPT, false, "only repair against nodes in the same datacenter");
        options.addOption(DC_REPAIR_OPT, true, "only repair against nodes in the specified datacenters (comma separated)");
        options.addOption(START_TOKEN_OPT, true, "token at which repair range starts");
        options.addOption(END_TOKEN_OPT, true, "token at which repair range ends");
        options.addOption(UPGRADE_ALL_SSTABLE_OPT, false, "includes sstables that are already on the most recent version during upgradesstables");
        options.addOption(NO_SNAPSHOT, false, "disables snapshot creation for scrub");
        options.addOption(CFSTATS_IGNORE_OPT, false, "ignore the supplied list of keyspace.columnfamiles in statistics");
        options.addOption(RESOLVE_IP, false, "show node domain names instead of IPs");
    }

    public NodeCmd(NodeProbe probe)
    {
        this.probe = probe;
    }

    private enum NodeCommand
    {
        CFHISTOGRAMS,
        CFSTATS,
        CLEANUP,
        CLEARSNAPSHOT,
        COMPACT,
        COMPACTIONSTATS,
        COMPACTIONHISTORY,
        DECOMMISSION,
        DESCRIBECLUSTER,
        DISABLEBINARY,
        DISABLEGOSSIP,
        DISABLEHANDOFF,
        DISABLETHRIFT,
        DRAIN,
        ENABLEBINARY,
        ENABLEGOSSIP,
        ENABLEHANDOFF,
        ENABLETHRIFT,
        FLUSH,
        GETCOMPACTIONTHRESHOLD,
        DISABLEAUTOCOMPACTION,
        ENABLEAUTOCOMPACTION,
        GETCOMPACTIONTHROUGHPUT,
        GETSTREAMTHROUGHPUT,
        GETENDPOINTS,
        GETSSTABLES,
        GOSSIPINFO,
        HELP,
        INFO,
        INVALIDATEKEYCACHE,
        INVALIDATEROWCACHE,
        JOIN,
        MOVE,
        NETSTATS,
        PAUSEHANDOFF,
        PROXYHISTOGRAMS,
        REBUILD,
        REFRESH,
        REMOVETOKEN,
        REMOVENODE,
        REPAIR,
        RESUMEHANDOFF,
        RING,
        SCRUB,
        SETCACHECAPACITY,
        SETCOMPACTIONTHRESHOLD,
        SETCOMPACTIONTHROUGHPUT,
        SETSTREAMTHROUGHPUT,
        SETTRACEPROBABILITY,
        SNAPSHOT,
        STATUS,
        STATUSBINARY,
        STATUSTHRIFT,
        STOP,
        TPSTATS,
        UPGRADESSTABLES,
        VERSION,
        DESCRIBERING,
        RANGEKEYSAMPLE,
        REBUILD_INDEX,
        RESETLOCALSCHEMA,
        ENABLEBACKUP,
        DISABLEBACKUP,
        SETCACHEKEYSTOSAVE,
        RELOADTRIGGERS
    }


    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        StringBuilder header = new StringBuilder(512);
        header.append("\nAvailable commands\n");
        final NodeToolHelp ntHelp = loadHelp();
        Collections.sort(ntHelp.commands, new Comparator<NodeToolHelp.NodeToolCommand>() 
        {
            @Override
            public int compare(NodeToolHelp.NodeToolCommand o1, NodeToolHelp.NodeToolCommand o2) 
            {
                return o1.name.compareTo(o2.name);
            }
        });
        for(NodeToolHelp.NodeToolCommand cmd : ntHelp.commands)
            addCmdHelp(header, cmd);
        String usage = String.format("java %s --host <arg> <command>%n", NodeCmd.class.getName());
        hf.printHelp(usage, "", options, "");
        System.out.println(header.toString());
    }

    private static NodeToolHelp loadHelp()
    {
        final InputStream is = NodeCmd.class.getClassLoader().getResourceAsStream("org/apache/cassandra/tools/NodeToolHelp.yaml");
        assert is != null;

        try
        {
            final Constructor constructor = new Constructor(NodeToolHelp.class);
            final Yaml yaml = new Yaml(constructor);
            return (NodeToolHelp)yaml.load(is);
        }
        finally
        {
            FileUtils.closeQuietly(is);
        }
    }

    private static void addCmdHelp(StringBuilder sb, NodeToolHelp.NodeToolCommand cmd)
    {
        sb.append("  ").append(cmd.name);
        // Ghetto indentation (trying, but not too hard, to not look too bad)
        if (cmd.name.length() <= 20)
            for (int i = cmd.name.length(); i < 22; ++i) sb.append(" ");
        sb.append(" - ").append(cmd.help);
  }


    /**
     * Write a textual representation of the Cassandra ring.
     *
     * @param outs
     *            the stream to write to
     */
    public void printRing(PrintStream outs, String keyspace)
    {
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
        LinkedHashMultimap<String, String> endpointsToTokens = LinkedHashMultimap.create();
        for (Map.Entry<String, String> entry : tokensToEndpoints.entrySet())
            endpointsToTokens.put(entry.getValue(), entry.getKey());

        int maxAddressLength = Collections.max(endpointsToTokens.keys(), new Comparator<String>() {
            @Override
            public int compare(String first, String second)
            {
                return ((Integer)first.length()).compareTo((Integer)second.length());
            }
        }).length();

        String formatPlaceholder = "%%-%ds  %%-12s%%-7s%%-8s%%-16s%%-20s%%-44s%%n";
        String format = String.format(formatPlaceholder, maxAddressLength);

        // Calculate per-token ownership of the ring
        Map<InetAddress, Float> ownerships;
        boolean keyspaceSelected;
        try
        {
            ownerships = probe.effectiveOwnership(keyspace);
            keyspaceSelected = true;
        }
        catch (IllegalStateException ex)
        {
            ownerships = probe.getOwnership();
            outs.printf("Note: Ownership information does not include topology; for complete information, specify a keyspace%n");
            keyspaceSelected = false;
        }
        try
        {
            outs.println();
            Map<String, Map<InetAddress, Float>> perDcOwnerships = Maps.newLinkedHashMap();
            // get the different datasets and map to tokens
            for (Map.Entry<InetAddress, Float> ownership : ownerships.entrySet())
            {
                String dc = probe.getEndpointSnitchInfoProxy().getDatacenter(ownership.getKey().getHostAddress());
                if (!perDcOwnerships.containsKey(dc))
                    perDcOwnerships.put(dc, new LinkedHashMap<InetAddress, Float>());
                perDcOwnerships.get(dc).put(ownership.getKey(), ownership.getValue());
            }
            for (Map.Entry<String, Map<InetAddress, Float>> entry : perDcOwnerships.entrySet())
                printDc(outs, format, entry.getKey(), endpointsToTokens, keyspaceSelected, entry.getValue());
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        if(DatabaseDescriptor.getNumTokens() > 1)
        {
            outs.println("  Warning: \"nodetool ring\" is used to output all the tokens of a node.");
            outs.println("  To view status related info of a node use \"nodetool status\" instead.\n");
        }
    }

    private void printDc(PrintStream outs, String format, String dc, LinkedHashMultimap<String, String> endpointsToTokens,
            boolean keyspaceSelected, Map<InetAddress, Float> filteredOwnerships)
    {
        Collection<String> liveNodes = probe.getLiveNodes();
        Collection<String> deadNodes = probe.getUnreachableNodes();
        Collection<String> joiningNodes = probe.getJoiningNodes();
        Collection<String> leavingNodes = probe.getLeavingNodes();
        Collection<String> movingNodes = probe.getMovingNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        outs.println("Datacenter: " + dc);
        outs.println("==========");

        // get the total amount of replicas for this dc and the last token in this dc's ring
        List<String> tokens = new ArrayList<String>();
        String lastToken = "";

        for (Map.Entry<InetAddress, Float> entry : filteredOwnerships.entrySet())
        {
            tokens.addAll(endpointsToTokens.get(entry.getKey().getHostAddress()));
            lastToken = tokens.get(tokens.size() - 1);
        }


        outs.printf(format, "Address", "Rack", "Status", "State", "Load", "Owns", "Token");

        if (filteredOwnerships.size() > 1)
            outs.printf(format, "", "", "", "", "", "", lastToken);
        else
            outs.println();

        for (Map.Entry<InetAddress, Float> entry : filteredOwnerships.entrySet())
        {
            String endpoint = entry.getKey().getHostAddress();
            for (String token : endpointsToTokens.get(endpoint))
            {
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
                String owns = new DecimalFormat("##0.00%").format(entry.getValue());
                outs.printf(format, endpoint, rack, status, state, load, owns, token);
            }
        }
        outs.println();
    }

    private class ClusterStatus
    {
        String kSpace = null, format = null;
        int maxAddressLength;
        Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;
        Map<String, String> loadMap, hostIDMap, tokensToEndpoints;
        EndpointSnitchInfoMBean epSnitchInfo;
        PrintStream outs;
        private final boolean resolveIp;

        ClusterStatus(PrintStream outs, String kSpace, boolean resolveIp)
        {
            this.kSpace = kSpace;
            this.outs = outs;
            this.resolveIp = resolveIp;
            joiningNodes = probe.getJoiningNodes();
            leavingNodes = probe.getLeavingNodes();
            movingNodes = probe.getMovingNodes();
            loadMap = probe.getLoadMap();
            tokensToEndpoints = probe.getTokenToEndpointMap();
            liveNodes = probe.getLiveNodes();
            unreachableNodes = probe.getUnreachableNodes();
            hostIDMap = probe.getHostIdMap();
            epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        }

        private void printStatusLegend()
        {
            outs.println("Status=Up/Down");
            outs.println("|/ State=Normal/Leaving/Joining/Moving");
        }

        class SetHostStat implements Iterable<HostStat> {
            final List<HostStat> hostStats = new ArrayList<HostStat>();

            public SetHostStat() {}

            public SetHostStat(Map<InetAddress, Float> ownerships) {
                for (Map.Entry<InetAddress, Float> entry : ownerships.entrySet()) {
                    hostStats.add(new HostStat(entry));
                }
            }

            @Override
            public Iterator<HostStat> iterator() {
                return hostStats.iterator();
            }

            public void add(HostStat entry) {
                hostStats.add(entry);
            }
        }

        class HostStat {
            public final String ip;
            public final String dns;
            public final Float owns;

            public HostStat(Map.Entry<InetAddress, Float> ownership) {
                this.ip = ownership.getKey().getHostAddress();
                this.dns = ownership.getKey().getHostName();
                this.owns = ownership.getValue();
            }

            public String ipOrDns() {
                if (resolveIp) {
                    return dns;
                }
                return ip;
            }
        }

        private Map<String, SetHostStat> getOwnershipByDc(SetHostStat ownerships)
        throws UnknownHostException
        {
            Map<String, SetHostStat> ownershipByDc = Maps.newLinkedHashMap();
            EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();

            for (HostStat ownership : ownerships)
            {
                String dc = epSnitchInfo.getDatacenter(ownership.ip);
                if (!ownershipByDc.containsKey(dc))
                    ownershipByDc.put(dc, new SetHostStat());
                ownershipByDc.get(dc).add(ownership);
            }

            return ownershipByDc;
        }

        private String getFormat(boolean hasEffectiveOwns, boolean isTokenPerNode)
        {
            if (format == null)
            {
                StringBuilder buf = new StringBuilder();
                String addressPlaceholder = String.format("%%-%ds  ", maxAddressLength);
                buf.append("%s%s  ");                         // status
                buf.append(addressPlaceholder);               // address
                buf.append("%-9s  ");                         // load
                if (!isTokenPerNode)  buf.append("%-6s  ");   // "Tokens"
                if (hasEffectiveOwns) buf.append("%-16s  ");  // "Owns (effective)"
                else                  buf.append("%-5s  ");   // "Owns
                buf.append("%-36s  ");                        // Host ID
                if (isTokenPerNode)   buf.append("%-39s  ");  // token
                buf.append("%s%n");                           // "Rack"

                format = buf.toString();
            }

            return format;
        }

        private void printNode(HostStat hostStat,
                boolean hasEffectiveOwns, boolean isTokenPerNode) throws UnknownHostException
        {
            String status, state, load, strOwns, hostID, rack, fmt;
            fmt = getFormat(hasEffectiveOwns, isTokenPerNode);
            String endpoint = hostStat.ip;
            if      (liveNodes.contains(endpoint))        status = "U";
            else if (unreachableNodes.contains(endpoint)) status = "D";
            else                                          status = "?";
            if      (joiningNodes.contains(endpoint))     state = "J";
            else if (leavingNodes.contains(endpoint))     state = "L";
            else if (movingNodes.contains(endpoint))      state = "M";
            else                                          state = "N";

            load = loadMap.containsKey(endpoint) ? loadMap.get(endpoint) : "?";
            strOwns = new DecimalFormat("##0.0%").format(hostStat.owns);
            hostID = hostIDMap.get(endpoint);
            rack = epSnitchInfo.getRack(endpoint);

            if (isTokenPerNode)
            {
                outs.printf(fmt, status, state, hostStat.ipOrDns(), load, strOwns, hostID, probe.getTokens(endpoint).get(0), rack);
            }
            else
            {
                int tokens = probe.getTokens(endpoint).size();
                outs.printf(fmt, status, state, hostStat.ipOrDns(), load, tokens, strOwns, hostID, rack);
            }
        }

        private void printNodesHeader(boolean hasEffectiveOwns, boolean isTokenPerNode)
        {
            String fmt = getFormat(hasEffectiveOwns, isTokenPerNode);
            String owns = hasEffectiveOwns ? "Owns (effective)" : "Owns";

            if (isTokenPerNode)
                outs.printf(fmt, "-", "-", "Address", "Load", owns, "Host ID", "Token", "Rack");
            else
                outs.printf(fmt, "-", "-", "Address", "Load", "Tokens", owns, "Host ID", "Rack");
        }

        void findMaxAddressLength(Map<String, SetHostStat> dcs) {
            maxAddressLength = 0;
            for (Map.Entry<String, SetHostStat> dc : dcs.entrySet())
            {
                for (HostStat stat : dc.getValue()) {
                    maxAddressLength = Math.max(maxAddressLength, stat.ipOrDns().length());
                }
            }
        }

        void print() throws UnknownHostException
        {
            SetHostStat ownerships;
            boolean hasEffectiveOwns = false, isTokenPerNode = true;

            try
            {
                ownerships = new SetHostStat(probe.effectiveOwnership(kSpace));
                hasEffectiveOwns = true;
            }
            catch (IllegalStateException e)
            {
                ownerships = new SetHostStat(probe.getOwnership());
            }

            // More tokens then nodes (aka vnodes)?
            if (new HashSet<String>(tokensToEndpoints.values()).size() < tokensToEndpoints.keySet().size())
                isTokenPerNode = false;

            Map<String, SetHostStat> dcs = getOwnershipByDc(ownerships);

            findMaxAddressLength(dcs);

            // Datacenters
            for (Map.Entry<String, SetHostStat> dc : dcs.entrySet())
            {
                String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
                outs.printf(dcHeader);
                for (int i=0; i < (dcHeader.length() - 1); i++) outs.print('=');
                outs.println();

                printStatusLegend();
                printNodesHeader(hasEffectiveOwns, isTokenPerNode);

                // Nodes
                for (HostStat entry : dc.getValue())
                    printNode(entry, hasEffectiveOwns, isTokenPerNode);
            }
        }
    }

    /** Writes a keyspaceName of cluster-wide node information to a PrintStream
     * @throws UnknownHostException */
    public void printClusterStatus(PrintStream outs, String keyspace, boolean resolveIp) throws UnknownHostException
    {
        new ClusterStatus(outs, keyspace, resolveIp).print();
    }

    public void printThreadPoolStats(PrintStream outs)
    {
        outs.printf("%-25s%10s%10s%15s%10s%18s%n", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked");

        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        while (threads.hasNext())
        {
            Entry<String, JMXEnabledThreadPoolExecutorMBean> thread = threads.next();
            String poolName = thread.getKey();
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = thread.getValue();
            outs.printf("%-25s%10s%10s%15s%10s%18s%n",
                        poolName,
                        threadPoolProxy.getActiveCount(),
                        threadPoolProxy.getPendingTasks(),
                        threadPoolProxy.getCompletedTasks(),
                        threadPoolProxy.getCurrentlyBlockedTasks(),
                        threadPoolProxy.getTotalBlockedTasks());
        }

        outs.printf("%n%-20s%10s%n", "Message type", "Dropped");
        for (Entry<String, Integer> entry : probe.getDroppedMessages().entrySet())
            outs.printf("%-20s%10s%n", entry.getKey(), entry.getValue());
    }

    /**
     * Write node information.
     *
     * @param outs the stream to write to
     */
    public void printInfo(PrintStream outs, ToolCommandLine cmd)
    {
        boolean gossipInitialized = probe.isInitialized();
        List<String> toks = probe.getTokens();

        // If there is just 1 token, print it now like we always have, otherwise,
        // require that -T/--tokens be passed (that output is potentially verbose).
        if (toks.size() == 1)
            outs.printf("%-17s: %s%n", "Token", toks.get(0));
        else if (!cmd.hasOption(TOKENS_OPT.left))
            outs.printf("%-17s: (invoke with -T/--tokens to see all %d tokens)%n", "Token", toks.size());

        outs.printf("%-17s: %s%n", "ID", probe.getLocalHostId());
        outs.printf("%-17s: %s%n", "Gossip active", gossipInitialized);
        outs.printf("%-17s: %s%n", "Thrift active", probe.isThriftServerRunning());
        outs.printf("%-17s: %s%n", "Native Transport active", probe.isNativeTransportRunning());
        outs.printf("%-17s: %s%n", "Load", probe.getLoadString());
        if (gossipInitialized)
            outs.printf("%-17s: %s%n", "Generation No", probe.getCurrentGenerationNumber());
        else
            outs.printf("%-17s: %s%n", "Generation No", 0);

        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        outs.printf("%-17s: %d%n", "Uptime (seconds)", secondsUp);

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double)heapUsage.getMax() / (1024 * 1024);
        outs.printf("%-17s: %.2f / %.2f%n", "Heap Memory (MB)", memUsed, memMax);

        // Data Center/Rack
        outs.printf("%-17s: %s%n", "Data Center", probe.getDataCenter());
        outs.printf("%-17s: %s%n", "Rack", probe.getRack());

        // Exceptions
        outs.printf("%-17s: %s%n", "Exceptions", probe.getStorageMetric("Exceptions"));

        CacheServiceMBean cacheService = probe.getCacheServiceMBean();

        // Key Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        outs.printf("%-17s: entries %d, size %d (bytes), capacity %d (bytes), %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                    "Key Cache",
                    probe.getCacheMetric("KeyCache", "Entries"),
                    probe.getCacheMetric("KeyCache", "Size"),
                    probe.getCacheMetric("KeyCache", "Capacity"),
                    probe.getCacheMetric("KeyCache", "Hits"),
                    probe.getCacheMetric("KeyCache", "Requests"),
                    probe.getCacheMetric("KeyCache", "HitRate"),
                    cacheService.getKeyCacheSavePeriodInSeconds());

        // Row Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        outs.printf("%-17s: entries %d, size %d (bytes), capacity %d (bytes), %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                    "Row Cache",
                    probe.getCacheMetric("RowCache", "Entries"),
                    probe.getCacheMetric("RowCache", "Size"),
                    probe.getCacheMetric("RowCache", "Capacity"),
                    probe.getCacheMetric("RowCache", "Hits"),
                    probe.getCacheMetric("RowCache", "Requests"),
                    probe.getCacheMetric("RowCache", "HitRate"),
                    cacheService.getRowCacheSavePeriodInSeconds());

        if (toks.size() > 1 && cmd.hasOption(TOKENS_OPT.left))
        {
            for (String tok : toks)
                outs.printf("%-17s: %s%n", "Token", tok);
        }
    }

    public void printReleaseVersion(PrintStream outs)
    {
        outs.println("ReleaseVersion: " + probe.getReleaseVersion());
    }

    public void printNetworkStats(final InetAddress addr, PrintStream outs)
    {
        outs.printf("Mode: %s%n", probe.getOperationMode());
        Set<StreamState> statuses = probe.getStreamStatus();
        if (statuses.isEmpty())
            outs.println("Not sending any streams.");
        for (StreamState status : statuses)
        {
            outs.printf("%s %s%n", status.description, status.planId.toString());
            for (SessionInfo info : status.sessions)
            {
                outs.printf("    %s%n", info.peer.toString());
                if (!info.receivingSummaries.isEmpty())
                {
                    outs.printf("        Receiving %d files, %d bytes total%n", info.getTotalFilesToReceive(), info.getTotalSizeToReceive());
                    for (ProgressInfo progress : info.getReceivingFiles())
                    {
                        outs.printf("            %s%n", progress.toString());
                    }
                }
                if (!info.sendingSummaries.isEmpty())
                {
                    outs.printf("        Sending %d files, %d bytes total%n", info.getTotalFilesToSend(), info.getTotalSizeToSend());
                    for (ProgressInfo progress : info.getSendingFiles())
                    {
                        outs.printf("            %s%n", progress.toString());
                    }
                }
            }
        }

        outs.printf("Read Repair Statistics:%nAttempted: %d%nMismatch (Blocking): %d%nMismatch (Background): %d%n", probe.getReadRepairAttempted(), probe.getReadRepairRepairedBlocking(), probe.getReadRepairRepairedBackground());

        MessagingServiceMBean ms = probe.msProxy;
        outs.printf("%-25s", "Pool Name");
        outs.printf("%10s", "Active");
        outs.printf("%10s", "Pending");
        outs.printf("%15s%n", "Completed");

        int pending;
        long completed;

        pending = 0;
        for (int n : ms.getCommandPendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getCommandCompletedTasks().values())
            completed += n;
        outs.printf("%-25s%10s%10s%15s%n", "Commands", "n/a", pending, completed);

        pending = 0;
        for (int n : ms.getResponsePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getResponseCompletedTasks().values())
            completed += n;
        outs.printf("%-25s%10s%10s%15s%n", "Responses", "n/a", pending, completed);
    }

    public void printCompactionStats(PrintStream outs)
    {
        int compactionThroughput = probe.getCompactionThroughput();
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        outs.println("pending tasks: " + probe.getCompactionMetric("PendingTasks"));
        if (cm.getCompactions().size() > 0)
            outs.printf("%25s%16s%16s%16s%16s%10s%10s%n", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");
        long remainingBytes = 0;
        for (Map<String, String> c : cm.getCompactions())
        {
            String percentComplete = new Long(c.get("total")) == 0
                                   ? "n/a"
                                   : new DecimalFormat("0.00").format((double) new Long(c.get("completed")) / new Long(c.get("total")) * 100) + "%";
            outs.printf("%25s%16s%16s%16s%16s%10s%10s%n", c.get("taskType"), c.get("keyspace"), c.get("columnfamily"), c.get("completed"), c.get("total"), c.get("unit"), percentComplete);
            if (c.get("taskType").equals(OperationType.COMPACTION.toString()))
                remainingBytes += (new Long(c.get("total")) - new Long(c.get("completed")));
        }
        long remainingTimeInSecs = compactionThroughput == 0 || remainingBytes == 0
                        ? -1
                        : (remainingBytes) / (long) (1024L * 1024L * compactionThroughput);
        String remainingTime = remainingTimeInSecs < 0
                        ? "n/a"
                        : String.format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));

        outs.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
    }

    /**
     * Print the compaction threshold
     *
     * @param outs the stream to write to
     */
    public void printCompactionThreshold(PrintStream outs, String ks, String cf)
    {
        ColumnFamilyStoreMBean cfsProxy = probe.getCfsProxy(ks, cf);
        outs.println("Current compaction thresholds for " + ks + "/" + cf + ": \n" +
                     " min = " + cfsProxy.getMinimumCompactionThreshold() + ", " +
                     " max = " + cfsProxy.getMaximumCompactionThreshold());
    }

    /**
     * Print the compaction throughput
     *
     * @param outs the stream to write to
     */
    public void printCompactionThroughput(PrintStream outs)
    {
        outs.println("Current compaction throughput: " + probe.getCompactionThroughput() + " MB/s");
    }

    /**
     * Print the stream throughput
     *
     * @param outs the stream to write to
     */
    public void printStreamThroughput(PrintStream outs)
    {
        outs.println("Current stream throughput: " + probe.getStreamThroughput() + " MB/s");
    }

    /**
     * Print the name, snitch, partitioner and schema version(s) of a cluster
     *
     * @param outs Output stream
     * @param host Server address
     */
    public void printClusterDescription(PrintStream outs, String host)
    {
        // display cluster name, snitch and partitioner
        outs.println("Cluster Information:");
        outs.println("\tName: " + probe.getClusterName());
        outs.println("\tSnitch: " + probe.getEndpointSnitchInfoProxy().getSnitchName());
        outs.println("\tPartitioner: " + probe.getPartitioner());

        // display schema version for each node
        outs.println("\tSchema versions:");
        Map<String, List<String>> schemaVersions = probe.getSpProxy().getSchemaVersions();
        for (String version : schemaVersions.keySet())
        {
            outs.println(String.format("\t\t%s: %s%n", version, schemaVersions.get(version)));
        }
    }

    public void printColumnFamilyStats(PrintStream outs, boolean ignoreMode, String [] filterList)
    {
        OptionFilter filter = new OptionFilter(ignoreMode, filterList);
        Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String keyspaceName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(keyspaceName) && filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
            {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(keyspaceName, columnFamilies);
            }
            else if (filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
            {
                cfstoreMap.get(keyspaceName).add(cfsProxy);
            }
        }

        // make sure all specified kss and cfs exist
        filter.verifyKeyspaces(probe.getKeyspaces());
        filter.verifyColumnFamilies();

        // print out the table statistics
        for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
        {
            String keyspaceName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long keyspaceReadCount = 0;
            long keyspaceWriteCount = 0;
            int keyspacePendingTasks = 0;
            double keyspaceTotalReadTime = 0.0f;
            double keyspaceTotalWriteTime = 0.0f;

            outs.println("Keyspace: " + keyspaceName);
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                String cfName = cfstore.getColumnFamilyName();
                long writeCount = ((JmxReporter.TimerMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getCount();
                long readCount = ((JmxReporter.TimerMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getCount();

                if (readCount > 0)
                {
                    keyspaceReadCount += readCount;
                    keyspaceTotalReadTime += (long)probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadTotalLatency");
                }
                if (writeCount > 0)
                {
                    keyspaceWriteCount += writeCount;
                    keyspaceTotalWriteTime += (long)probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteTotalLatency");
                }
                keyspacePendingTasks += (int)probe.getColumnFamilyMetric(keyspaceName, cfName, "PendingTasks");
            }

            double keyspaceReadLatency = keyspaceReadCount > 0 ? keyspaceTotalReadTime / keyspaceReadCount / 1000 : Double.NaN;
            double keyspaceWriteLatency = keyspaceWriteCount > 0 ? keyspaceTotalWriteTime / keyspaceWriteCount / 1000 : Double.NaN;

            outs.println("\tRead Count: " + keyspaceReadCount);
            outs.println("\tRead Latency: " + String.format("%s", keyspaceReadLatency) + " ms.");
            outs.println("\tWrite Count: " + keyspaceWriteCount);
            outs.println("\tWrite Latency: " + String.format("%s", keyspaceWriteLatency) + " ms.");
            outs.println("\tPending Tasks: " + keyspacePendingTasks);

            // print out column family statistics for this keyspace
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                String cfName = cfstore.getColumnFamilyName();
                if(cfName.contains("."))
                    outs.println("\t\tTable (index): " + cfName);
                else
                    outs.println("\t\tTable: " + cfName);

                outs.println("\t\tSSTable count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveSSTableCount"));

                int[] leveledSStables = cfstore.getSSTableCountPerLevel();
                if (leveledSStables != null)
                {
                    outs.print("\t\tSSTables in each level: [");
                    for (int level = 0; level < leveledSStables.length; level++)
                    {
                        int count = leveledSStables[level];
                        outs.print(count);
                        long maxCount = 4L; // for L0
                        if (level > 0)
                            maxCount = (long) Math.pow(10, level);
                        //  show max threshold for level when exceeded
                        if (count > maxCount)
                            outs.print("/" + maxCount);

                        if (level < leveledSStables.length - 1)
                            outs.print(", ");
                        else
                            outs.println("]");
                    }
                }
                outs.println("\t\tSpace used (live), bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveDiskSpaceUsed"));
                outs.println("\t\tSpace used (total), bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "TotalDiskSpaceUsed"));
                outs.println("\t\tSpace used by snapshots (total), bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "SnapshotsSize"));
                outs.println("\t\tSSTable Compression Ratio: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionRatio"));
                outs.println("\t\tMemtable cell count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableColumnsCount"));
                outs.println("\t\tMemtable data size, bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableDataSize"));
                outs.println("\t\tMemtable switch count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableSwitchCount"));
                outs.println("\t\tLocal read count: " + ((JmxReporter.TimerMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getCount());
                double localReadLatency = ((JmxReporter.TimerMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getMean() / 1000;
                double localRLatency = localReadLatency > 0 ? localReadLatency : Double.NaN;
                outs.printf("\t\tLocal read latency: %01.3f ms%n", localRLatency);
                outs.println("\t\tLocal write count: " + ((JmxReporter.TimerMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getCount());
                double localWriteLatency = ((JmxReporter.TimerMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getMean() / 1000;
                double localWLatency = localWriteLatency > 0 ? localWriteLatency : Double.NaN;
                outs.printf("\t\tLocal write latency: %01.3f ms%n", localWLatency);
                outs.println("\t\tPending tasks: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "PendingTasks"));
                outs.println("\t\tBloom filter false positives: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterFalsePositives"));
                outs.println("\t\tBloom filter false ratio: " + String.format("%01.5f", probe.getColumnFamilyMetric(keyspaceName, cfName, "RecentBloomFilterFalseRatio")));
                outs.println("\t\tBloom filter space used, bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterDiskSpaceUsed"));
                outs.println("\t\tCompacted partition minimum bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MinRowSize"));
                outs.println("\t\tCompacted partition maximum bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MaxRowSize"));
                outs.println("\t\tCompacted partition mean bytes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MeanRowSize"));
                outs.println("\t\tAverage live cells per slice (last five minutes): " + ((JmxReporter.HistogramMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveScannedHistogram")).getMean());
                outs.println("\t\tAverage tombstones per slice (last five minutes): " + ((JmxReporter.HistogramMBean)probe.getColumnFamilyMetric(keyspaceName, cfName, "TombstoneScannedHistogram")).getMean());

                outs.println("");
            }
            outs.println("----------------");
        }
    }

    public void printRemovalStatus(PrintStream outs)
    {
        outs.println("RemovalStatus: " + probe.getRemovalStatus());
    }

    private void printCfHistograms(String keySpace, String columnFamily, PrintStream output)
    {
        // calculate percentile of row size and column count
        long[] estimatedRowSize = (long[]) probe.getColumnFamilyMetric(keySpace, columnFamily, "EstimatedRowSizeHistogram");
        long[] estimatedColumnCount = (long[]) probe.getColumnFamilyMetric(keySpace, columnFamily, "EstimatedColumnCountHistogram");

        long[] bucketOffsets = new EstimatedHistogram().getBucketOffsets();
        EstimatedHistogram rowSizeHist = new EstimatedHistogram(bucketOffsets, estimatedRowSize);
        EstimatedHistogram columnCountHist = new EstimatedHistogram(bucketOffsets, estimatedColumnCount);

        // build arrays to store percentile values
        double[] estimatedRowSizePercentiles = new double[7];
        double[] estimatedColumnCountPercentiles = new double[7];
        double[] offsetPercentiles = new double[]{0.5, 0.75, 0.95, 0.98, 0.99};
        for (int i = 0; i < offsetPercentiles.length; i++)
        {
            estimatedRowSizePercentiles[i] = rowSizeHist.percentile(offsetPercentiles[i]);
            estimatedColumnCountPercentiles[i] = columnCountHist.percentile(offsetPercentiles[i]);
        }

        // min value
        estimatedRowSizePercentiles[5] = rowSizeHist.min();
        estimatedColumnCountPercentiles[5] = columnCountHist.min();
        // max value
        estimatedRowSizePercentiles[6] = rowSizeHist.max();
        estimatedColumnCountPercentiles[6] = columnCountHist.max();

        String[] percentiles = new String[]{ "50%", "75%", "95%", "98%", "99%", "Min", "Max" };
        double[] readLatency = probe.metricPercentilesAsArray((JmxReporter.HistogramMBean)probe.getColumnFamilyMetric(keySpace, columnFamily, "ReadLatency"));
        double[] writeLatency = probe.metricPercentilesAsArray((JmxReporter.TimerMBean)probe.getColumnFamilyMetric(keySpace, columnFamily, "WriteLatency"));
        double[] sstablesPerRead = probe.metricPercentilesAsArray((JmxReporter.HistogramMBean)probe.getColumnFamilyMetric(keySpace, columnFamily, "SSTablesPerReadHistogram"));

        output.println(String.format("%s/%s histograms", keySpace, columnFamily));
        output.println(String.format("%-10s%10s%18s%18s%18s%18s",
                                     "Percentile", "SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count"));
        output.println(String.format("%-10s%10s%18s%18s%18s%18s",
                                     "", "", "(micros)", "(micros)", "(bytes)", ""));

        for (int i = 0; i < percentiles.length; i++)
        {
            output.println(String.format("%-10s%10.2f%18.2f%18.2f%18.0f%18.0f",
                                         percentiles[i],
                                         sstablesPerRead[i],
                                         writeLatency[i],
                                         readLatency[i],
                                         estimatedRowSizePercentiles[i],
                                         estimatedColumnCountPercentiles[i]));
        }
        output.println();
    }

    private void printProxyHistograms(PrintStream output)
    {
        String[] percentiles = new String[]{ "50%", "75%", "95%", "98%", "99%", "Min", "Max" };
        double[] readLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Read"));
        double[] writeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Write"));
        double[] rangeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("RangeSlice"));

        output.println("proxy histograms");
        output.println(String.format("%-10s%18s%18s%18s",
                                     "Percentile", "Read Latency", "Write Latency", "Range Latency"));
        output.println(String.format("%-10s%18s%18s%18s",
                                     "", "(micros)", "(micros)", "(micros)"));
        for (int i = 0; i < percentiles.length; i++)
        {
            output.println(String.format("%-10s%18.2f%18.2f%18.2f",
                                         percentiles[i],
                                         readLatency[i],
                                         writeLatency[i],
                                         rangeLatency[i]));
        }
        output.println();
    }

    private void printEndPoints(String keySpace, String cf, String key, PrintStream output)
    {
        List<InetAddress> endpoints = this.probe.getEndpoints(keySpace, cf, key);

        for (InetAddress anEndpoint : endpoints)
        {
           output.println(anEndpoint.getHostAddress());
        }
    }

    private void printSSTables(String keyspace, String cf, String key, PrintStream output)
    {
        List<String> sstables = this.probe.getSSTables(keyspace, cf, key);
        for (String sstable : sstables)
        {
            output.println(sstable);
        }
    }

    private void printIsNativeTransportRunning(PrintStream outs)
    {
        outs.println(probe.isNativeTransportRunning() ? "running" : "not running");
    }

    private void printIsThriftServerRunning(PrintStream outs)
    {
        outs.println(probe.isThriftServerRunning() ? "running" : "not running");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ParseException
    {
        CommandLineParser parser = new PosixParser();
        ToolCommandLine cmd = null;

        try
        {
            cmd = new ToolCommandLine(parser.parse(options, args));
        }
        catch (ParseException p)
        {
            badUse(p.getMessage());
        }

        String host = cmd.hasOption(HOST_OPT.left) ? cmd.getOptionValue(HOST_OPT.left) : DEFAULT_HOST;

        int port = DEFAULT_PORT;

        String portNum = cmd.getOptionValue(PORT_OPT.left);
        if (portNum != null)
        {
            try
            {
                port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }

        NodeCommand command = null;

        try
        {
            command = cmd.getCommand();
        }
        catch (IllegalArgumentException e)
        {
            badUse(e.getMessage());
        }

        if(NodeCommand.HELP.equals(command))
        {
            printUsage();
            System.exit(0);
        }

        NodeProbe probe = null;

        try
        {
            String username = cmd.getOptionValue(USERNAME_OPT.left);
            String password = cmd.getOptionValue(PASSWORD_OPT.left);

            try
            {
                probe = username == null ? new NodeProbe(host, port) : new NodeProbe(host, port, username, password);
            }
            catch (IOException ioe)
            {
                Throwable inner = findInnermostThrowable(ioe);
                if (inner instanceof ConnectException)
                {
                    System.err.printf("Failed to connect to '%s:%d': %s%n", host, port, inner.getMessage());
                    System.exit(1);
                }
                else if (inner instanceof UnknownHostException)
                {
                    System.err.printf("Cannot resolve '%s': unknown host%n", host);
                    System.exit(1);
                }
                else
                {
                    err(ioe, "Error connecting to remote JMX agent!");
                }
            }

            NodeCmd nodeCmd = new NodeCmd(probe);

            //print history here after we've already determined we can reasonably call cassandra
            printHistory(args, cmd);

            // Execute the requested command.
            String[] arguments = cmd.getCommandArguments();
            String tag;
            String columnFamilyName = null;

            switch (command)
            {
                case RING :
                    if (arguments.length > 0) { nodeCmd.printRing(System.out, arguments[0]); }
                    else                      { nodeCmd.printRing(System.out, null); };
                    break;

                case INFO            : nodeCmd.printInfo(System.out, cmd); break;
                case CFSTATS         :
                    boolean ignoreMode = cmd.hasOption(CFSTATS_IGNORE_OPT.left);
                    if (arguments.length > 0) { nodeCmd.printColumnFamilyStats(System.out, ignoreMode, arguments); }
                    else                      { nodeCmd.printColumnFamilyStats(System.out, false, null); }
                    break;
                case TPSTATS         : nodeCmd.printThreadPoolStats(System.out); break;
                case VERSION         : nodeCmd.printReleaseVersion(System.out); break;
                case COMPACTIONSTATS : nodeCmd.printCompactionStats(System.out); break;
                case COMPACTIONHISTORY:nodeCmd.printCompactionHistory(System.out); break;
                case DESCRIBECLUSTER : nodeCmd.printClusterDescription(System.out, host); break;
                case DISABLEBINARY   : probe.stopNativeTransport(); break;
                case ENABLEBINARY    : probe.startNativeTransport(); break;
                case STATUSBINARY    : nodeCmd.printIsNativeTransportRunning(System.out); break;
                case DISABLEGOSSIP   : probe.stopGossiping(); break;
                case ENABLEGOSSIP    : probe.startGossiping(); break;
                case DISABLEHANDOFF  : probe.disableHintedHandoff(); break;
                case ENABLEHANDOFF   : probe.enableHintedHandoff(); break;
                case PAUSEHANDOFF    : probe.pauseHintsDelivery(); break;
                case RESUMEHANDOFF   : probe.resumeHintsDelivery(); break;
                case DISABLETHRIFT   : probe.stopThriftServer(); break;
                case ENABLETHRIFT    : probe.startThriftServer(); break;
                case STATUSTHRIFT    : nodeCmd.printIsThriftServerRunning(System.out); break;
                case RESETLOCALSCHEMA: probe.resetLocalSchema(); break;
                case ENABLEBACKUP    : probe.setIncrementalBackupsEnabled(true); break;
                case DISABLEBACKUP   : probe.setIncrementalBackupsEnabled(false); break;
                    

                case STATUS :
                    boolean resolveIp = cmd.hasOption(RESOLVE_IP.left);
                    if (arguments.length > 0) nodeCmd.printClusterStatus(System.out, arguments[0], resolveIp);
                    else                      nodeCmd.printClusterStatus(System.out, null, resolveIp);
                    break;

                case DECOMMISSION :
                    if (arguments.length > 0)
                    {
                        System.err.println("Decommission will decommission the node you are connected to and does not take arguments!");
                        System.exit(1);
                    }
                    probe.decommission();
                    break;

                case DRAIN :
                    try { probe.drain(); }
                    catch (ExecutionException ee) { err(ee, "Error occured during flushing"); }
                    break;

                case NETSTATS :
                    if (arguments.length > 0) { nodeCmd.printNetworkStats(InetAddress.getByName(arguments[0]), System.out); }
                    else                      { nodeCmd.printNetworkStats(null, System.out); }
                    break;

                case SNAPSHOT :
                    columnFamilyName = cmd.getOptionValue(SNAPSHOT_COLUMNFAMILY_OPT.left);
                    /* FALL THRU */
                case CLEARSNAPSHOT :
                    tag = cmd.getOptionValue(TAG_OPT.left);
                    handleSnapshots(command, tag, arguments, columnFamilyName, probe);
                    break;

                case MOVE :
                    if (arguments.length != 1) { badUse("Missing token argument for move."); }
                    try
                    {
                        probe.move(arguments[0]);
                    }
                    catch (UnsupportedOperationException uoerror)
                    {
                        System.err.println(uoerror.getMessage());
                        System.exit(1);
                    }
                    break;

                case JOIN:
                    if (probe.isJoined())
                    {
                        System.err.println("This node has already joined the ring.");
                        System.exit(1);
                    }

                    probe.joinRing();
                    break;

                case SETCOMPACTIONTHROUGHPUT :
                    if (arguments.length != 1) { badUse("Missing value argument."); }
                    probe.setCompactionThroughput(Integer.parseInt(arguments[0]));
                    break;

                case SETSTREAMTHROUGHPUT :
                    if (arguments.length != 1) { badUse("Missing value argument."); }
                    probe.setStreamThroughput(Integer.parseInt(arguments[0]));
                    break;

                case SETTRACEPROBABILITY :
                    if (arguments.length != 1) { badUse("Missing value argument."); }
                    probe.setTraceProbability(Double.parseDouble(arguments[0]));
                    break;

                case REBUILD :
                    if (arguments.length > 1) { badUse("Too many arguments."); }
                    probe.rebuild(arguments.length == 1 ? arguments[0] : null);
                    break;

                case REMOVETOKEN :
                    System.err.println("Warn: removetoken is deprecated, please use removenode instead");
                case REMOVENODE  :
                    if (arguments.length != 1) { badUse("Missing an argument for removenode (either status, force, or an ID)"); }
                    else if (arguments[0].equals("status")) { nodeCmd.printRemovalStatus(System.out); }
                    else if (arguments[0].equals("force"))  { nodeCmd.printRemovalStatus(System.out); probe.forceRemoveCompletion(); }
                    else                                    { probe.removeNode(arguments[0]); }
                    break;

                case INVALIDATEKEYCACHE :
                    probe.invalidateKeyCache();
                    break;

                case INVALIDATEROWCACHE :
                    probe.invalidateRowCache();
                    break;

                case CLEANUP :
                case COMPACT :
                case REPAIR  :
                case FLUSH   :
                case SCRUB   :
                case UPGRADESSTABLES   :
                case DISABLEAUTOCOMPACTION:
                case ENABLEAUTOCOMPACTION:
                    optionalKSandCFs(command, cmd, arguments, probe);
                    break;

                case GETCOMPACTIONTHRESHOLD :
                    if (arguments.length != 2) { badUse("getcompactionthreshold requires ks and cf args."); }
                    nodeCmd.printCompactionThreshold(System.out, arguments[0], arguments[1]);
                    break;

                case GETCOMPACTIONTHROUGHPUT : nodeCmd.printCompactionThroughput(System.out); break;
                case GETSTREAMTHROUGHPUT : nodeCmd.printStreamThroughput(System.out); break;

                case CFHISTOGRAMS :
                    if (arguments.length != 2) { badUse("cfhistograms requires ks and cf args"); }
                    nodeCmd.printCfHistograms(arguments[0], arguments[1], System.out);
                    break;

                case SETCACHECAPACITY :
                    if (arguments.length != 2) { badUse("setcachecapacity requires key-cache-capacity, and row-cache-capacity args."); }
                    probe.setCacheCapacities(Integer.parseInt(arguments[0]), Integer.parseInt(arguments[1]));
                    break;

                case SETCACHEKEYSTOSAVE :
                    if (arguments.length != 2) { badUse("setcachekeystosave requires key-cache-keys-to-save, and row-cache-keys-to-save args."); }
                    probe.setCacheKeysToSave(Integer.parseInt(arguments[0]), Integer.parseInt(arguments[1]));
                    break;

                case SETCOMPACTIONTHRESHOLD :
                    if (arguments.length != 4) { badUse("setcompactionthreshold requires ks, cf, min, and max threshold args."); }
                    int minthreshold = Integer.parseInt(arguments[2]);
                    int maxthreshold = Integer.parseInt(arguments[3]);
                    if ((minthreshold < 0) || (maxthreshold < 0)) { badUse("Thresholds must be positive integers"); }
                    if (minthreshold > maxthreshold)              { badUse("Min threshold cannot be greater than max."); }
                    if (minthreshold < 2 && maxthreshold != 0)    { badUse("Min threshold must be at least 2"); }
                    probe.setCompactionThreshold(arguments[0], arguments[1], minthreshold, maxthreshold);
                    break;
                case GETENDPOINTS :
                    if (arguments.length != 3) { badUse("getendpoints requires ks, cf and key args"); }
                    nodeCmd.printEndPoints(arguments[0], arguments[1], arguments[2], System.out);
                    break;

                case PROXYHISTOGRAMS :
                    if (arguments.length != 0) { badUse("proxyhistograms does not take arguments"); }
                    nodeCmd.printProxyHistograms(System.out);
                    break;

                case GETSSTABLES:
                    if (arguments.length != 3) { badUse("getsstables requires ks, cf and key args"); }
                    nodeCmd.printSSTables(arguments[0], arguments[1], arguments[2], System.out);
                    break;

                case REFRESH:
                    if (arguments.length != 2) { badUse("load_new_sstables requires ks and cf args"); }
                    probe.loadNewSSTables(arguments[0], arguments[1]);
                    break;

                case REBUILD_INDEX:
                    if (arguments.length < 2) { badUse("rebuild_index requires ks and cf args"); }
                    if (arguments.length >= 3)
                        probe.rebuildIndex(arguments[0], arguments[1], arguments[2].split(","));
                    else
                        probe.rebuildIndex(arguments[0], arguments[1]);

                    break;

                case GOSSIPINFO : nodeCmd.printGossipInfo(System.out); break;

                case STOP:
                    if (arguments.length != 1) { badUse("stop requires a type."); }
                    probe.stop(arguments[0].toUpperCase());
                    break;

                case DESCRIBERING :
                    if (arguments.length != 1) { badUse("Missing keyspace argument for describering."); }
                    nodeCmd.printDescribeRing(arguments[0], System.out);
                    break;

                case RANGEKEYSAMPLE :
                    nodeCmd.printRangeKeySample(System.out);
                    break;

                case RELOADTRIGGERS :
                    probe.reloadTriggers();
                    break;

                default :
                    throw new RuntimeException("Unreachable code.");
            }
        }
        finally
        {
            if (probe != null)
            {
                try
                {
                    probe.close();
                }
                catch (IOException ex)
                {
                    // swallow the exception so the user will see the real one.
                }
            }
        }
        System.exit(probe.isFailed() ? 1 : 0);
    }

    private void printCompactionHistory(PrintStream out)
    {
        out.println("Compaction History: ");

        TabularData tabularData = this.probe.getCompactionHistory();
        if (tabularData.isEmpty())
        {
            out.printf("There is no compaction history");
            return;
        }

        String format = "%-41s%-19s%-29s%-26s%-15s%-15s%s%n";
        List<String> indexNames = tabularData.getTabularType().getIndexNames();
        out.printf(format, (Object[]) indexNames.toArray(new String[indexNames.size()]));

        Set<?> values = tabularData.keySet();
        for (Object eachValue : values)
        {
            List<?> value = (List<?>) eachValue;
            out.printf(format, value.toArray(new Object[value.size()]));
        }
    }

    private static void printHistory(String[] args, ToolCommandLine cmd)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;
        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        final String password = cmd.getOptionValue(PASSWORD_OPT.left);
        if (password != null)
            cmdLine = cmdLine.replace(password, "<hidden>");

        try (FileWriter writer = new FileWriter(new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE), true))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date()) + ": " + cmdLine + "\n");
        }
        catch (IOException ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    private static Throwable findInnermostThrowable(Throwable ex)
    {
        Throwable inner = ex.getCause();
        return inner == null ? ex : findInnermostThrowable(inner);
    }

    private void printDescribeRing(String keyspaceName, PrintStream out)
    {
        out.println("Schema Version:" + probe.getSchemaVersion());
        out.println("TokenRange: ");
        try
        {
            for (String tokenRangeString : probe.describeRing(keyspaceName))
            {
                out.println("\t" + tokenRangeString);
            }
        }
        catch (IOException e)
        {
            err(e, e.getMessage());
        }
    }

    private void printRangeKeySample(PrintStream outs)
    {
        outs.println("RangeKeySample: ");
        List<String> tokenStrings = this.probe.sampleKeyRange();
        for (String tokenString : tokenStrings)
        {
            outs.println("\t" + tokenString);
        }
    }

    private void printGossipInfo(PrintStream out) {
        out.println(probe.getGossipInfo());
    }

    private static void badUse(String useStr)
    {
        System.err.println(useStr);
        printUsage();
        System.exit(1);
    }

    private static void err(Exception e, String errStr)
    {
        System.err.println(errStr);
        e.printStackTrace();
        System.exit(3);
    }

    private static void complainNonzeroArgs(String[] args, NodeCommand cmd)
    {
        if (args.length > 0) {
            System.err.println("Too many arguments for command '"+cmd.toString()+"'.");
            printUsage();
            System.exit(1);
        }
    }

    private static void handleSnapshots(NodeCommand nc, String tag, String[] cmdArgs, String columnFamily, NodeProbe probe) throws IOException
    {
        String[] keyspaces = Arrays.copyOfRange(cmdArgs, 0, cmdArgs.length);
        System.out.print("Requested " + ((nc == NodeCommand.SNAPSHOT) ? "creating" : "clearing") + " snapshot for: ");
        if ( keyspaces.length > 0 )
        {
          for (int i = 0; i < keyspaces.length; i++)
              System.out.print(keyspaces[i] + " ");
        }
        else
        {
            System.out.print("all keyspaces ");
        }

        if (columnFamily != null)
        {
            System.out.print("and table: " + columnFamily);
        }
        System.out.println();

        switch (nc)
        {
            case SNAPSHOT :
                if (tag == null || tag.equals(""))
                    tag = new Long(System.currentTimeMillis()).toString();
                probe.takeSnapshot(tag, columnFamily, keyspaces);
                System.out.println("Snapshot directory: " + tag);
                break;
            case CLEARSNAPSHOT :
                probe.clearSnapshot(tag, keyspaces);
                break;
        }
    }

    private static void optionalKSandCFs(NodeCommand nc, ToolCommandLine cmd, String[] cmdArgs, NodeProbe probe) throws InterruptedException, IOException
    {
        // if there is one additional arg, it's the keyspace; more are columnfamilies
        List<String> keyspaces = cmdArgs.length == 0 ? probe.getKeyspaces() : Arrays.asList(cmdArgs[0]);
        for (String keyspace : keyspaces)
        {
            if (!probe.getKeyspaces().contains(keyspace))
            {
                System.err.println("Keyspace [" + keyspace + "] does not exist.");
                System.exit(1);
            }
        }

        // second loop so we're less likely to die halfway through due to invalid keyspace
        for (String keyspace : keyspaces)
        {
            String[] columnFamilies = cmdArgs.length <= 1 ? new String[0] : Arrays.copyOfRange(cmdArgs, 1, cmdArgs.length);
            switch (nc)
            {
                case REPAIR  :
                    boolean sequential = !cmd.hasOption(PARALLEL_REPAIR_OPT.left);
                    boolean localDC = cmd.hasOption(LOCAL_DC_REPAIR_OPT.left);
                    boolean specificDC = cmd.hasOption(DC_REPAIR_OPT.left);
                    boolean primaryRange = cmd.hasOption(PRIMARY_RANGE_OPT.left);
                    Collection<String> dataCenters = null;
                    if (specificDC)
                        dataCenters = Arrays.asList(cmd.getOptionValue(DC_REPAIR_OPT.left).split(","));
                    else if (localDC)
                        dataCenters = Arrays.asList(probe.getDataCenter());
                    if (cmd.hasOption(START_TOKEN_OPT.left) || cmd.hasOption(END_TOKEN_OPT.left))
                        probe.forceRepairRangeAsync(System.out, keyspace, sequential, dataCenters, cmd.getOptionValue(START_TOKEN_OPT.left), cmd.getOptionValue(END_TOKEN_OPT.left), columnFamilies);
                    else
                        probe.forceRepairAsync(System.out, keyspace, sequential, dataCenters, primaryRange, columnFamilies);
                    break;
                case FLUSH   :
                    try { probe.forceKeyspaceFlush(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred during flushing"); }
                    break;
                case COMPACT :
                    try { probe.forceKeyspaceCompaction(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred during compaction"); }
                    break;
                case CLEANUP :
                    if (keyspace.equals(Keyspace.SYSTEM_KS)) { break; } // Skip cleanup on system cfs.
                    try { probe.forceKeyspaceCleanup(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred during cleanup"); }
                    break;
                case SCRUB :
                    boolean disableSnapshot = cmd.hasOption(NO_SNAPSHOT.left);
                    try { probe.scrub(disableSnapshot, keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred while scrubbing keyspace " + keyspace); }
                    break;
                case UPGRADESSTABLES :
                    boolean excludeCurrentVersion = !cmd.hasOption(UPGRADE_ALL_SSTABLE_OPT.left);
                    try { probe.upgradeSSTables(keyspace, excludeCurrentVersion, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occurred while upgrading the sstables for keyspace " + keyspace); }
                    break;
                case ENABLEAUTOCOMPACTION:
                    probe.enableAutoCompaction(keyspace, columnFamilies);
                    break;
                case DISABLEAUTOCOMPACTION:
                    probe.disableAutoCompaction(keyspace, columnFamilies);
                    break;
                default:
                    throw new RuntimeException("Unreachable code.");
            }
        }
    }

    /**
     * Used for filtering keyspaces and columnfamilies to be displayed using the cfstats command.
     */
    private static class OptionFilter
    {
        private Map<String, List<String>> filter = new HashMap<String, List<String>>();
        private Map<String, List<String>> verifier = new HashMap<String, List<String>>();
        private String [] filterList;
        private boolean ignoreMode;

        public OptionFilter(boolean ignoreMode, String... filterList)
        {
            this.filterList = filterList;
            this.ignoreMode = ignoreMode;

            if(filterList == null)
                return;

            for(String s : filterList)
            {
                String [] keyValues = s.split("\\.", 2);

                // build the map that stores the ks' and cfs to use
                if(!filter.containsKey(keyValues[0]))
                {
                    filter.put(keyValues[0], new ArrayList<String>());
                    verifier.put(keyValues[0], new ArrayList<String>());

                    if(keyValues.length == 2)
                    {
                        filter.get(keyValues[0]).add(keyValues[1]);
                        verifier.get(keyValues[0]).add(keyValues[1]);
                    }
                }
                else
                {
                    if(keyValues.length == 2)
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
            if(filterList == null)
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
            for(String ks : verifier.keySet())
                if(!keyspaces.contains(ks))
                    throw new RuntimeException("Unknown keyspace: " + ks);
        }

        public void verifyColumnFamilies()
        {
            for(String ks : filter.keySet())
                if(verifier.get(ks).size() > 0)
                    throw new RuntimeException("Unknown column families: " + verifier.get(ks).toString() + " in keyspace: " + ks);
        }
    }

    private static class ToolOptions extends Options
    {
        public void addOption(Pair<String, String> opts, boolean hasArgument, String description)
        {
            addOption(opts, hasArgument, description, false);
        }

        public void addOption(Pair<String, String> opts, boolean hasArgument, String description, boolean required)
        {
            addOption(opts.left, opts.right, hasArgument, description, required);
        }

        public void addOption(String opt, String longOpt, boolean hasArgument, String description, boolean required)
        {
            Option option = new Option(opt, longOpt, hasArgument, description);
            option.setRequired(required);
            addOption(option);
        }
    }

    private static class ToolCommandLine
    {
        private final CommandLine commandLine;

        public ToolCommandLine(CommandLine commands)
        {
            commandLine = commands;
        }

        public Option[] getOptions()
        {
            return commandLine.getOptions();
        }

        public boolean hasOption(String opt)
        {
            return commandLine.hasOption(opt);
        }

        public String getOptionValue(String opt)
        {
            return commandLine.getOptionValue(opt);
        }

        public NodeCommand getCommand()
        {
            if (commandLine.getArgs().length == 0)
                throw new IllegalArgumentException("Command was not specified.");

            String command = commandLine.getArgs()[0];

            try
            {
                return NodeCommand.valueOf(command.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException("Unrecognized command: " + command);
            }
        }

        public String[] getCommandArguments()
        {
            List params = commandLine.getArgList();

            if (params.size() < 2) // command parameters are empty
                return new String[0];

            String[] toReturn = new String[params.size() - 1];

            for (int i = 1; i < params.size(); i++)
            {
                String parm = (String) params.get(i);
                // why? look at CASSANDRA-4808
                if (parm.startsWith("\\"))
                    parm = parm.substring(1);
                toReturn[i - 1] = parm;
            }
            return toReturn;
        }
    }
}
