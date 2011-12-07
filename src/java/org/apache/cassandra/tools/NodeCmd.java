package org.apache.cassandra.tools;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.*;

import org.apache.cassandra.cache.InstrumentingCacheMBean;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Pair;

public class NodeCmd
{
    private static final Pair<String, String> HOST_OPT = new Pair<String, String>("h", "host");
    private static final Pair<String, String> PORT_OPT = new Pair<String, String>("p", "port");
    private static final Pair<String, String> USERNAME_OPT = new Pair<String, String>("u",  "username");
    private static final Pair<String, String> PASSWORD_OPT = new Pair<String, String>("pw", "password");
    private static final Pair<String, String> TAG_OPT = new Pair<String, String>("t", "tag");
    private static final Pair<String, String> PRIMARY_RANGE_OPT = new Pair<String, String>("pr", "partitioner-range");
    private static final int DEFAULT_PORT = 7199;

    private static ToolOptions options = null;

    private NodeProbe probe;
    
    static
    {
        options = new ToolOptions();

        options.addOption(HOST_OPT,     true, "node hostname or ip address", true);
        options.addOption(PORT_OPT,     true, "remote jmx agent port number");
        options.addOption(USERNAME_OPT, true, "remote jmx agent username");
        options.addOption(PASSWORD_OPT, true, "remote jmx agent password");
        options.addOption(TAG_OPT,      true, "optional name to give a snapshot");
        options.addOption(PRIMARY_RANGE_OPT, false, "only repair the first range returned by the partitioner for the node");
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
        DECOMMISSION,
        DISABLEGOSSIP,
        DISABLETHRIFT,
        DRAIN,
        ENABLEGOSSIP,
        ENABLETHRIFT,
        FLUSH,
        GETCOMPACTIONTHRESHOLD,
        GETENDPOINTS,
        GOSSIPINFO,
        INFO,
        INVALIDATEKEYCACHE,
        INVALIDATEROWCACHE,
        JOIN,
        MOVE,
        NETSTATS,
        REFRESH,
        REMOVETOKEN,
        REPAIR,
        RING,
        SCRUB,
        SETCACHECAPACITY,
        SETCOMPACTIONTHRESHOLD,
        SETCOMPACTIONTHROUGHPUT,
        SNAPSHOT,
        STATUSTHRIFT,
        TPSTATS,
        UPGRADESSTABLES,
        VERSION,
        DESCRIBERING,
    }

    
    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        StringBuilder header = new StringBuilder();
        header.append("\nAvailable commands:\n");
        // No args
        addCmdHelp(header, "ring", "Print informations on the token ring");
        addCmdHelp(header, "join", "Join the ring");
        addCmdHelp(header, "info", "Print node informations (uptime, load, ...)");
        addCmdHelp(header, "cfstats", "Print statistics on column families");
        addCmdHelp(header, "version", "Print cassandra version");
        addCmdHelp(header, "tpstats", "Print usage statistics of thread pools");
        addCmdHelp(header, "drain", "Drain the node (stop accepting writes and flush all column families)");
        addCmdHelp(header, "decommission", "Decommission the node");
        addCmdHelp(header, "compactionstats", "Print statistics on compactions");
        addCmdHelp(header, "disablegossip", "Disable gossip (effectively marking the node dead)");
        addCmdHelp(header, "enablegossip", "Reenable gossip");
        addCmdHelp(header, "disablethrift", "Disable thrift server");
        addCmdHelp(header, "enablethrift", "Reenable thrift server");
        addCmdHelp(header, "statusthrift", "Status of thrift server");
        addCmdHelp(header, "gossipinfo", "Shows the gossip information for the cluster");

        // One arg
        addCmdHelp(header, "netstats [host]", "Print network information on provided host (connecting node by default)");
        addCmdHelp(header, "move <new token>", "Move node on the token ring to a new token");
        addCmdHelp(header, "removetoken status|force|<token>", "Show status of current token removal, force completion of pending removal or remove providen token");
        addCmdHelp(header, "setcompactionthroughput <value_in_mb>", "Set the MB/s throughput cap for compaction in the system, or 0 to disable throttling.");
        addCmdHelp(header, "describering [keyspace]", "Shows the token ranges info of a given keyspace.");

        // Two args
        addCmdHelp(header, "snapshot [keyspaces...] -t [snapshotName]", "Take a snapshot of the specified keyspaces using optional name snapshotName");
        addCmdHelp(header, "clearsnapshot [keyspaces...] -t [snapshotName]", "Remove snapshots for the specified keyspaces. Either remove all snapshots or remove the snapshots with the given name.");
        addCmdHelp(header, "flush [keyspace] [cfnames]", "Flush one or more column family");
        addCmdHelp(header, "repair [keyspace] [cfnames]", "Repair one or more column family (use -rp to repair only the first range returned by the partitioner)");
        addCmdHelp(header, "cleanup [keyspace] [cfnames]", "Run cleanup on one or more column family");
        addCmdHelp(header, "compact [keyspace] [cfnames]", "Force a (major) compaction on one or more column family");
        addCmdHelp(header, "scrub [keyspace] [cfnames]", "Scrub (rebuild sstables for) one or more column family");
        addCmdHelp(header, "upgradesstables [keyspace] [cfnames]", "Scrub (rebuild sstables for) one or more column family");
        addCmdHelp(header, "invalidatekeycache [keyspace] [cfnames]", "Invalidate the key cache of one or more column family");
        addCmdHelp(header, "invalidaterowcache [keyspace] [cfnames]", "Invalidate the key cache of one or more column family");
        addCmdHelp(header, "getcompactionthreshold <keyspace> <cfname>", "Print min and max compaction thresholds for a given column family");
        addCmdHelp(header, "cfhistograms <keyspace> <cfname>", "Print statistic histograms for a given column family");
        addCmdHelp(header, "refresh <keyspace> <cf-name>", "Load newly placed SSTables to the system without restart.");

        // Three args
        addCmdHelp(header, "getendpoints <keyspace> <cf> <key>", "Print the end points that owns the key");

        // Four args
        addCmdHelp(header, "setcachecapacity <keyspace> <cfname> <keycachecapacity> <rowcachecapacity>", "Set the key and row cache capacities of a given column family");
        addCmdHelp(header, "setcompactionthreshold <keyspace> <cfname> <minthreshold> <maxthreshold>", "Set the min and max compaction thresholds for a given column family");

        String usage = String.format("java %s --host <arg> <command>%n", NodeCmd.class.getName());
        hf.printHelp(usage, "", options, "");
        System.out.println(header.toString());
    }

    private static void addCmdHelp(StringBuilder sb, String cmd, String description)
    {
        sb.append("  ").append(cmd);
        // Ghetto indentation (trying, but not too hard, to not look too bad)
        if (cmd.length() <= 20)
            for (int i = cmd.length(); i < 22; ++i) sb.append(" ");
        sb.append(" - ").append(description).append("\n");
    }
    
    /**
     * Write a textual representation of the Cassandra ring.
     * 
     * @param outs the stream to write to
     */
    public void printRing(PrintStream outs)
    {
        Map<Token, String> tokenToEndpoint = probe.getTokenToEndpointMap();
        List<Token> sortedTokens = new ArrayList<Token>(tokenToEndpoint.keySet());
        Collections.sort(sortedTokens);

        Collection<String> liveNodes = probe.getLiveNodes();
        Collection<String> deadNodes = probe.getUnreachableNodes();
        Collection<String> joiningNodes = probe.getJoiningNodes();
        Collection<String> leavingNodes = probe.getLeavingNodes();
        Collection<String> movingNodes = probe.getMovingNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        String format = "%-16s%-12s%-12s%-7s%-8s%-16s%-8s%-44s%n";
        outs.printf(format, "Address", "DC", "Rack", "Status", "State", "Load", "Owns", "Token");
        // show pre-wrap token twice so you can always read a node's range as
        // (previous line token, current line token]
        if (sortedTokens.size() > 1)
            outs.printf(format, "", "", "", "", "", "", "", sortedTokens.get(sortedTokens.size() - 1));

        // Calculate per-token ownership of the ring
        Map<Token, Float> ownerships = probe.getOwnership();

        for (Token token : sortedTokens)
        {
            String primaryEndpoint = tokenToEndpoint.get(token);
            String dataCenter;
            try
            {
                dataCenter = probe.getEndpointSnitchInfoProxy().getDatacenter(primaryEndpoint);
            }
            catch (UnknownHostException e)
            {
                dataCenter = "Unknown";
            }
            String rack;
            try
            {
                rack = probe.getEndpointSnitchInfoProxy().getRack(primaryEndpoint);
            }
            catch (UnknownHostException e)
            {
                rack = "Unknown";
            }
            String status = liveNodes.contains(primaryEndpoint)
                            ? "Up"
                            : deadNodes.contains(primaryEndpoint)
                              ? "Down"
                              : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint))
                state = "Joining";
            else if (leavingNodes.contains(primaryEndpoint))
                state = "Leaving";
            else if (movingNodes.contains(primaryEndpoint))
                state = "Moving";

            String load = loadMap.containsKey(primaryEndpoint)
                          ? loadMap.get(primaryEndpoint)
                          : "?";
            String owns = new DecimalFormat("##0.00%").format(ownerships.get(token));
            outs.printf(format, primaryEndpoint, dataCenter, rack, status, state, load, owns, token);
        }
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
    public void printInfo(PrintStream outs)
    {
        boolean gossipInitialized = probe.isInitialized();
        outs.printf("%-17s: %s%n", "Token", probe.getToken());
        outs.printf("%-17s: %s%n", "Gossip active", gossipInitialized);
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
        outs.printf("%-17s: %s%n", "Exceptions", probe.getExceptionCount());
    }

    public void printReleaseVersion(PrintStream outs)
    {
        outs.println("ReleaseVersion: " + probe.getReleaseVersion());
    }

    public void printNetworkStats(final InetAddress addr, PrintStream outs)
    {
        outs.printf("Mode: %s%n", probe.getOperationMode());
        Set<InetAddress> hosts = addr == null ? probe.getStreamDestinations() : new HashSet<InetAddress>(){{add(addr);}};
        if (hosts.size() == 0)
            outs.println("Not sending any streams.");
        for (InetAddress host : hosts)
        {
            try
            {
                List<String> files = probe.getFilesDestinedFor(host);
                if (files.size() > 0)
                {
                    outs.printf("Streaming to: %s%n", host);
                    for (String file : files)
                        outs.printf("   %s%n", file);
                }
                else
                {
                    outs.printf(" Nothing streaming to %s%n", host);
                }
            }
            catch (IOException ex)
            {
                outs.printf("   Error retrieving file data for %s%n", host);
            }
        }

        hosts = addr == null ? probe.getStreamSources() : new HashSet<InetAddress>(){{add(addr); }};
        if (hosts.size() == 0)
            outs.println("Not receiving any streams.");
        for (InetAddress host : hosts)
        {
            try
            {
                List<String> files = probe.getIncomingFiles(host);
                if (files.size() > 0)
                {
                    outs.printf("Streaming from: %s%n", host);
                    for (String file : files)
                        outs.printf("   %s%n", file);
                }
                else
                {
                    outs.printf(" Nothing streaming from %s%n", host);
                }
            }
            catch (IOException ex)
            {
                outs.printf("   Error retrieving file data for %s%n", host);
            }
        }

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
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        outs.println("pending tasks: " + cm.getPendingTasks());
        if (cm.getCompactions().size() > 0)
            outs.printf("%25s%16s%16s%16s%16s%10s%n", "compaction type", "keyspace", "column family", "bytes compacted", "bytes total", "progress");
        for (CompactionInfo c : cm.getCompactions())
        {
            String percentComplete = c.getTotalBytes() == 0
                                   ? "n/a"
                                   : new DecimalFormat("0.00").format((double) c.getBytesComplete() / c.getTotalBytes() * 100) + "%";
            outs.printf("%25s%16s%16s%16s%16s%10s%n", c.getTaskType(), c.getKeyspace(), c.getColumnFamily(), c.getBytesComplete(), c.getTotalBytes(), percentComplete);
        }
    }

    public void printColumnFamilyStats(PrintStream outs)
    {
        Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String tableName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(tableName))
            {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(tableName, columnFamilies);
            }
            else
            {
                cfstoreMap.get(tableName).add(cfsProxy);
            }
        }

        // print out the table statistics
        for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
        {
            String tableName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long tableReadCount = 0;
            long tableWriteCount = 0;
            int tablePendingTasks = 0;
            double tableTotalReadTime = 0.0f;
            double tableTotalWriteTime = 0.0f;

            outs.println("Keyspace: " + tableName);
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                long writeCount = cfstore.getWriteCount();
                long readCount = cfstore.getReadCount();

                if (readCount > 0)
                {
                    tableReadCount += readCount;
                    tableTotalReadTime += cfstore.getTotalReadLatencyMicros();
                }
                if (writeCount > 0)
                {
                    tableWriteCount += writeCount;
                    tableTotalWriteTime += cfstore.getTotalWriteLatencyMicros();
                }
                tablePendingTasks += cfstore.getPendingTasks();
            }

            double tableReadLatency = tableReadCount > 0 ? tableTotalReadTime / tableReadCount / 1000 : Double.NaN;
            double tableWriteLatency = tableWriteCount > 0 ? tableTotalWriteTime / tableWriteCount / 1000 : Double.NaN;

            outs.println("\tRead Count: " + tableReadCount);
            outs.println("\tRead Latency: " + String.format("%s", tableReadLatency) + " ms.");
            outs.println("\tWrite Count: " + tableWriteCount);
            outs.println("\tWrite Latency: " + String.format("%s", tableWriteLatency) + " ms.");
            outs.println("\tPending Tasks: " + tablePendingTasks);

            // print out column family statistics for this table
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                outs.println("\t\tColumn Family: " + cfstore.getColumnFamilyName());
                outs.println("\t\tSSTable count: " + cfstore.getLiveSSTableCount());
                outs.println("\t\tSpace used (live): " + cfstore.getLiveDiskSpaceUsed());
                outs.println("\t\tSpace used (total): " + cfstore.getTotalDiskSpaceUsed());
                outs.println("\t\tNumber of Keys (estimate): " + cfstore.estimateKeys());				
                outs.println("\t\tMemtable Columns Count: " + cfstore.getMemtableColumnsCount());
                outs.println("\t\tMemtable Data Size: " + cfstore.getMemtableDataSize());
                outs.println("\t\tMemtable Switch Count: " + cfstore.getMemtableSwitchCount());
                outs.println("\t\tRead Count: " + cfstore.getReadCount());
                outs.println("\t\tRead Latency: " + String.format("%01.3f", cfstore.getRecentReadLatencyMicros() / 1000) + " ms.");
                outs.println("\t\tWrite Count: " + cfstore.getWriteCount());
                outs.println("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getRecentWriteLatencyMicros() / 1000) + " ms.");
                outs.println("\t\tPending Tasks: " + cfstore.getPendingTasks());
                outs.println("\t\tBloom Filter False Postives: " + cfstore.getBloomFilterFalsePositives());
                outs.println("\t\tBloom Filter False Ratio: " + String.format("%01.5f", cfstore.getRecentBloomFilterFalseRatio()));
                outs.println("\t\tBloom Filter Space Used: " + cfstore.getBloomFilterDiskSpaceUsed());

                InstrumentingCacheMBean keyCacheMBean = probe.getKeyCacheMBean(tableName, cfstore.getColumnFamilyName());
                if (keyCacheMBean.getCapacity() > 0)
                {
                    outs.println("\t\tKey cache capacity: " + keyCacheMBean.getCapacity());
                    outs.println("\t\tKey cache size: " + keyCacheMBean.getSize());
                    outs.println("\t\tKey cache hit rate: " + keyCacheMBean.getRecentHitRate());
                }
                else
                {
                    outs.println("\t\tKey cache: disabled");
                }

                InstrumentingCacheMBean rowCacheMBean = probe.getRowCacheMBean(tableName, cfstore.getColumnFamilyName());
                if (rowCacheMBean.getCapacity() > 0)
                {
                    outs.println("\t\tRow cache capacity: " + rowCacheMBean.getCapacity());
                    outs.println("\t\tRow cache size: " + rowCacheMBean.getSize());
                    outs.println("\t\tRow cache hit rate: " + rowCacheMBean.getRecentHitRate());
                }
                else
                {
                    outs.println("\t\tRow cache: disabled");
                }

                outs.println("\t\tCompacted row minimum size: " + cfstore.getMinRowSize());
                outs.println("\t\tCompacted row maximum size: " + cfstore.getMaxRowSize());
                outs.println("\t\tCompacted row mean size: " + cfstore.getMeanRowSize());

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
        ColumnFamilyStoreMBean store = this.probe.getCfsProxy(keySpace, columnFamily);

        // default is 90 offsets
        long[] offsets = new EstimatedHistogram().getBucketOffsets();

        long[] rrlh = store.getRecentReadLatencyHistogramMicros();
        long[] rwlh = store.getRecentWriteLatencyHistogramMicros();
        long[] sprh = store.getRecentSSTablesPerReadHistogram();
        long[] ersh = store.getEstimatedRowSizeHistogram();
        long[] ecch = store.getEstimatedColumnCountHistogram();

        output.println(String.format("%s/%s histograms", keySpace, columnFamily));

        output.println(String.format("%-10s%10s%18s%18s%18s%18s",
                                     "Offset", "SSTables", "Write Latency", "Read Latency", "Row Size", "Column Count"));

        for (int i = 0; i < offsets.length; i++)
        {
            output.println(String.format("%-10d%10s%18s%18s%18s%18s",
                                         offsets[i],
                                         (i < sprh.length ? sprh[i] : ""),
                                         (i < rwlh.length ? rwlh[i] : ""),
                                         (i < rrlh.length ? rrlh[i] : ""),
                                         (i < ersh.length ? ersh[i] : ""),
                                         (i < ecch.length ? ecch[i] : "")));
        }
    }

    private void printEndPoints(String keySpace, String cf, String key, PrintStream output)
    {
        List<InetAddress> endpoints = this.probe.getEndpoints(keySpace, cf, key);

        for (InetAddress anEndpoint : endpoints)
        {
           output.println(anEndpoint.getHostAddress());
        }
    }

    private void printIsThriftServerRunning(PrintStream outs)
    {
        outs.println(probe.isThriftServerRunning() ? "running" : "not running");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException, ParseException
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

        String host = cmd.getOptionValue(HOST_OPT.left);
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

        String username = cmd.getOptionValue(USERNAME_OPT.left);
        String password = cmd.getOptionValue(PASSWORD_OPT.left);
        
        NodeProbe probe = null;
        try
        {
            probe = username == null ? new NodeProbe(host, port) : new NodeProbe(host, port, username, password);
        }
        catch (IOException ioe)
        {
            err(ioe, "Error connection to remote JMX agent!");
        }
        try
        {
            NodeCommand command = null;

            try
            {
                command = cmd.getCommand();
            }
            catch (IllegalArgumentException e)
            {
                badUse(e.getMessage());
            }


            NodeCmd nodeCmd = new NodeCmd(probe);

            // Execute the requested command.
            String[] arguments = cmd.getCommandArguments();

            switch (command)
            {
                case RING            : nodeCmd.printRing(System.out); break;
                case INFO            : nodeCmd.printInfo(System.out); break;
                case CFSTATS         : nodeCmd.printColumnFamilyStats(System.out); break;
                case DECOMMISSION    : probe.decommission(); break;
                case TPSTATS         : nodeCmd.printThreadPoolStats(System.out); break;
                case VERSION         : nodeCmd.printReleaseVersion(System.out); break;
                case COMPACTIONSTATS : nodeCmd.printCompactionStats(System.out); break;
                case DISABLEGOSSIP   : probe.stopGossiping(); break;
                case ENABLEGOSSIP    : probe.startGossiping(); break;
                case DISABLETHRIFT   : probe.stopThriftServer(); break;
                case ENABLETHRIFT    : probe.startThriftServer(); break;
                case STATUSTHRIFT    : nodeCmd.printIsThriftServerRunning(System.out); break;
    
                case DRAIN :
                    try { probe.drain(); }
                    catch (ExecutionException ee) { err(ee, "Error occured during flushing"); }
                    break;

                case NETSTATS :
                    if (arguments.length > 0) { nodeCmd.printNetworkStats(InetAddress.getByName(arguments[0]), System.out); }
                    else                      { nodeCmd.printNetworkStats(null, System.out); }
                    break;

                case SNAPSHOT :
                case CLEARSNAPSHOT :
                    String tag = cmd.getOptionValue(TAG_OPT.left);
                    handleSnapshots(command, tag, arguments, probe);
                    break;

                case MOVE :
                    if (arguments.length != 1) { badUse("Missing token argument for move."); }
                    probe.move(arguments[0]);
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
                    probe.setCompactionThroughput(Integer.valueOf(arguments[0]));
                    break;

                case REMOVETOKEN :
                    if (arguments.length != 1) { badUse("Missing an argument for removetoken (either status, force, or a token)"); }
                    else if (arguments[0].equals("status")) { nodeCmd.printRemovalStatus(System.out); }
                    else if (arguments[0].equals("force"))  { nodeCmd.printRemovalStatus(System.out); probe.forceRemoveCompletion(); }
                    else                                    { probe.removeToken(arguments[0]); }
                    break;

                case CLEANUP :
                case COMPACT :
                case REPAIR  :
                case FLUSH   :
                case SCRUB   :
                case UPGRADESSTABLES   :
                case INVALIDATEKEYCACHE :
                case INVALIDATEROWCACHE :
                    optionalKSandCFs(command, cmd, arguments, probe);
                    break;

                case GETCOMPACTIONTHRESHOLD :
                    if (arguments.length != 2) { badUse("getcompactionthreshold requires ks and cf args."); }
                    probe.getCompactionThreshold(System.out, arguments[0], arguments[1]);
                    break;

                case CFHISTOGRAMS :
                    if (arguments.length != 2) { badUse("cfhistograms requires ks and cf args"); }
                    nodeCmd.printCfHistograms(arguments[0], arguments[1], System.out);
                    break;

                case SETCACHECAPACITY :
                    if (arguments.length != 4) { badUse("setcachecapacity requires ks, cf, keycachecap, and rowcachecap args."); }
                    probe.setCacheCapacities(arguments[0], arguments[1], Integer.parseInt(arguments[2]), Integer.parseInt(arguments[3]));
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

                case REFRESH:
                    if (arguments.length != 2) { badUse("load_new_sstables requires ks and cf args"); }
                    probe.loadNewSSTables(arguments[0], arguments[1]);
                    break;

                case GOSSIPINFO : nodeCmd.printGossipInfo(System.out); break;

                case DESCRIBERING :
                    if (arguments.length != 1) { badUse("Missing keyspace argument for describering."); }
                    nodeCmd.printDescribeRing(arguments[0], System.out);
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
        System.exit(0);
    }

    private void printDescribeRing(String keyspaceName, PrintStream out)
    {
        out.println("TokenRange: ");
        try
        {
            for (String tokenRangeString : probe.describeRing(keyspaceName))
            {
                out.println("\t" + tokenRangeString);
            }
        }
        catch (InvalidRequestException e)
        {
            err(e, e.getWhy());
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

    private static void handleSnapshots(NodeCommand nc, String tag, String[] cmdArgs, NodeProbe probe) throws InterruptedException, IOException
    {
        String[] keyspaces = Arrays.copyOfRange(cmdArgs, 0, cmdArgs.length);
        System.out.print("Requested snapshot for: ");        
        if ( keyspaces.length > 0 ) 
        {
          for (int i = 0; i < keyspaces.length; i++) 
              System.out.print(keyspaces[i] + " ");            
        } 
        else 
        {
            System.out.print("all keyspaces");
        }
        System.out.println();
        
        switch (nc)
        {
            case SNAPSHOT :
                if (tag == null || tag.equals(""))
                    tag = new Long(System.currentTimeMillis()).toString();
                probe.takeSnapshot(tag, keyspaces);
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
                    if (cmd.hasOption(PRIMARY_RANGE_OPT.left))
                        probe.forceTableRepairPrimaryRange(keyspace, columnFamilies);
                    else
                        probe.forceTableRepair(keyspace, columnFamilies);
                    break;
                case INVALIDATEKEYCACHE : probe.invalidateKeyCaches(keyspace, columnFamilies); break;
                case INVALIDATEROWCACHE : probe.invalidateRowCaches(keyspace, columnFamilies); break;
                case FLUSH   :
                    try { probe.forceTableFlush(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occured during flushing"); }
                    break;
                case COMPACT :
                    try { probe.forceTableCompaction(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occured during compaction"); }
                    break;
                case CLEANUP :
                    if (keyspace.equals("system")) { break; } // Skip cleanup on system cfs.
                    try { probe.forceTableCleanup(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occured during cleanup"); }
                    break;
                case SCRUB :
                    try { probe.scrub(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occured while scrubbing keyspace " + keyspace); }
                    break;
                case UPGRADESSTABLES :
                    try { probe.upgradeSSTables(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occured while upgrading the sstables for keyspace " + keyspace); }
                    break;
                default:
                    throw new RuntimeException("Unreachable code.");
            }
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
                toReturn[i - 1] = (String) params.get(i);

            return toReturn;
        }
    }
}
