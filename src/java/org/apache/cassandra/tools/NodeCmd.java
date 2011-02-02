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
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.*;

import org.apache.cassandra.cache.JMXInstrumentedCacheMBean;
import org.apache.cassandra.concurrent.IExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.CompactionManagerMBean;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.utils.EstimatedHistogram;

public class NodeCmd {
    private static final String HOST_OPT_LONG = "host";
    private static final String HOST_OPT_SHORT = "h";
    private static final String PORT_OPT_LONG = "port";
    private static final String PORT_OPT_SHORT = "p";
    private static final String USERNAME_OPT_LONG = "username";
    private static final String USERNAME_OPT_SHORT = "u";
    private static final String PASSWORD_OPT_LONG = "password";
    private static final String PASSWORD_OPT_SHORT = "pw";
    private static final int defaultPort = 8080;
    private static Options options = null;
    
    private NodeProbe probe;
    
    static
    {
        options = new Options();
        Option optHost = new Option(HOST_OPT_SHORT, HOST_OPT_LONG, true, "node hostname or ip address");
        optHost.setRequired(true);
        options.addOption(optHost);
        options.addOption(PORT_OPT_SHORT, PORT_OPT_LONG, true, "remote jmx agent port number");
        options.addOption(USERNAME_OPT_SHORT, USERNAME_OPT_LONG, true, "remote jmx agent username");
        options.addOption(PASSWORD_OPT_SHORT, PASSWORD_OPT_LONG, true, "remote jmx agent password");
    }
    
    public NodeCmd(NodeProbe probe)
    {
        this.probe = probe;
    }

    public enum NodeCommand {
        RING, INFO, CFSTATS, SNAPSHOT, CLEARSNAPSHOT, VERSION, TPSTATS, FLUSH, DRAIN,
        DECOMMISSION, MOVE, LOADBALANCE, REMOVETOKEN, REPAIR, CLEANUP, COMPACT,
        SETCACHECAPACITY, GETCOMPACTIONTHRESHOLD, SETCOMPACTIONTHRESHOLD, NETSTATS, CFHISTOGRAMS,
        COMPACTIONSTATS, DISABLEGOSSIP, ENABLEGOSSIP, INVALIDATEKEYCACHE, INVALIDATEROWCACHE
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
        addCmdHelp(header, "info", "Print node informations (uptime, load, ...)");
        addCmdHelp(header, "cfstats", "Print statistics on column families");
        addCmdHelp(header, "clearsnapshot", "Remove all existing snapshots");
        addCmdHelp(header, "version", "Print cassandra version");
        addCmdHelp(header, "tpstats", "Print usage statistics of thread pools");
        addCmdHelp(header, "drain", "Drain the node (stop accepting writes and flush all column families)");
        addCmdHelp(header, "decommission", "Decommission the node");
        addCmdHelp(header, "loadbalance", "Loadbalance the node");
        addCmdHelp(header, "compactionstats", "Print statistics on compactions");
        addCmdHelp(header, "disablegossip", "Disable gossip (effectively marking the node dead)");
        addCmdHelp(header, "enablegossip", "Reenable gossip");

        // One arg
        addCmdHelp(header, "snapshot [snapshotname]", "Take a snapshot using optional name snapshotname");
        addCmdHelp(header, "netstats [host]", "Print network information on provided host (connecting node by default)");
        addCmdHelp(header, "move <new token>", "Move node on the token ring to a new token");
        addCmdHelp(header, "removetoken status|force|<token>", "Show status of current token removal, force completion of pending removal or remove providen token");

        // Two args
        addCmdHelp(header, "flush [keyspace] [cfnames]", "Flush one or more column family");
        addCmdHelp(header, "repair [keyspace] [cfnames]", "Repair one or more column family");
        addCmdHelp(header, "cleanup [keyspace] [cfnames]", "Run cleanup on one or more column family");
        addCmdHelp(header, "compact [keyspace] [cfnames]", "Force a (major) compaction on one or more column family");
        addCmdHelp(header, "invalidatekeycache [keyspace] [cfnames]", "Invalidate the key cache of one or more column family");
        addCmdHelp(header, "invalidaterowcache [keyspace] [cfnames]", "Invalidate the key cache of one or more column family");
        addCmdHelp(header, "getcompactionthreshold <keyspace> <cfname>", "Print min and max compaction thresholds for a given column family");
        addCmdHelp(header, "cfhistograms <keyspace> <cfname>", "Print statistic histograms for a given column family");

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
        Map<String, String> loadMap = probe.getLoadMap();

        outs.printf("%-16s%-7s%-8s%-16s%-8s%-44s%n", "Address", "Status", "State", "Load", "Owns", "Token");
        // show pre-wrap token twice so you can always read a node's range as
        // (previous line token, current line token]
        if (sortedTokens.size() > 1)
            outs.printf("%-16s%-7s%-8s%-16s%-8s%-44s%n", "", "", "", "", "", sortedTokens.get(sortedTokens.size() - 1));

        // Calculate per-token ownership of the ring
        Map<Token, Float> ownerships = probe.getOwnership();

        for (Token token : sortedTokens)
        {
            String primaryEndpoint = tokenToEndpoint.get(token);
            String status = liveNodes.contains(primaryEndpoint)
                            ? "Up"
                            : deadNodes.contains(primaryEndpoint)
                              ? "Down"
                              : "?";
            String state = joiningNodes.contains(primaryEndpoint)
                           ? "Joining"
                           : leavingNodes.contains(primaryEndpoint)
                             ? "Leaving"
                             : "Normal";
            String load = loadMap.containsKey(primaryEndpoint)
                          ? loadMap.get(primaryEndpoint)
                          : "?";
            String owns = new DecimalFormat("##0.00%").format(ownerships.get(token));
            outs.printf("%-16s%-7s%-8s%-16s%-8s%-44s%n", primaryEndpoint, status, state, load, owns, token);
        }
    }

    public void printThreadPoolStats(PrintStream outs)
    {
        outs.printf("%-25s%10s%10s%15s%n", "Pool Name", "Active", "Pending", "Completed");

        Iterator<Map.Entry<String, IExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        while (threads.hasNext())
        {
            Entry<String, IExecutorMBean> thread = threads.next();
            String poolName = thread.getKey();
            IExecutorMBean threadPoolProxy = thread.getValue();
            outs.printf("%-25s%10s%10s%15s%n",
                        poolName, threadPoolProxy.getActiveCount(), threadPoolProxy.getPendingTasks(), threadPoolProxy.getCompletedTasks());
        }
    }

    /**
     * Write node information.
     * 
     * @param outs the stream to write to
     */
    public void printInfo(PrintStream outs)
    {
        outs.println(probe.getToken());
        outs.printf("%-17s: %s%n", "Gossip active", probe.isInitialized());
        outs.printf("%-17s: %s%n", "Load", probe.getLoadString());
        outs.printf("%-17s: %s%n", "Generation No", probe.getCurrentGenerationNumber());
        
        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        outs.printf("%-17s: %d%n", "Uptime (seconds)", secondsUp);

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double)heapUsage.getMax() / (1024 * 1024);
        outs.printf("%-17s: %.2f / %.2f%n", "Heap Memory (MB)", memUsed, memMax);
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

        MessagingServiceMBean ms = probe.getMsProxy();
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
        outs.println("compaction type: " + (cm.getCompactionType() == null ? "n/a" : cm.getCompactionType()));
        outs.println("column family: " + (cm.getColumnFamilyInProgress() == null ? "n/a" : cm.getColumnFamilyInProgress()));
        outs.println("bytes compacted: " + (cm.getBytesCompacted() == null ? "n/a" : cm.getBytesCompacted()));
        outs.println("bytes total in progress: " + (cm.getBytesTotalInProgress() == null ? "n/a" : cm.getBytesTotalInProgress() ));
        outs.println("pending tasks: " + cm.getPendingTasks());
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
                outs.println("\t\tMemtable Columns Count: " + cfstore.getMemtableColumnsCount());
                outs.println("\t\tMemtable Data Size: " + cfstore.getMemtableDataSize());
                outs.println("\t\tMemtable Switch Count: " + cfstore.getMemtableSwitchCount());
                outs.println("\t\tRead Count: " + cfstore.getReadCount());
                outs.println("\t\tRead Latency: " + String.format("%01.3f", cfstore.getRecentReadLatencyMicros() / 1000) + " ms.");
                outs.println("\t\tWrite Count: " + cfstore.getWriteCount());
                outs.println("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getRecentWriteLatencyMicros() / 1000) + " ms.");
                outs.println("\t\tPending Tasks: " + cfstore.getPendingTasks());

                JMXInstrumentedCacheMBean keyCacheMBean = probe.getKeyCacheMBean(tableName, cfstore.getColumnFamilyName());
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

                JMXInstrumentedCacheMBean rowCacheMBean = probe.getRowCacheMBean(tableName, cfstore.getColumnFamilyName());
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
        long[] offsets = new EstimatedHistogram(90).getBucketOffsets();

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
                                         (i < rrlh.length ? rrlh[i] : ""),
                                         (i < rwlh.length ? rwlh[i] : ""),
                                         (i < ersh.length ? ersh[i] : ""),
                                         (i < ecch.length ? ecch[i] : "")));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ParseException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException parseExcep)
        {
            badUse(parseExcep.toString());
        }

        String host = cmd.getOptionValue(HOST_OPT_LONG);
        int port = defaultPort;
        
        String portNum = cmd.getOptionValue(PORT_OPT_LONG);
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
        String username = cmd.getOptionValue(USERNAME_OPT_LONG);
        String password = cmd.getOptionValue(PASSWORD_OPT_LONG);
        
        NodeProbe probe = null;
        try
        {
            probe = username == null ? new NodeProbe(host, port) : new NodeProbe(host, port, username, password);
        }
        catch (IOException ioe)
        {
            err(ioe, "Error connection to remote JMX agent!");
        }
        
        if (cmd.getArgs().length < 1)
            badUse("Missing argument for command.");

        NodeCmd nodeCmd = new NodeCmd(probe);
        
        // Execute the requested command.
        String[] arguments = cmd.getArgs();
        String cmdName = arguments[0];

        boolean validCommand = false;
        for (NodeCommand n : NodeCommand.values())
        {
            if (cmdName.toUpperCase().equals(n.name()))
                validCommand = true;
        }

        if (!validCommand)
            badUse("Unrecognized command: " + cmdName);

        NodeCommand nc = NodeCommand.valueOf(cmdName.toUpperCase());
        switch (nc)
        {
            case RING            : nodeCmd.printRing(System.out); break;
            case INFO            : nodeCmd.printInfo(System.out); break;
            case CFSTATS         : nodeCmd.printColumnFamilyStats(System.out); break;
            case DECOMMISSION    : probe.decommission(); break;
            case LOADBALANCE     : probe.loadBalance(); break;
            case CLEARSNAPSHOT   : probe.clearSnapshot(); break;
            case TPSTATS         : nodeCmd.printThreadPoolStats(System.out); break;
            case VERSION         : nodeCmd.printReleaseVersion(System.out); break;
            case COMPACTIONSTATS : nodeCmd.printCompactionStats(System.out); break;
            case DISABLEGOSSIP   : probe.stopGossiping(); break;
            case ENABLEGOSSIP    : probe.startGossiping(); break;

            case DRAIN :
                try { probe.drain(); }
                catch (ExecutionException ee) { err(ee, "Error occured during flushing"); }
                break;

            case NETSTATS :
                if (arguments.length > 1) { nodeCmd.printNetworkStats(InetAddress.getByName(arguments[1]), System.out); }
                else                      { nodeCmd.printNetworkStats(null, System.out); }
                break;

            case SNAPSHOT :
                if (arguments.length > 1) { probe.takeSnapshot(arguments[1]); }
                else                      { probe.takeSnapshot(""); }
                break;

            case MOVE :
                if (arguments.length != 2) { badUse("Missing token argument for move."); }
                probe.move(arguments[1]);
                break;

            case REMOVETOKEN :
                if (arguments.length != 2) { badUse("Missing an argument for removetoken (either status, force, or a token)"); }
                else if (arguments[1].equals("status")) { nodeCmd.printRemovalStatus(System.out); }
                else if (arguments[1].equals("force"))  { nodeCmd.printRemovalStatus(System.out); probe.forceRemoveCompletion(); }
                else                                    { probe.removeToken(arguments[1]); }
                break;

            case CLEANUP :
            case COMPACT :
            case REPAIR  :
            case FLUSH   :
            case INVALIDATEKEYCACHE :
            case INVALIDATEROWCACHE :
                optionalKSandCFs(nc, arguments, probe);
                break;

            case GETCOMPACTIONTHRESHOLD :
                if (arguments.length != 3) { badUse("getcompactionthreshold requires ks and cf args."); }
                probe.getCompactionThreshold(System.out, arguments[1], arguments[2]);
                break;

            case CFHISTOGRAMS :
                if (arguments.length != 3) { badUse("cfhistograms requires ks and cf args"); }
                nodeCmd.printCfHistograms(arguments[1], arguments[2], System.out);
                break;

            case SETCACHECAPACITY :
                if (arguments.length != 5) { badUse("setcachecapacity requires ks, cf, keycachecap, and rowcachecap args."); }
                probe.setCacheCapacities(arguments[1], arguments[2], Integer.parseInt(arguments[3]), Integer.parseInt(arguments[4]));
                break;

            case SETCOMPACTIONTHRESHOLD :
                if (arguments.length != 5) { badUse("setcompactionthreshold requires ks, cf, min, and max threshold args."); }
                int minthreshold = Integer.parseInt(arguments[3]);
                int maxthreshold = Integer.parseInt(arguments[4]);
                if ((minthreshold < 0) || (maxthreshold < 0)) { badUse("Thresholds must be positive integers"); }
                if (minthreshold > maxthreshold)              { badUse("Min threshold cannot be greater than max."); }
                if (minthreshold < 2 && maxthreshold != 0)    { badUse("Min threshold must be at least 2"); }
                probe.setCompactionThreshold(arguments[1], arguments[2], minthreshold, maxthreshold);
                break;

            default :
                throw new RuntimeException("Unreachable code.");

        }

        System.exit(0);
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

    private static void optionalKSandCFs(NodeCommand nc, String[] cmdArgs, NodeProbe probe) throws InterruptedException, IOException
    {
        // Per-keyspace
        if (cmdArgs.length == 1)
        {
            for (String keyspace : probe.getKeyspaces())
            {
                switch (nc)
                {
                    case REPAIR             : probe.forceTableRepair(keyspace); break;
                    case INVALIDATEKEYCACHE : probe.invalidateKeyCaches(keyspace); break;
                    case INVALIDATEROWCACHE : probe.invalidateRowCaches(keyspace); break;
                    case FLUSH   :
                        try { probe.forceTableFlush(keyspace); }
                        catch (ExecutionException ee) { err(ee, "Error occured while flushing keyspace " + keyspace); }
                        break;
                    case COMPACT :
                        try { probe.forceTableCompaction(keyspace); }
                        catch (ExecutionException ee) { err(ee, "Error occured while compacting keyspace " + keyspace); }
                        break;
                    case CLEANUP :
                        if (keyspace.equals("system")) { break; } // Skip cleanup on system cfs.
                        try { probe.forceTableCleanup(keyspace); }
                        catch (ExecutionException ee) { err(ee, "Error occured while cleaning up keyspace " + keyspace); }
                        break;
                    default:
                        throw new RuntimeException("Unreachable code.");
                }
            }
        }
        // Per-cf (or listed cfs) in given keyspace
        else
        {
            String keyspace = cmdArgs[1];
            String[] columnFamilies = new String[cmdArgs.length - 2];
            for (int i = 0; i < columnFamilies.length; i++)
            {
                columnFamilies[i] = cmdArgs[i + 2];
            }
            switch (nc)
            {
                case REPAIR  : probe.forceTableRepair(keyspace, columnFamilies); break;
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
                    try { probe.forceTableCleanup(keyspace, columnFamilies); }
                    catch (ExecutionException ee) { err(ee, "Error occured during cleanup"); }
                    break;
                default:
                    throw new RuntimeException("Unreachable code.");
            }
        }
    }
}
