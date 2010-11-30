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

import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.cli.*;

import org.apache.cassandra.cache.JMXInstrumentedCacheMBean;
import org.apache.cassandra.concurrent.IExecutorMBean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingServiceMBean;

public class NodeCmd {
    private static final String HOST_OPT_LONG = "host";
    private static final String HOST_OPT_SHORT = "h";
    private static final String PORT_OPT_LONG = "port";
    private static final String PORT_OPT_SHORT = "p";
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
    }
    
    public NodeCmd(NodeProbe probe)
    {
        this.probe = probe;
    }
    
    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        String header = String.format(
                "%nAvailable commands: ring, info, version, cleanup, compact [keyspacename], cfstats, snapshot [snapshotname], " +
                "clearsnapshot, tpstats, flush, drain, repair, decommission, move, loadbalance, removetoken [status|force]|[token], " +
                "setcachecapacity [keyspace] [cfname] [keycachecapacity] [rowcachecapacity], " +
                "getcompactionthreshold [keyspace] [cfname], setcompactionthreshold [cfname] [minthreshold] [maxthreshold], " +
                "netstats [host], cfhistograms <keyspace> <column_family>");
        String usage = String.format("java %s --host <arg> <command>%n", NodeCmd.class.getName());
        hf.printHelp(usage, "", options, header);
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
            int tableReadCount = 0;
            int tableWriteCount = 0;
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
            System.err.println(parseExcep);
            printUsage();
            System.exit(1);
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
        
        NodeProbe probe = null;
        try
        {
            probe = new NodeProbe(host, port);
        }
        catch (IOException ioe)
        {
            System.err.println("Error connecting to remote JMX agent!");
            ioe.printStackTrace();
            System.exit(3);
        }
        
        if (cmd.getArgs().length < 1)
        {
            System.err.println("Missing argument for command.");
            printUsage();
            System.exit(1);
        }
        
        NodeCmd nodeCmd = new NodeCmd(probe);
        
        // Execute the requested command.
        String[] arguments = cmd.getArgs();
        String cmdName = arguments[0];
        if (cmdName.equals("ring"))
        {
            nodeCmd.printRing(System.out);
        }
        else if (cmdName.equals("info"))
        {
            nodeCmd.printInfo(System.out);
        }
        else if (cmdName.equals("cleanup"))
        {
            try
            {
                if (arguments.length > 1)
                    probe.forceTableCleanup(arguments[1]);
                else
                    probe.forceTableCleanup();
            }
            catch (ExecutionException ee)
            {
                System.err.println("Error occured during Keyspace cleanup");
                ee.printStackTrace();
                System.exit(3);
            }
        }
        else if (cmdName.equals("compact"))
        {
            try
            {
                if (arguments.length > 1)
                    probe.forceTableCompaction(arguments[1]);
                else
                    probe.forceTableCompaction();
            }
            catch (ExecutionException ee)
            {
                System.err.println("Error occured during Keyspace compaction");
                ee.printStackTrace();
                System.exit(3);
            }
        }
        else if (cmdName.equals("cfstats"))
        {
            nodeCmd.printColumnFamilyStats(System.out);
        }
        else if (cmdName.equals("decommission"))
        {
            probe.decommission();
        }
        else if (cmdName.equals("loadbalance"))
        {
            probe.loadBalance();
        }
        else if (cmdName.equals("move"))
        {
            if (arguments.length <= 1)
            {
                System.err.println("missing token argument");
            }
            probe.move(arguments[1]);
        }
        else if (cmdName.equals("removetoken"))
        {
            if (arguments.length <= 1)
            {
                System.err.println("Missing an argument.");
                printUsage();
            }
            else if (arguments[1].equals("status"))
            {
                nodeCmd.printRemovalStatus(System.out);
            }
            else if (arguments[1].equals("force"))
            {
                nodeCmd.printRemovalStatus(System.out);
                probe.forceRemoveCompletion();
            }
            else
                probe.removeToken(arguments[1]);

        }
        else if (cmdName.equals("snapshot"))
        {
            String snapshotName = "";
            if (arguments.length > 1)
            {
                snapshotName = arguments[1];
            }
            probe.takeSnapshot(snapshotName);
        }
        else if (cmdName.equals("clearsnapshot"))
        {
            probe.clearSnapshot();
        }
        else if (cmdName.equals("tpstats"))
        {
            nodeCmd.printThreadPoolStats(System.out);
        }
        else if (cmdName.equals("flush") || cmdName.equals("repair"))
        {
            if (cmd.getArgs().length < 2)
            {
                System.err.println("Missing keyspace argument.");
                printUsage();
                System.exit(1);
            }

            String[] columnFamilies = new String[cmd.getArgs().length - 2];
            for (int i = 0; i < columnFamilies.length; i++)
            {
                columnFamilies[i] = cmd.getArgs()[i + 2];
            }
            if (cmdName.equals("flush"))
                try
                {
                    probe.forceTableFlush(cmd.getArgs()[1], columnFamilies);
                } catch (ExecutionException ee)
                {
                    System.err.println("Error occured during flushing");
                    ee.printStackTrace();
                    System.exit(3);
                }
            else // cmdName.equals("repair")
                probe.forceTableRepair(cmd.getArgs()[1], columnFamilies);
        }
        else if (cmdName.equals("drain"))
        {
            try 
            {
                probe.drain();
            } catch (ExecutionException ee) 
            {
                System.err.println("Error occured during flushing");
                ee.printStackTrace();
                System.exit(3);
            }    	
        }
        else if (cmdName.equals("setcachecapacity"))
        {
            if (cmd.getArgs().length != 5) // ks cf keycachecap rowcachecap
            {
                System.err.println("cacheinfo requires: Keyspace name, ColumnFamily name, key cache capacity (in keys), and row cache capacity (in rows)");
            }
            String tableName = cmd.getArgs()[1];
            String cfName = cmd.getArgs()[2];
            int keyCacheCapacity = Integer.valueOf(cmd.getArgs()[3]);
            int rowCacheCapacity = Integer.valueOf(cmd.getArgs()[4]);
            probe.setCacheCapacities(tableName, cfName, keyCacheCapacity, rowCacheCapacity);
        }
        else if (cmdName.equals("getcompactionthreshold"))
        {
            if (arguments.length < 3) // ks cf
            {
                System.err.println("Missing keyspace/cfname");
                printUsage();
                System.exit(1);
            }
            probe.getCompactionThreshold(System.out, cmd.getArgs()[1], cmd.getArgs()[2]);
        }
        else if (cmdName.equals("setcompactionthreshold"))
        {
            if (cmd.getArgs().length != 5) // ks cf min max
            {
                System.err.println("setcompactionthreshold requires: Keyspace name, ColumnFamily name, " +
                                   "min threshold, and max threshold.");
                printUsage();
                System.exit(1);
            }
            String ks = cmd.getArgs()[1];
            String cf = cmd.getArgs()[2];
            int minthreshold = Integer.parseInt(arguments[3]);
            int maxthreshold = Integer.parseInt(arguments[4]);

            if ((minthreshold < 0) || (maxthreshold < 0))
            {
                System.err.println("Thresholds must be positive integers.");
                printUsage();
                System.exit(1);
            }

            if (minthreshold > maxthreshold)
            {
                System.err.println("Min threshold can't be greater than Max threshold");
                printUsage();
                System.exit(1);
            }

            if (minthreshold < 2 && maxthreshold != 0)
            {
                System.err.println("Min threshold must be at least 2");
                printUsage();
                System.exit(1);
            }
            probe.setCompactionThreshold(ks, cf, minthreshold, maxthreshold);
        }
        else if (cmdName.equals("netstats"))
        {
            // optional host
            String otherHost = arguments.length > 1 ? arguments[1] : null;
            nodeCmd.printNetworkStats(otherHost == null ? null : InetAddress.getByName(otherHost), System.out);
        }
        else if (cmdName.equals("cfhistograms"))
        {
            if (arguments.length < 3)
            {
                System.err.println("Usage of cfhistograms: <keyspace> <column_family>.");
                System.exit(1);
            }

            nodeCmd.printCfHistograms(arguments[1], arguments[2], System.out);
        }
        else if (cmdName.equals("version"))
        {
            nodeCmd.printReleaseVersion(System.out);
        }
        else
        {
            System.err.println("Unrecognized command: " + cmdName + ".");
            printUsage();
            System.exit(1);
        }

        System.exit(0);
    }
}
