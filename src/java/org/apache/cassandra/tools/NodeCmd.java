package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.cassandra.cache.JMXAggregatingCacheMBean;
import org.apache.cassandra.cache.JMXInstrumentedCacheMBean;
import org.apache.cassandra.concurrent.IExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.dht.Range;

import org.apache.commons.cli.*;

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
                "%nAvailable commands: ring, info, cleanup, compact, cfstats, snapshot [snapshotname], clearsnapshot, " +
                "tpstats, flush, repair, decommission, move, loadbalance, removetoken, " +
                "setcachecapacity <keyspace> <cfname> <keycachecapacity> <rowcachecapacity>, " +
                "getcompactionthreshold, setcompactionthreshold [minthreshold] ([maxthreshold])");
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
        Map<Range, List<String>> rangeMap = probe.getRangeToEndPointMap(null);
        List<Range> ranges = new ArrayList<Range>(rangeMap.keySet());
        Collections.sort(ranges);
        Set<String> liveNodes = probe.getLiveNodes();
        Set<String> deadNodes = probe.getUnreachableNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        // Print range-to-endpoint mapping
        int counter = 0;
        outs.print(String.format("%-14s", "Address"));
        outs.print(String.format("%-11s", "Status"));
        outs.print(String.format("%-14s", "Load"));
        outs.print(String.format("%-43s", "Range"));
        outs.println("Ring");
        // emphasize that we're showing the right part of each range
        if (ranges.size() > 1)
        {
            outs.println(String.format("%-14s%-11s%-14s%-43s", "", "", "", ranges.get(0).left));
        }
        // normal range & node info
        for (Range range : ranges) {
            List<String> endpoints = rangeMap.get(range);
            String primaryEndpoint = endpoints.get(0);

            outs.print(String.format("%-14s", primaryEndpoint));

            String status = liveNodes.contains(primaryEndpoint)
                          ? "Up"
                          : deadNodes.contains(primaryEndpoint)
                            ? "Down"
                            : "?";
            outs.print(String.format("%-11s", status));

            String load = loadMap.containsKey(primaryEndpoint) ? loadMap.get(primaryEndpoint) : "?";
            outs.print(String.format("%-14s", load));

            outs.print(String.format("%-43s", range.right));

            String asciiRingArt;
            if (counter == 0)
            {
                asciiRingArt = "|<--|";
            }
            else if (counter == (rangeMap.size() - 1))
            {
                asciiRingArt = "|-->|";
            }
            else
            {
                if ((rangeMap.size() > 4) && ((counter % 2) == 0))
                    asciiRingArt = "v   |";
                else if ((rangeMap.size() > 4) && ((counter % 2) != 0))
                    asciiRingArt = "|   ^";
                else
                    asciiRingArt = "|   |";
            }
            outs.println(asciiRingArt);
            
            counter++;
        }
    }
    
    public void printThreadPoolStats(PrintStream outs)
    {
        outs.print(String.format("%-25s", "Pool Name"));
        outs.print(String.format("%10s", "Active"));
        outs.print(String.format("%10s", "Pending"));
        outs.print(String.format("%15s", "Completed"));
        outs.println();
        
        Iterator<Map.Entry<String, IExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        
        for (; threads.hasNext();)
        {
            Map.Entry<String, IExecutorMBean> thread = threads.next();
            String poolName = thread.getKey();
            IExecutorMBean threadPoolProxy = thread.getValue();
            outs.print(String.format("%-25s", poolName));
            outs.print(String.format("%10d", threadPoolProxy.getActiveCount()));
            outs.print(String.format("%10d", threadPoolProxy.getPendingTasks()));
            outs.print(String.format("%15d", threadPoolProxy.getCompletedTasks()));
            outs.println();
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
        outs.println(String.format("%-17s: %s", "Load", probe.getLoadString()));
        outs.println(String.format("%-17s: %s", "Generation No", probe.getCurrentGenerationNumber()));
        
        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        outs.println(String.format("%-17s: %d", "Uptime (seconds)", secondsUp));

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double)heapUsage.getMax() / (1024 * 1024);
        outs.println(String.format("%-17s: %.2f / %.2f", "Heap Memory (MB)", memUsed, memMax));
    }
    
    public void printColumnFamilyStats(PrintStream outs)
    {
        Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        for (;cfamilies.hasNext();)
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
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

                JMXAggregatingCacheMBean keyCacheMBean = probe.getKeyCacheMBean(tableName, cfstore.getColumnFamilyName());
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

                outs.println("");
            }
            outs.println("----------------");
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
            probe.forceTableCleanup();
        }
        else if (cmdName.equals("compact"))
        {
            probe.forceTableCompaction();
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
                System.err.println("missing token argument");
            }
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
                probe.forceTableFlush(cmd.getArgs()[1], columnFamilies);
            else // cmdName.equals("repair")
                probe.forceTableRepair(cmd.getArgs()[1], columnFamilies);
        }
        else if (cmdName.equals("setcachecapacity"))
        {
            if (cmd.getArgs().length != 5)
            {
                System.err.println("cacheinfo requires keyspace and column family name arguments, followed by key cache capacity and row cache capacity, in rows");
            }
            String tableName = cmd.getArgs()[1];
            String cfName = cmd.getArgs()[2];
            int keyCacheCapacity = Integer.valueOf(cmd.getArgs()[3]);
            int rowCacheCapacity = Integer.valueOf(cmd.getArgs()[4]);
            probe.setCacheCapacities(tableName, cfName, keyCacheCapacity, rowCacheCapacity);
        }
        else if (cmdName.equals("getcompactionthreshold"))
        {
            probe.getCompactionThreshold(System.out);
        }
        else if (cmdName.equals("setcompactionthreshold"))
        {
            if (arguments.length < 2)
            {
                System.err.println("Missing threshold value(s)");
                printUsage();
                System.exit(1);
            }
            int minthreshold = Integer.parseInt(arguments[1]);
            int maxthreshold = CompactionManager.instance.getMaximumCompactionThreshold();
            if (arguments.length > 2)
            {
                maxthreshold = Integer.parseInt(arguments[2]);
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
            probe.setCompactionThreshold(minthreshold, maxthreshold);
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
