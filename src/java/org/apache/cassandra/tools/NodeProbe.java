/**
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

import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.cassandra.concurrent.IExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.CompactionManagerMBean;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * JMX client operations for Cassandra.
 */
public class NodeProbe
{
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.service:type=StorageService";
    private static final String HOST_OPTION = "host";
    private static final String PORT_OPTION = "port";
    private static final int defaultPort = 8080;
    private static Options options = null;
    private CommandLine cmd = null;
    private String host;
    private int port;
    
    private MBeanServerConnection mbeanServerConn;
    private StorageServiceMBean ssProxy;
    private MemoryMXBean memProxy;
    private RuntimeMXBean runtimeProxy;
    private CompactionManagerMBean mcmProxy;

    static
    {
        options = new Options();
        Option optHost = new Option(HOST_OPTION, true, "node hostname or ip address");
        optHost.setRequired(true);
        options.addOption(optHost);
        options.addOption(PORT_OPTION, true, "remote jmx agent port number");
    }
    
    /**
     * Creates a NodeProbe using command-line arguments.
     * 
     * @param cmdArgs list of arguments passed on the command line
     * @throws ParseException for missing required, or unrecognized options
     * @throws IOException on connection failures
     */
    private NodeProbe(String[] cmdArgs) throws ParseException, IOException, InterruptedException
    {
        parseArgs(cmdArgs);
        this.host = cmd.getOptionValue(HOST_OPTION);
        
        String portNum = cmd.getOptionValue(PORT_OPTION);
        if (portNum != null)
        {
            try
            {
                this.port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }
        else
        {
            this.port = defaultPort;
        }
        
        connect();
    }
    
    /**
     * Creates a NodeProbe using the specified JMX host and port.
     * 
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    public NodeProbe(String host, int port) throws IOException, InterruptedException
    {
        this.host = host;
        this.port = port;
        connect();
    }
    
    /**
     * Creates a NodeProbe using the specified JMX host and default port.
     * 
     * @param host hostname or IP address of the JMX agent
     * @throws IOException on connection failures
     */
    public NodeProbe(String host) throws IOException, InterruptedException
    {
        this.host = host;
        this.port = defaultPort;
        connect();
    }
    
    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     * 
     * @throws IOException on connection failures
     */
    private void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
        JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, null);
        mbeanServerConn = jmxc.getMBeanServerConnection();
        
        try
        {
            ObjectName name = new ObjectName(ssObjName);
            ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
            name = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
            mcmProxy = JMX.newMBeanProxy(mbeanServerConn, name, CompactionManagerMBean.class);
        } catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(
                    "Invalid ObjectName? Please report this as a bug.", e);
        }
        
        memProxy = ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn, 
                ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
        runtimeProxy = ManagementFactory.newPlatformMXBeanProxy(
                mbeanServerConn, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
    }

    public void forceTableCleanup() throws IOException
    {
        ssProxy.forceTableCleanup();
    }
    
    public void forceTableCompaction() throws IOException
    {
        ssProxy.forceTableCompaction();
    }

    public void forceTableFlush(String tableName, String... columnFamilies) throws IOException
    {
        ssProxy.forceTableFlush(tableName, columnFamilies);
    }

    public void forceTableRepair(String tableName, String... columnFamilies) throws IOException
    {
        ssProxy.forceTableRepair(tableName, columnFamilies);
    }

    /**
     * Write a textual representation of the Cassandra ring.
     * 
     * @param outs the stream to write to
     */
    public void printRing(PrintStream outs)
    {
        Map<Range, List<String>> rangeMap = ssProxy.getRangeToEndPointMap();
        List<Range> ranges = new ArrayList<Range>(rangeMap.keySet());
        Collections.sort(ranges);
        Set<String> liveNodes = ssProxy.getLiveNodes();
        Set<String> deadNodes = ssProxy.getUnreachableNodes();
        Map<String, String> loadMap = ssProxy.getLoadMap();

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
            outs.println(String.format("%-14s%-11s%-14s%-43s", "", "", "", ranges.get(0).left()));
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

            outs.print(String.format("%-43s", range.right()));

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
    
    public void printColumnFamilyStats(PrintStream outs) {

        ObjectName query;
        try {
            Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();
            
            // get a list of column family stores
            query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilyStores,*");
            Set<ObjectName> result = mbeanServerConn.queryNames(query, null);
            for (ObjectName objectName: result) {
                String tableName = objectName.getKeyProperty("name");
                ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(
                        mbeanServerConn, objectName, ColumnFamilyStoreMBean.class);
                
                if (!cfstoreMap.containsKey(tableName)) {
                    List <ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                    columnFamilies.add(cfsProxy);
                    cfstoreMap.put(tableName, columnFamilies);
                } 
                else {
                    cfstoreMap.get(tableName).add(cfsProxy);
                }
            }

            // print out the table statistics
            for (String tableName: cfstoreMap.keySet()) {
                List <ColumnFamilyStoreMBean> columnFamilies = cfstoreMap.get(tableName);
                int tableReadCount = 0;
                int tableWriteCount = 0;
                int tablePendingTasks = 0;
                double tableTotalReadTime = 0.0f;
                double tableTotalWriteTime = 0.0f;
                
                outs.println("Keyspace: " + tableName);
                for (ColumnFamilyStoreMBean cfstore: columnFamilies) {
                    int writeCount = cfstore.getWriteCount();
                    int readCount = cfstore.getReadCount();
                    
                    if (readCount > 0)
                    {
                        tableReadCount += readCount;
                        tableTotalReadTime += cfstore.getReadLatency() * readCount;
                    }
                    if (writeCount > 0)
                    {
                        tableWriteCount += writeCount;
                        tableTotalWriteTime += cfstore.getWriteLatency() * writeCount;
                    }
                    tablePendingTasks += cfstore.getPendingTasks();
                }
                
                double tableReadLatency = tableReadCount > 0 ? tableTotalReadTime / tableReadCount : Double.NaN;
                double tableWriteLatency = tableWriteCount > 0 ? tableTotalWriteTime / tableWriteCount : Double.NaN;
                
                outs.println("\tRead Count: " + tableReadCount);
                outs.println("\tRead Latency: " + String.format("%01.3f", tableReadLatency) + " ms.");
                outs.println("\tWrite Count: " + tableWriteCount);
                outs.println("\tWrite Latency: " + String.format("%01.3f", tableWriteLatency) + " ms.");
                outs.println("\tPending Tasks: " + tablePendingTasks);
                // print out column family statistic for this table
                for (ColumnFamilyStoreMBean cfstore: columnFamilies) {
                    outs.println("\t\tColumn Family: " + cfstore.getColumnFamilyName());
                    outs.println("\t\tMemtable Columns Count: " + cfstore.getMemtableColumnsCount());
                    outs.println("\t\tMemtable Data Size: " + cfstore.getMemtableDataSize());
                    outs.println("\t\tMemtable Switch Count: " + cfstore.getMemtableSwitchCount());
                    outs.println("\t\tRead Count: " + cfstore.getReadCount());
                    outs.println("\t\tRead Latency: " + String.format("%01.3f", cfstore.getReadLatency()) + " ms.");
                    outs.println("\t\tWrite Count: " + cfstore.getWriteCount());
                    outs.println("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getWriteLatency()) + " ms.");
                    outs.println("\t\tPending Tasks: " + cfstore.getPendingTasks());
                    outs.println("");
                }
                outs.println("----------------");
            }
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", e);
        } catch (IOException e) {
            throw new RuntimeException("Could not retrieve list of stat mbeans.", e);
        }
        
    }

    /**
     * Write node information.
     * 
     * @param outs the stream to write to
     */
    public void printInfo(PrintStream outs)
    {
        outs.println(ssProxy.getToken());
        outs.println(String.format("%-17s: %s", "Load", ssProxy.getLoadString()));
        outs.println(String.format("%-17s: %s", "Generation No", ssProxy.getCurrentGenerationNumber()));
        
        // Uptime
        long secondsUp = runtimeProxy.getUptime() / 1000;
        outs.println(String.format("%-17s: %d", "Uptime (seconds)", secondsUp));

        // Memory usage
        MemoryUsage heapUsage = memProxy.getHeapMemoryUsage();
        double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double)heapUsage.getMax() / (1024 * 1024);
        outs.println(String.format("%-17s: %.2f / %.2f", "Heap Memory (MB)", memUsed, memMax));
    }
    
    /**
     * Take a snapshot of all the tables.
     * 
     * @param snapshotName the name of the snapshot.
     */
    public void takeSnapshot(String snapshotName) throws IOException
    {
        ssProxy.takeAllSnapshot(snapshotName);
    }

    /**
     * Remove all the existing snapshots.
     */
    public void clearSnapshot() throws IOException
    {
        ssProxy.clearSnapshot();
    }

    public void decommission() throws InterruptedException
    {
        ssProxy.decommission();
    }

    public void loadBalance() throws IOException, InterruptedException
    {
        ssProxy.loadBalance();
    }

    public void move(String newToken) throws InterruptedException
    {
        ssProxy.move(newToken);
    }

    public void removeToken(String token)
    {
        ssProxy.removeToken(token);
    }

    /**
     * Print out the size of the queues in the thread pools
     *
     * @param outs Output stream to generate the output on.
     */
    public void printThreadPoolStats(PrintStream outs)
    {
        ObjectName query;
        try
        {
            outs.print(String.format("%-25s", "Pool Name"));
            outs.print(String.format("%10s", "Active"));
            outs.print(String.format("%10s", "Pending"));
            outs.print(String.format("%15s", "Completed"));
            outs.println();

            query = new ObjectName("org.apache.cassandra.concurrent:type=*");
            Set<ObjectName> result = mbeanServerConn.queryNames(query, null);
            for (ObjectName objectName : result)
            {
                String poolName = objectName.getKeyProperty("type");
                IExecutorMBean threadPoolProxy = JMX.newMBeanProxy(mbeanServerConn, objectName, IExecutorMBean.class);
                outs.print(String.format("%-25s", poolName));
                outs.print(String.format("%10d", threadPoolProxy.getActiveCount()));
                outs.print(String.format("%10d", threadPoolProxy.getPendingTasks()));
                outs.print(String.format("%15d", threadPoolProxy.getCompletedTasks()));
                outs.println();
            }
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not retrieve list of stat mbeans.", e);
        }
    }

    /**
     * Get the compaction threshold
     *
     * @param outs the stream to write to
     */
    public void getCompactionThreshold(PrintStream outs)
    {
        outs.println("Current compaction threshold: Min=" +  mcmProxy.getMinimumCompactionThreshold() +
            ", Max=" +  mcmProxy.getMaximumCompactionThreshold());
    }

    /**
     * Set the compaction threshold
     *
     * @param minimumCompactionThreshold minimum compaction threshold
     * @param maximumCompactionThreshold maximum compaction threshold
     */
    public void setCompactionThreshold(int minimumCompactionThreshold, int maximumCompactionThreshold)
    {
        mcmProxy.setMinimumCompactionThreshold(minimumCompactionThreshold);
        if (maximumCompactionThreshold >= 0)
        {
             mcmProxy.setMaximumCompactionThreshold(maximumCompactionThreshold);
        }
    }

    /**
     * Parse the supplied command line arguments.
     * 
     * @param args arguments passed on the command line
     * @throws ParseException for missing required, or unrecognized options
     */
    private void parseArgs(String[] args) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        cmd = parser.parse(options, args);
    }
    
    /**
     * Retrieve any non-option arguments passed on the command line.
     * 
     * @return non-option command args
     */
    private String[] getArgs()
    {
        return cmd.getArgs();
    }
    
    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        String header = String.format(
                "%nAvailable commands: ring, info, cleanup, compact, cfstats, snapshot [name], clearsnapshot, " +
                "tpstats, flush, repair, decommission, move, loadbalance, removetoken, " +
                " getcompactionthreshold, setcompactionthreshold [minthreshold] ([maxthreshold])");
        String usage = String.format("java %s -host <arg> <command>%n", NodeProbe.class.getName());
        hf.printHelp(usage, "", options, header);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException
    {
        NodeProbe probe = null;
        try
        {
            probe = new NodeProbe(args);
        }
        catch (ParseException pe)
        {
            System.err.println(pe.getMessage());
            NodeProbe.printUsage();
            System.exit(1);
        }
        catch (IOException ioe)
        {
            System.err.println("Error connecting to remote JMX agent!");
            ioe.printStackTrace();
            System.exit(3);
        }
        
        if (probe.getArgs().length < 1)
        {
            System.err.println("Missing argument for command.");
            NodeProbe.printUsage();
            System.exit(1);
        }
        
        // Execute the requested command.
        String[] arguments = probe.getArgs();
        String cmdName = arguments[0];
        if (cmdName.equals("ring"))
        {
            probe.printRing(System.out);
        }
        else if (cmdName.equals("info"))
        {
            probe.printInfo(System.out);
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
            probe.printColumnFamilyStats(System.out);
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
            probe.printThreadPoolStats(System.out);
        }
        else if (cmdName.equals("flush") || cmdName.equals("repair"))
        {
            if (probe.getArgs().length < 2)
            {
                System.err.println("Missing keyspace argument.");
                NodeProbe.printUsage();
                System.exit(1);
            }

            String[] columnFamilies = new String[probe.getArgs().length - 2];
            for (int i = 0; i < columnFamilies.length; i++)
            {
                columnFamilies[i] = probe.getArgs()[i + 2];
            }
            if (cmdName.equals("flush"))
                probe.forceTableFlush(probe.getArgs()[1], columnFamilies);
            else // cmdName.equals("repair")
                probe.forceTableRepair(probe.getArgs()[1], columnFamilies);
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
                NodeProbe.printUsage();
                System.exit(1);
            }
            int minthreshold = Integer.parseInt(arguments[1]);
            int maxthreshold = CompactionManager.instance().getMaximumCompactionThreshold();
            if (arguments.length > 2)
            {
                maxthreshold = Integer.parseInt(arguments[2]);
            }

            if (minthreshold > maxthreshold)
            {
                System.err.println("Min threshold can't be greater than Max threshold");
                NodeProbe.printUsage();
                System.exit(1);
            }

            if (minthreshold < 2 && maxthreshold != 0)
            {
                System.err.println("Min threshold must be at least 2");
                NodeProbe.printUsage();
                System.exit(1);
            }
            probe.setCompactionThreshold(minthreshold, maxthreshold);
        }
        else
        {
            System.err.println("Unrecognized command: " + cmdName + ".");
            NodeProbe.printUsage();
            System.exit(1);
        }

        System.exit(0);
    }

}
