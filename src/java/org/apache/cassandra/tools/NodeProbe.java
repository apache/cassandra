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

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.EndPoint;
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
    private NodeProbe(String[] cmdArgs) throws ParseException, IOException
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
    public NodeProbe(String host, int port) throws IOException
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
    public NodeProbe(String host) throws IOException
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
    
    /**
     * Retrieve a map of range to end points that describe the ring topology
     * of a Cassandra cluster.
     * 
     * @return mapping of ranges to end points
     */
    public Map<Range, List<EndPoint>> getRangeToEndpointMap()
    {
        return ssProxy.getRangeToEndPointMap();
    }
    
    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried. The 
     * returned string is a space delimited list of host:port end points.
     * 
     * @return space delimited list of nodes
     */
    public String getLiveNodes()
    {
        return ssProxy.getLiveNodes();
    }
    
    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined
     * by this node's failure detector. The returned string is a space
     * delimited list of host:port end points.
     * 
     * @return space delimited list of nodes
     */
    public String getUnreachableNodes()
    {
        return ssProxy.getUnreachableNodes();
    }
    
    /**
     * Fetch a string representation of the token.
     * 
     * @return a string token
     */
    public String getToken()
    {
        return ssProxy.getToken();
    }
    
    /**
     * Return the generation value for this node.
     * 
     * @return generation number
     */
    public int getCurrentGenerationNumber()
    {
        return ssProxy.getCurrentGenerationNumber();
    }
    
    /**
     * Retrieve a textual representation of the on-disk size of data
     * stored on this node.
     * 
     * @return the size description
     */
    public String getLoadInfo()
    {
        return ssProxy.getLoadInfo();
    }
    
    /**
     * Trigger a cleanup of keys on all tables.
     */
    public void forceTableCleanup() throws IOException
    {
        ssProxy.forceTableCleanup();
    }
    
    /**
     * Trigger compaction of all tables.
     */
    public void forceTableCompaction() throws IOException
    {
        ssProxy.forceTableCompaction();
    }
    
    /**
     * Write a textual representation of the Cassandra ring.
     * 
     * @param outs the stream to write to
     */
    public void printRing(PrintStream outs)
    {
        Map<Range, List<EndPoint>> rangeMap = getRangeToEndpointMap();
        
        // Print range-to-endpoint mapping
        int counter = 0;
        for (Range range : rangeMap.keySet()) {
            List<EndPoint> endpoints = rangeMap.get(range);
            
            outs.print(String.format("%-46s ", range.left()));
            outs.print(String.format("%2d ", endpoints.size()));
            outs.print(String.format("%-15s", endpoints.get(0).getHost()));
            
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
                {
                    asciiRingArt = "v   |";
                }
                else if ((rangeMap.size() > 4) && ((counter % 2) != 0))
                {
                    asciiRingArt = "|   ^";
                }
                else
                {
                    asciiRingArt = "|   |";
                }
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
                
                outs.println("Table: " + tableName);
                for (ColumnFamilyStoreMBean cfstore: columnFamilies) {
                    int writeCount = cfstore.getWriteCount();
                    int readCount = cfstore.getReadCount();
                    
                    tableReadCount += readCount;
                    tableTotalReadTime += cfstore.getReadLatency() * readCount;
                    tableWriteCount += writeCount;
                    tableTotalWriteTime += cfstore.getWriteLatency() * writeCount;
                    tablePendingTasks += cfstore.getPendingTasks();
                }
                
                double tableReadLatency = Double.NaN;
                double tableWriteLatency = Double.NaN;
                
                if (tableReadCount > 0.0f) {
                    tableReadLatency = tableTotalReadTime / tableReadCount;
                }
                if (tableWriteCount > 0.0f) {
                    tableWriteLatency = tableTotalWriteTime / tableWriteCount;
                }
                
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
                    outs.println("\t\tRead Disk Count: " + cfstore.getReadDiskHits());
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
     * Write a list of nodes with corresponding status.
     * 
     * @param outs the stream to write to
     */
    public void printCluster(PrintStream outs)
    {
        for (String upNode : getLiveNodes().split("\\s+"))
        {
            if (upNode.length() > 0)
            {
                outs.println(String.format("%-21s up", upNode));
            }
        }
        
        for (String downNode : getUnreachableNodes().split("\\s+"))
        {
            if (downNode.length() > 0)
            {
                outs.println(String.format("%-21s down", downNode));
            }
        }
    }
    
    /**
     * Write node information.
     * 
     * @param outs the stream to write to
     */
    public void printInfo(PrintStream outs)
    {
        outs.println(getToken());
        outs.println(String.format("%-17s: %s", "Load Info", getLoadInfo()));
        outs.println(String.format("%-17s: %s", "Generation No", getCurrentGenerationNumber()));
        
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
     * Retrieve any non-option arguments passed on the command line.
     * 
     * @return non-option command args
     */
    private String[] getArgs()
    {
        return cmd.getArgs();
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
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        String header = String.format(
                "%nAvailable commands: ring, cluster, info, cleanup, compact, cfstats");
        String usage = String.format("java %s -host <arg> <command>%n", NodeProbe.class.getName());
        hf.printHelp(usage, "", options, header);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException
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
        String cmdName = probe.getArgs()[0];
        if (cmdName.equals("ring"))
        {
            probe.printRing(System.out);
        }
        else if (cmdName.equals("cluster"))
        {
            probe.printCluster(System.out);
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
        else
        {
            System.err.println("Unrecognized command: " + cmdName + ".");
            NodeProbe.printUsage();
            System.exit(1);
        }

        System.exit(0);
    }

}
