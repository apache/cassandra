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

/**
 * JMX client operations for Cassandra.
 */
public class NodeProbe
{
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.service:type=StorageService";
    private static final int defaultPort = 8080;
    private String host;
    private int port;

    private MBeanServerConnection mbeanServerConn;
    private StorageServiceMBean ssProxy;
    private MemoryMXBean memProxy;
    private RuntimeMXBean runtimeProxy;
    private CompactionManagerMBean mcmProxy;
    
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
    
    public void printColumnFamilyStats(PrintStream outs)
    {
        ObjectName query;
        try
        {
            Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();
            
            // get a list of column family stores
            query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilyStores,*");
            Set<ObjectName> result = mbeanServerConn.queryNames(query, null);
            for (ObjectName objectName: result) {
                String tableName = objectName.getKeyProperty("keyspace");
                ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mbeanServerConn, objectName, ColumnFamilyStoreMBean.class);

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
            for (String tableName : cfstoreMap.keySet())
            {
                List<ColumnFamilyStoreMBean> columnFamilies = cfstoreMap.get(tableName);
                int tableReadCount = 0;
                int tableWriteCount = 0;
                int tablePendingTasks = 0;
                double tableTotalReadTime = 0.0f;
                double tableTotalWriteTime = 0.0f;
                
                outs.println("Keyspace: " + tableName);
                for (ColumnFamilyStoreMBean cfstore : columnFamilies)
                {
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
                    outs.println("\t\tRead Latency: " + String.format("%01.3f", cfstore.getReadLatency()) + " ms.");
                    outs.println("\t\tWrite Count: " + cfstore.getWriteCount());
                    outs.println("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getWriteLatency()) + " ms.");
                    outs.println("\t\tPending Tasks: " + cfstore.getPendingTasks());
                    outs.println("");
                }
                outs.println("----------------");
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
}
