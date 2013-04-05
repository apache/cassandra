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

import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.collect.Iterables;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.*;
import org.apache.cassandra.streaming.StreamingService;
import org.apache.cassandra.streaming.StreamingServiceMBean;
import org.apache.cassandra.utils.SimpleCondition;

/**
 * JMX client operations for Cassandra.
 */
public class NodeProbe
{
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    private static final int defaultPort = 7199;
    final String host;
    final int port;
    private String username;
    private String password;

    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;
    private CompactionManagerMBean compactionProxy;
    private StorageServiceMBean ssProxy;
    private MemoryMXBean memProxy;
    private RuntimeMXBean runtimeProxy;
    private StreamingServiceMBean streamProxy;
    public MessagingServiceMBean msProxy;
    private FailureDetectorMBean fdProxy;
    private CacheServiceMBean cacheService;
    private PBSPredictorMBean PBSPredictorProxy;
    private StorageProxyMBean spProxy;
    private HintedHandOffManagerMBean hhProxy;
    private boolean failed;

    /**
     * Creates a NodeProbe using the specified JMX host, port, username, and password.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    public NodeProbe(String host, int port, String username, String password) throws IOException, InterruptedException
    {
        assert username != null && !username.isEmpty() && password != null && !password.isEmpty()
               : "neither username nor password can be blank";

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
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
        Map<String,Object> env = new HashMap<String,Object>();
        if (username != null)
        {
            String[] creds = { username, password };
            env.put(JMXConnector.CREDENTIALS, creds);
        }
        jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        mbeanServerConn = jmxc.getMBeanServerConnection();

        try
        {
            ObjectName name = new ObjectName(ssObjName);
            ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
            name = new ObjectName(PBSPredictor.MBEAN_NAME);
            PBSPredictorProxy = JMX.newMBeanProxy(mbeanServerConn, name, PBSPredictorMBean.class);
            name = new ObjectName(MessagingService.MBEAN_NAME);
            msProxy = JMX.newMBeanProxy(mbeanServerConn, name, MessagingServiceMBean.class);
            name = new ObjectName(StreamingService.MBEAN_OBJECT_NAME);
            streamProxy = JMX.newMBeanProxy(mbeanServerConn, name, StreamingServiceMBean.class);
            name = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
            compactionProxy = JMX.newMBeanProxy(mbeanServerConn, name, CompactionManagerMBean.class);
            name = new ObjectName(FailureDetector.MBEAN_NAME);
            fdProxy = JMX.newMBeanProxy(mbeanServerConn, name, FailureDetectorMBean.class);
            name = new ObjectName(CacheService.MBEAN_NAME);
            cacheService = JMX.newMBeanProxy(mbeanServerConn, name, CacheServiceMBean.class);
            name = new ObjectName(StorageProxy.MBEAN_NAME);
            spProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageProxyMBean.class);
            name = new ObjectName(HintedHandOffManager.MBEAN_NAME);
            hhProxy = JMX.newMBeanProxy(mbeanServerConn, name, HintedHandOffManagerMBean.class);
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

    public void close() throws IOException
    {
        jmxc.close();
    }

    public void forceTableCleanup(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceTableCleanup(tableName, columnFamilies);
    }

    public void scrub(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.scrub(tableName, columnFamilies);
    }

    public void upgradeSSTables(String tableName, boolean excludeCurrentVersion, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.upgradeSSTables(tableName, excludeCurrentVersion, columnFamilies);
    }

    public void forceTableCompaction(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceTableCompaction(tableName, columnFamilies);
    }

    public void forceTableFlush(String tableName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceTableFlush(tableName, columnFamilies);
    }

    public void forceTableRepair(String tableName, boolean isSequential, boolean isLocal, String... columnFamilies) throws IOException
    {
        ssProxy.forceTableRepair(tableName, isSequential, isLocal, columnFamilies);
    }

    public void forceRepairAsync(final PrintStream out, final String tableName, boolean isSequential, boolean isLocal, boolean primaryRange, String... columnFamilies) throws IOException
    {
        RepairRunner runner = new RepairRunner(out, tableName, columnFamilies);
        try
        {
            ssProxy.addNotificationListener(runner, null, null);
            if (!runner.repairAndWait(ssProxy, isSequential, isLocal, primaryRange))
                failed = true;
        }
        catch (Exception e)
        {
            throw new IOException(e) ;
        }
        finally
        {
            try
            {
               ssProxy.removeNotificationListener(runner);
            }
            catch (ListenerNotFoundException ignored) {}
        }
    }

    public void forceRepairRangeAsync(final PrintStream out, final String tableName, boolean isSequential, boolean isLocal, final String startToken, final String endToken, String... columnFamilies) throws IOException
    {
        RepairRunner runner = new RepairRunner(out, tableName, columnFamilies);
        try
        {
            ssProxy.addNotificationListener(runner, null, null);
            if (!runner.repairRangeAndWait(ssProxy,  isSequential, isLocal, startToken, endToken))
                failed = true;
        }
        catch (Exception e)
        {
            throw new IOException(e) ;
        }
        finally
        {
            try
            {
                ssProxy.removeNotificationListener(runner);
            }
            catch (ListenerNotFoundException ignored) {}
        }
    }

    public void forceTableRepairPrimaryRange(String tableName, boolean isSequential, boolean isLocal, String... columnFamilies) throws IOException
    {
        ssProxy.forceTableRepairPrimaryRange(tableName, isSequential, isLocal, columnFamilies);
    }

    public void forceTableRepairRange(String beginToken, String endToken, String tableName, boolean isSequential, boolean isLocal, String... columnFamilies) throws IOException
    {
        ssProxy.forceTableRepairRange(beginToken, endToken, tableName, isSequential, isLocal, columnFamilies);
    }

    public void invalidateKeyCache() throws IOException
    {
        cacheService.invalidateKeyCache();
    }

    public void invalidateRowCache() throws IOException
    {
        cacheService.invalidateRowCache();
    }

    public void drain() throws IOException, InterruptedException, ExecutionException
    {
        ssProxy.drain();
    }

    public Map<String, String> getTokenToEndpointMap()
    {
        return ssProxy.getTokenToEndpointMap();
    }

    public List<String> getLiveNodes()
    {
        return ssProxy.getLiveNodes();
    }

    public List<String> getJoiningNodes()
    {
        return ssProxy.getJoiningNodes();
    }

    public List<String> getLeavingNodes()
    {
        return ssProxy.getLeavingNodes();
    }

    public List<String> getMovingNodes()
    {
        return ssProxy.getMovingNodes();
    }

    public List<String> getUnreachableNodes()
    {
        return ssProxy.getUnreachableNodes();
    }

    public Map<String, String> getLoadMap()
    {
        return ssProxy.getLoadMap();
    }

    public Map<InetAddress, Float> getOwnership()
    {
        return ssProxy.getOwnership();
    }

    public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException
    {
        return ssProxy.effectiveOwnership(keyspace);
    }

    public CacheServiceMBean getCacheServiceMBean()
    {
        String cachePath = "org.apache.cassandra.db:type=Caches";

        try
        {
            return JMX.newMBeanProxy(mbeanServerConn, new ObjectName(cachePath), CacheServiceMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies()
    {
        try
        {
            return new ColumnFamilyStoreMBeanIterator(mbeanServerConn);
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

    public CompactionManagerMBean getCompactionManagerProxy()
    {
      return compactionProxy;
    }

    public List<String> getTokens()
    {
        return ssProxy.getTokens();
    }

    public List<String> getTokens(String endpoint)
    {
        try
        {
            return ssProxy.getTokens(endpoint);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public String getLocalHostId()
    {
        return ssProxy.getLocalHostId();
    }

    public Map<String, String> getHostIdMap()
    {
        return ssProxy.getHostIdMap();
    }

    public String getLoadString()
    {
        return ssProxy.getLoadString();
    }

    public String getReleaseVersion()
    {
        return ssProxy.getReleaseVersion();
    }

    public int getCurrentGenerationNumber()
    {
        return ssProxy.getCurrentGenerationNumber();
    }

    public long getUptime()
    {
        return runtimeProxy.getUptime();
    }

    public MemoryUsage getHeapMemoryUsage()
    {
        return memProxy.getHeapMemoryUsage();
    }

    /**
     * Take a snapshot of all the tables, optionally specifying only a specific column family.
     *
     * @param snapshotName the name of the snapshot.
     * @param columnFamily the column family to snapshot or all on null
     * @param keyspaces the keyspaces to snapshot
     */
    public void takeSnapshot(String snapshotName, String columnFamily, String... keyspaces) throws IOException
    {
        if (columnFamily != null)
        {
            if (keyspaces.length != 1)
            {
                throw new IOException("When specifying the column family for a snapshot, you must specify one and only one keyspace");
            }
            ssProxy.takeColumnFamilySnapshot(keyspaces[0], columnFamily, snapshotName);
        }
        else
            ssProxy.takeSnapshot(snapshotName, keyspaces);
    }

    /**
     * Remove all the existing snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaces) throws IOException
    {
        ssProxy.clearSnapshot(tag, keyspaces);
    }

    public boolean isJoined()
    {
        return ssProxy.isJoined();
    }

    public void joinRing() throws IOException
    {
        ssProxy.joinRing();
    }

    public void decommission() throws InterruptedException
    {
        ssProxy.decommission();
    }

    public void move(String newToken) throws IOException, InterruptedException
    {
        ssProxy.move(newToken);
    }

    public void removeNode(String token)
    {
        ssProxy.removeNode(token);
    }

    public String getRemovalStatus()
    {
        return ssProxy.getRemovalStatus();
    }

    public void forceRemoveCompletion()
    {
        ssProxy.forceRemoveCompletion();
    }

    public Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> getThreadPoolMBeanProxies()
    {
        try
        {
            return new ThreadPoolProxyMBeanIterator(mbeanServerConn);
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
    public void getCompactionThreshold(PrintStream outs, String ks, String cf)
    {
        ColumnFamilyStoreMBean cfsProxy = getCfsProxy(ks, cf);
        outs.println("Current compaction thresholds for " + ks + "/" + cf + ": \n" +
                     " min = " + cfsProxy.getMinimumCompactionThreshold() + ", " +
                     " max = " + cfsProxy.getMaximumCompactionThreshold());
    }

    /**
     * Set the compaction threshold
     *
     * @param minimumCompactionThreshold minimum compaction threshold
     * @param maximumCompactionThreshold maximum compaction threshold
     */
    public void setCompactionThreshold(String ks, String cf, int minimumCompactionThreshold, int maximumCompactionThreshold)
    {
        ColumnFamilyStoreMBean cfsProxy = getCfsProxy(ks, cf);
        cfsProxy.setCompactionThresholds(minimumCompactionThreshold, maximumCompactionThreshold);
    }

    public void setCacheCapacities(int keyCacheCapacity, int rowCacheCapacity)
    {
        try
        {
            String keyCachePath = "org.apache.cassandra.db:type=Caches";
            CacheServiceMBean cacheMBean = JMX.newMBeanProxy(mbeanServerConn, new ObjectName(keyCachePath), CacheServiceMBean.class);
            cacheMBean.setKeyCacheCapacityInMB(keyCacheCapacity);
            cacheMBean.setRowCacheCapacityInMB(rowCacheCapacity);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public List<InetAddress> getEndpoints(String keyspace, String cf, String key)
    {
        return ssProxy.getNaturalEndpoints(keyspace, cf, key);
    }

    public List<String> getSSTables(String keyspace, String cf, String key)
    {
        ColumnFamilyStoreMBean cfsProxy = getCfsProxy(keyspace, cf);
        return cfsProxy.getSSTablesForKey(key);
    }

    public Set<InetAddress> getStreamDestinations()
    {
        return streamProxy.getStreamDestinations();
    }

    public List<String> getFilesDestinedFor(InetAddress host) throws IOException
    {
        return streamProxy.getOutgoingFiles(host.getHostAddress());
    }

    public Set<InetAddress> getStreamSources()
    {
        return streamProxy.getStreamSources();
    }

    public List<String> getIncomingFiles(InetAddress host) throws IOException
    {
        return streamProxy.getIncomingFiles(host.getHostAddress());
    }

    public String getOperationMode()
    {
        return ssProxy.getOperationMode();
    }

    public void truncate(String tableName, String cfName)
    {
        try
        {
            ssProxy.truncate(tableName, cfName);
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException("Error while executing truncate", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error while executing truncate", e);
        }
    }

    public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy()
    {
        try
        {
            return JMX.newMBeanProxy(mbeanServerConn, new ObjectName("org.apache.cassandra.db:type=EndpointSnitchInfo"), EndpointSnitchInfoMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ColumnFamilyStoreMBean getCfsProxy(String ks, String cf)
    {
        ColumnFamilyStoreMBean cfsProxy = null;
        try
        {
            String type = cf.contains(".") ? "IndexColumnFamilies" : "ColumnFamilies";
            Set<ObjectName> beans = mbeanServerConn.queryNames(
                    new ObjectName("org.apache.cassandra.db:type=*" + type +",keyspace=" + ks + ",columnfamily=" + cf), null);

            if (beans.isEmpty())
                throw new MalformedObjectNameException("couldn't find that bean");
            assert beans.size() == 1;
            for (ObjectName bean : beans)
                cfsProxy = JMX.newMBeanProxy(mbeanServerConn, bean, ColumnFamilyStoreMBean.class);
        }
        catch (MalformedObjectNameException mone)
        {
            System.err.println("ColumnFamilyStore for " + ks + "/" + cf + " not found.");
            System.exit(1);
        }
        catch (IOException e)
        {
            System.err.println("ColumnFamilyStore for " + ks + "/" + cf + " not found: " + e);
            System.exit(1);
        }

        return cfsProxy;
    }

    public StorageProxyMBean getSpProxy()
    {
        return spProxy;
    }

    public String getEndpoint()
    {
        // Try to find the endpoint using the local token, doing so in a crazy manner
        // to maintain backwards compatibility with the MBean interface
        String stringToken = ssProxy.getTokens().get(0);
        Map<String, String> tokenToEndpoint = ssProxy.getTokenToEndpointMap();

        for (Map.Entry<String, String> pair : tokenToEndpoint.entrySet())
        {
            if (pair.getKey().toString().equals(stringToken))
            {
                return pair.getValue();
            }
        }

        throw new RuntimeException("Could not find myself in the endpoint list, something is very wrong!  Is the Cassandra node fully started?");
    }

    public String getDataCenter()
    {
        try
        {
            return getEndpointSnitchInfoProxy().getDatacenter(getEndpoint());
        }
        catch (UnknownHostException e)
        {
            return "Unknown";
        }
    }

    public String getRack()
    {
        try
        {
            return getEndpointSnitchInfoProxy().getRack(getEndpoint());
        }
        catch (UnknownHostException e)
        {
            return "Unknown";
        }
    }

    public List<String> getKeyspaces()
    {
        return ssProxy.getKeyspaces();
    }

    public void disableHintedHandoff()
    {
        spProxy.setHintedHandoffEnabled(false);
    }

    public void enableHintedHandoff()
    {
        spProxy.setHintedHandoffEnabled(true);
    }

    public void pauseHintsDelivery()
    {
        hhProxy.pauseHintsDelivery(true);
    }

    public void resumeHintsDelivery()
    {
        hhProxy.pauseHintsDelivery(false);
    }

    public void stopNativeTransport()
    {
        ssProxy.stopNativeTransport();
    }

    public void startNativeTransport()
    {
        ssProxy.startNativeTransport();
    }

    public boolean isNativeTransportRunning()
    {
        return ssProxy.isNativeTransportRunning();
    }

    public void stopGossiping()
    {
        ssProxy.stopGossiping();
    }

    public void startGossiping()
    {
        ssProxy.startGossiping();
    }

    public void stopThriftServer()
    {
        ssProxy.stopRPCServer();
    }

    public void startThriftServer()
    {
        ssProxy.startRPCServer();
    }

    public boolean isThriftServerRunning()
    {
        return ssProxy.isRPCServerRunning();
    }

    public boolean isInitialized()
    {
        return ssProxy.isInitialized();
    }

    public void setCompactionThroughput(int value)
    {
        ssProxy.setCompactionThroughputMbPerSec(value);
    }

    public int getCompactionThroughput()
    {
        return ssProxy.getCompactionThroughputMbPerSec();
    }

    public int getExceptionCount()
    {
        return ssProxy.getExceptionCount();
    }

    public Map<String, Integer> getDroppedMessages()
    {
        return msProxy.getDroppedMessages();
    }

    public void loadNewSSTables(String ksName, String cfName)
    {
        ssProxy.loadNewSSTables(ksName, cfName);
    }

    public void rebuildIndex(String ksName, String cfName, String... idxNames)
    {
        ssProxy.rebuildSecondaryIndex(ksName, cfName, idxNames);
    }

    public String getGossipInfo()
    {
        return fdProxy.getAllEndpointStates();
    }

    public void stop(String string)
    {
        compactionProxy.stopCompaction(string);
    }

    public void setStreamThroughput(int value)
    {
        ssProxy.setStreamThroughputMbPerSec(value);
    }

    public void setTraceProbability(double value)
    {
        ssProxy.setTraceProbability(value);
    }

    public String getSchemaVersion()
    {
        return ssProxy.getSchemaVersion();
    }

    public List<String> describeRing(String keyspaceName) throws IOException
    {
        return ssProxy.describeRingJMX(keyspaceName);
    }

    public PBSPredictorMBean getPBSPredictorMBean()
    {
        return PBSPredictorProxy;
    }

    public void rebuild(String sourceDc)
    {
        ssProxy.rebuild(sourceDc);
    }

    public List<String> sampleKeyRange()
    {
        return ssProxy.sampleKeyRange();
    }

    public void resetLocalSchema() throws IOException
    {
        ssProxy.resetLocalSchema();
    }

    public boolean isFailed()
    {
        return failed;
    }
}

class ColumnFamilyStoreMBeanIterator implements Iterator<Map.Entry<String, ColumnFamilyStoreMBean>>
{
    private MBeanServerConnection mbeanServerConn;
    Iterator<Entry<String, ColumnFamilyStoreMBean>> mbeans;

    public ColumnFamilyStoreMBeanIterator(MBeanServerConnection mbeanServerConn)
        throws MalformedObjectNameException, NullPointerException, IOException
    {
        this.mbeanServerConn = mbeanServerConn;
        List<Entry<String, ColumnFamilyStoreMBean>> cfMbeans = getCFSMBeans(mbeanServerConn, "ColumnFamilies");
        cfMbeans.addAll(getCFSMBeans(mbeanServerConn, "IndexColumnFamilies"));
        Collections.sort(cfMbeans, new Comparator<Entry<String, ColumnFamilyStoreMBean>>()
        {
            public int compare(Entry<String, ColumnFamilyStoreMBean> e1, Entry<String, ColumnFamilyStoreMBean> e2)
            {
                //compare keyspace, then CF name, then normal vs. index
                int tableCmp = e1.getKey().compareTo(e2.getKey());
                if(tableCmp != 0)
                    return tableCmp;

                // get CF name and split it for index name
                String e1CF[] = e1.getValue().getColumnFamilyName().split("\\.");
                String e2CF[] = e1.getValue().getColumnFamilyName().split("\\.");
                assert e1CF.length <= 2 && e2CF.length <= 2 : "unexpected split count for column family name";

                //if neither are indexes, just compare CF names
                if(e1CF.length == 1 && e2CF.length == 1)
                    return e1CF[0].compareTo(e2CF[0]);

                //check if it's the same CF
                int cfNameCmp = e1CF[0].compareTo(e2CF[0]);
                if(cfNameCmp != 0)
                    return cfNameCmp;

                // if both are indexes (for the same CF), compare them
                if(e1CF.length == 2 && e2CF.length == 2)
                    return e1CF[1].compareTo(e2CF[1]);

                //if length of e1CF is 1, it's not an index, so sort it higher
                return e1CF.length == 1 ? 1 : -1;
            }
        });
        mbeans = cfMbeans.iterator();
    }

    private List<Entry<String, ColumnFamilyStoreMBean>> getCFSMBeans(MBeanServerConnection mbeanServerConn, String type)
            throws MalformedObjectNameException, IOException
    {
        ObjectName query = new ObjectName("org.apache.cassandra.db:type=" + type +",*");
        Set<ObjectName> cfObjects = mbeanServerConn.queryNames(query, null);
        List<Entry<String, ColumnFamilyStoreMBean>> mbeans = new ArrayList<Entry<String, ColumnFamilyStoreMBean>>(cfObjects.size());
        for(ObjectName n : cfObjects)
        {
            String tableName = n.getKeyProperty("keyspace");
            ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mbeanServerConn, n, ColumnFamilyStoreMBean.class);
            mbeans.add(new AbstractMap.SimpleImmutableEntry<String, ColumnFamilyStoreMBean>(tableName, cfsProxy));
        }
        return mbeans;
    }

    public boolean hasNext()
    {
        return mbeans.hasNext();
    }

    public Entry<String, ColumnFamilyStoreMBean> next()
    {
        return mbeans.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}

class ThreadPoolProxyMBeanIterator implements Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>>
{
    private final Iterator<ObjectName> resIter;
    private final MBeanServerConnection mbeanServerConn;

    public ThreadPoolProxyMBeanIterator(MBeanServerConnection mbeanServerConn)
    throws MalformedObjectNameException, NullPointerException, IOException
    {
        Set<ObjectName> requests = mbeanServerConn.queryNames(new ObjectName("org.apache.cassandra.request:type=*"), null);
        Set<ObjectName> internal = mbeanServerConn.queryNames(new ObjectName("org.apache.cassandra.internal:type=*"), null);
        resIter = Iterables.concat(requests, internal).iterator();
        this.mbeanServerConn = mbeanServerConn;
    }

    public boolean hasNext()
    {
        return resIter.hasNext();
    }

    public Map.Entry<String, JMXEnabledThreadPoolExecutorMBean> next()
    {
        ObjectName objectName = resIter.next();
        String poolName = objectName.getKeyProperty("type");
        JMXEnabledThreadPoolExecutorMBean threadPoolProxy = JMX.newMBeanProxy(mbeanServerConn, objectName, JMXEnabledThreadPoolExecutorMBean.class);
        return new AbstractMap.SimpleImmutableEntry<String, JMXEnabledThreadPoolExecutorMBean>(poolName, threadPoolProxy);
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}

class RepairRunner implements NotificationListener
{
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    private final Condition condition = new SimpleCondition();
    private final PrintStream out;
    private final String keyspace;
    private final String[] columnFamilies;
    private int cmd;
    private boolean success = true;

    RepairRunner(PrintStream out, String keyspace, String... columnFamilies)
    {
        this.out = out;
        this.keyspace = keyspace;
        this.columnFamilies = columnFamilies;
    }

    public boolean repairAndWait(StorageServiceMBean ssProxy, boolean isSequential, boolean isLocal, boolean primaryRangeOnly) throws InterruptedException
    {
        cmd = ssProxy.forceRepairAsync(keyspace, isSequential, isLocal, primaryRangeOnly, columnFamilies);
        if (cmd > 0)
        {
            condition.await();
        }
        else
        {
            String message = String.format("[%s] Nothing to repair for keyspace '%s'", format.format(System.currentTimeMillis()), keyspace);
            out.println(message);
        }
        return success;
    }

    public boolean repairRangeAndWait(StorageServiceMBean ssProxy, boolean isSequential, boolean isLocal, String startToken, String endToken) throws InterruptedException
    {
        cmd = ssProxy.forceRepairRangeAsync(startToken, endToken, keyspace, isSequential, isLocal, columnFamilies);
        if (cmd > 0)
        {
            condition.await();
        }
        else
        {
            String message = String.format("[%s] Nothing to repair for keyspace '%s'", format.format(System.currentTimeMillis()), keyspace);
            out.println(message);
        }
        return success;
    }

    public void handleNotification(Notification notification, Object handback)
    {
        if ("repair".equals(notification.getType()))
        {
            int[] status = (int[]) notification.getUserData();
            assert status.length == 2;
            if (cmd == status[0])
            {
                String message = String.format("[%s] %s", format.format(notification.getTimeStamp()), notification.getMessage());
                out.println(message);
                // repair status is int array with [0] = cmd number, [1] = status
                if (status[1] == AntiEntropyService.Status.SESSION_FAILED.ordinal())
                    success = false;
                else if (status[1] == AntiEntropyService.Status.FINISHED.ordinal())
                    condition.signalAll();
            }
        }
    }
}
