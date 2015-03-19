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
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.openmbean.*;

import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.*;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;

import org.apache.cassandra.metrics.ColumnFamilyMetrics.Sampler;

/**
 * JMX client operations for Cassandra.
 */
public class NodeProbe implements AutoCloseable
{
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
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
    private GossiperMBean gossProxy;
    private MemoryMXBean memProxy;
    private GCInspectorMXBean gcProxy;
    private RuntimeMXBean runtimeProxy;
    private StreamManagerMBean streamProxy;
    public MessagingServiceMBean msProxy;
    private FailureDetectorMBean fdProxy;
    private CacheServiceMBean cacheService;
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
    public NodeProbe(String host, int port, String username, String password) throws IOException
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
            name = new ObjectName(MessagingService.MBEAN_NAME);
            msProxy = JMX.newMBeanProxy(mbeanServerConn, name, MessagingServiceMBean.class);
            name = new ObjectName(StreamManagerMBean.OBJECT_NAME);
            streamProxy = JMX.newMBeanProxy(mbeanServerConn, name, StreamManagerMBean.class);
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
            name = new ObjectName(GCInspector.MBEAN_NAME);
            gcProxy = JMX.newMBeanProxy(mbeanServerConn, name, GCInspectorMXBean.class);
            name = new ObjectName(Gossiper.MBEAN_NAME);
            gossProxy = JMX.newMBeanProxy(mbeanServerConn, name, GossiperMBean.class);
        }
        catch (MalformedObjectNameException e)
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

    public int forceKeyspaceCleanup(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.forceKeyspaceCleanup(keyspaceName, columnFamilies);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.scrub(disableSnapshot, skipCorrupted, keyspaceName, columnFamilies);
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.upgradeSSTables(keyspaceName, excludeCurrentVersion, columnFamilies);
    }

    public void forceKeyspaceCleanup(PrintStream out, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        if (forceKeyspaceCleanup(keyspaceName, columnFamilies) != 0)
        {
            failed = true;
            out.println("Aborted cleaning up atleast one table in keyspace "+keyspaceName+", check server logs for more information.");
        }
    }

    public void scrub(PrintStream out, boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        if (scrub(disableSnapshot, skipCorrupted, keyspaceName, columnFamilies) != 0)
        {
            failed = true;
            out.println("Aborted scrubbing atleast one table in keyspace "+keyspaceName+", check server logs for more information.");
        }
    }

    public void upgradeSSTables(PrintStream out, String keyspaceName, boolean excludeCurrentVersion, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        if (upgradeSSTables(keyspaceName, excludeCurrentVersion, columnFamilies) != 0)
        {
            failed = true;
            out.println("Aborted upgrading sstables for atleast one table in keyspace "+keyspaceName+", check server logs for more information.");
        }
    }


    public void forceKeyspaceCompaction(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceKeyspaceCompaction(keyspaceName, columnFamilies);
    }

    public void forceKeyspaceFlush(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceKeyspaceFlush(keyspaceName, columnFamilies);
    }

    public void repairAsync(final PrintStream out, final String keyspace, Map<String, String> options) throws IOException
    {
        RepairRunner runner = new RepairRunner(out, ssProxy, keyspace, options);
        try
        {
            jmxc.addConnectionNotificationListener(runner, null, null);
            ssProxy.addNotificationListener(runner, null, null);
            runner.run();
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
                jmxc.removeConnectionNotificationListener(runner);
            }
            catch (Throwable e)
            {
                out.println("Exception occurred during clean-up. " + e);
            }
        }
    }

    public Map<Sampler, CompositeData> getPartitionSample(String ks, String cf, int capacity, int duration, int count, List<Sampler> samplers) throws OpenDataException
    {
        ColumnFamilyStoreMBean cfsProxy = getCfsProxy(ks, cf);
        for(Sampler sampler : samplers)
        {
            cfsProxy.beginLocalSampling(sampler.name(), capacity);
        }
        Uninterruptibles.sleepUninterruptibly(duration, TimeUnit.MILLISECONDS);
        Map<Sampler, CompositeData> result = Maps.newHashMap();
        for(Sampler sampler : samplers)
        {
            result.put(sampler, cfsProxy.finishLocalSampling(sampler.name(), count));
        }
        return result;
    }

    public void invalidateCounterCache()
    {
        cacheService.invalidateCounterCache();
    }

    public void invalidateKeyCache()
    {
        cacheService.invalidateKeyCache();
    }

    public void invalidateRowCache()
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

    public double[] getAndResetGCStats()
    {
        return gcProxy.getAndResetStats();
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
     * Take a snapshot of all the keyspaces, optionally specifying only a specific column family.
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
                throw new IOException("When specifying the table for a snapshot, you must specify one and only one keyspace");
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

    public Map<String, TabularData> getSnapshotDetails()
    {
        return ssProxy.getSnapshotDetails();
    }

    public long trueSnapshotsSize()
    {
        return ssProxy.trueSnapshotsSize();
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

    public void move(String newToken) throws IOException
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

    public void assassinateEndpoint(String address) throws UnknownHostException
    {
        gossProxy.assassinateEndpoint(address);
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

    public void disableAutoCompaction(String ks, String ... columnFamilies) throws IOException
    {
        ssProxy.disableAutoCompaction(ks, columnFamilies);
    }

    public void enableAutoCompaction(String ks, String ... columnFamilies) throws IOException
    {
        ssProxy.enableAutoCompaction(ks, columnFamilies);
    }

    public void setIncrementalBackupsEnabled(boolean enabled)
    {
        ssProxy.setIncrementalBackupsEnabled(enabled);
    }

    public boolean isIncrementalBackupsEnabled()
    {
        return ssProxy.isIncrementalBackupsEnabled();
    }

    public void setCacheCapacities(int keyCacheCapacity, int rowCacheCapacity, int counterCacheCapacity)
    {
        try
        {
            String keyCachePath = "org.apache.cassandra.db:type=Caches";
            CacheServiceMBean cacheMBean = JMX.newMBeanProxy(mbeanServerConn, new ObjectName(keyCachePath), CacheServiceMBean.class);
            cacheMBean.setKeyCacheCapacityInMB(keyCacheCapacity);
            cacheMBean.setRowCacheCapacityInMB(rowCacheCapacity);
            cacheMBean.setCounterCacheCapacityInMB(counterCacheCapacity);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void setCacheKeysToSave(int keyCacheKeysToSave, int rowCacheKeysToSave, int counterCacheKeysToSave)
    {
        try
        {
            String keyCachePath = "org.apache.cassandra.db:type=Caches";
            CacheServiceMBean cacheMBean = JMX.newMBeanProxy(mbeanServerConn, new ObjectName(keyCachePath), CacheServiceMBean.class);
            cacheMBean.setKeyCacheKeysToSave(keyCacheKeysToSave);
            cacheMBean.setRowCacheKeysToSave(rowCacheKeysToSave);
            cacheMBean.setCounterCacheKeysToSave(counterCacheKeysToSave);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        ssProxy.setHintedHandoffThrottleInKB(throttleInKB);
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

    public Set<StreamState> getStreamStatus()
    {
        return Sets.newHashSet(Iterables.transform(streamProxy.getCurrentStreams(), new Function<CompositeData, StreamState>()
        {
            public StreamState apply(CompositeData input)
            {
                return StreamStateCompositeData.fromCompositeData(input);
            }
        }));
    }

    public String getOperationMode()
    {
        return ssProxy.getOperationMode();
    }

    public boolean isStarting()
    {
        return ssProxy.isStarting();
    }

    public void truncate(String keyspaceName, String cfName)
    {
        try
        {
            ssProxy.truncate(keyspaceName, cfName);
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
            if (pair.getKey().equals(stringToken))
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

    public List<String> getNonSystemKeyspaces()
    {
        return ssProxy.getNonSystemKeyspaces();
    }

    public String getClusterName()
    {
        return ssProxy.getClusterName();
    }

    public String getPartitioner()
    {
        return ssProxy.getPartitionerName();
    }

    public void disableHintedHandoff()
    {
        spProxy.setHintedHandoffEnabled(false);
    }

    public void enableHintedHandoff()
    {
        spProxy.setHintedHandoffEnabled(true);
    }

    public boolean isHandoffEnabled()
    {
        return spProxy.getHintedHandoffEnabled();
    }

    public void enableHintedHandoff(String dcNames)
    {
        spProxy.setHintedHandoffEnabledByDCList(dcNames);
    }

    public void pauseHintsDelivery()
    {
        hhProxy.pauseHintsDelivery(true);
    }

    public void resumeHintsDelivery()
    {
        hhProxy.pauseHintsDelivery(false);
    }

    public void truncateHints(final String host)
    {
        hhProxy.deleteHintsForEndpoint(host);
    }

    public void truncateHints()
    {
        try
        {
            hhProxy.truncateAllHints();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Error while executing truncate hints", e);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Error while executing truncate hints", e);
        }
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

    public boolean isGossipRunning()
    {
        return ssProxy.isGossipRunning();
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

    public void stopCassandraDaemon()
    {
        ssProxy.stopDaemon();
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

    public int getStreamThroughput()
    {
        return ssProxy.getStreamThroughputMbPerSec();
    }

    public int getExceptionCount()
    {
        return (int)StorageMetrics.exceptions.getCount();
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

    public long getReadRepairAttempted()
    {
        return spProxy.getReadRepairAttempted();
    }

    public long getReadRepairRepairedBlocking()
    {
        return spProxy.getReadRepairRepairedBlocking();
    }

    public long getReadRepairRepairedBackground()
    {
        return spProxy.getReadRepairRepairedBackground();
    }

    // JMX getters for the o.a.c.metrics API below.
    /**
     * Retrieve cache metrics based on the cache type (KeyCache, RowCache, or CounterCache)
     * @param cacheType KeyCach, RowCache, or CounterCache
     * @param metricName Capacity, Entries, HitRate, Size, Requests or Hits.
     */
    public Object getCacheMetric(String cacheType, String metricName)
    {
        try
        {
            switch(metricName)
            {
                case "Capacity":
                case "Entries":
                case "HitRate":
                case "Size":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName),
                            CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                case "Requests":
                case "Hits":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName),
                            CassandraMetricsRegistry.JmxMeterMBean.class).getCount();
                default:
                    throw new RuntimeException("Unknown cache metric name.");

            }
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Object getThreadPoolMetric(Stage stage, String metricName)
    {
        return ThreadPoolMetrics.getJmxMetric(mbeanServerConn, stage.getJmxType(), stage.getJmxName(), metricName);
    }

    /**
     * Retrieve ColumnFamily metrics
     * @param ks Keyspace for which stats are to be displayed.
     * @param cf ColumnFamily for which stats are to be displayed.
     * @param metricName View {@link org.apache.cassandra.metrics.ColumnFamilyMetrics}.
     */
    public Object getColumnFamilyMetric(String ks, String cf, String metricName)
    {
        try
        {
            String type = cf.contains(".") ? "IndexColumnFamily": "ColumnFamily";
            ObjectName oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=%s,keyspace=%s,scope=%s,name=%s", type, ks, cf, metricName));
            switch(metricName)
            {
                case "BloomFilterDiskSpaceUsed":
                case "BloomFilterFalsePositives":
                case "BloomFilterFalseRatio":
                case "BloomFilterOffHeapMemoryUsed":
                case "IndexSummaryOffHeapMemoryUsed":
                case "CompressionMetadataOffHeapMemoryUsed":
                case "CompressionRatio":
                case "EstimatedColumnCountHistogram":
                case "EstimatedRowSizeHistogram":
                case "KeyCacheHitRate":
                case "LiveSSTableCount":
                case "MaxRowSize":
                case "MeanRowSize":
                case "MemtableColumnsCount":
                case "MemtableLiveDataSize":
                case "MemtableOffHeapSize":
                case "MinRowSize":
                case "RecentBloomFilterFalsePositives":
                case "RecentBloomFilterFalseRatio":
                case "SnapshotsSize":
                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                case "LiveDiskSpaceUsed":
                case "MemtableSwitchCount":
                case "SpeculativeRetries":
                case "TotalDiskSpaceUsed":
                case "WriteTotalLatency":
                case "ReadTotalLatency":
                case "PendingFlushes":
                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxCounterMBean.class).getCount();
                case "CoordinatorReadLatency":
                case "CoordinatorScanLatency":
                case "ReadLatency":
                case "WriteLatency":
                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxTimerMBean.class);
                case "LiveScannedHistogram":
                case "SSTablesPerReadHistogram":
                case "TombstoneScannedHistogram":
                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxHistogramMBean.class);
                default:
                    throw new RuntimeException("Unknown table metric.");
            }
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve Proxy metrics
     * @param scope RangeSlice, Read or Write
     */
    public CassandraMetricsRegistry.JmxTimerMBean getProxyMetric(String scope)
    {
        try
        {
            return JMX.newMBeanProxy(mbeanServerConn,
                    new ObjectName("org.apache.cassandra.metrics:type=ClientRequest,scope=" + scope + ",name=Latency"),
                    CassandraMetricsRegistry.JmxTimerMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve Proxy metrics
     * @param metricName CompletedTasks, PendingTasks, BytesCompacted or TotalCompactionsCompleted.
     */
    public Object getCompactionMetric(String metricName)
    {
        try
        {
            switch(metricName)
            {
                case "BytesCompacted":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName),
                            CassandraMetricsRegistry.JmxCounterMBean.class);
                case "CompletedTasks":
                case "PendingTasks":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName),
                            CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                case "TotalCompactionsCompleted":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName),
                            CassandraMetricsRegistry.JmxMeterMBean.class);
                default:
                    throw new RuntimeException("Unknown compaction metric.");
            }
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve Proxy metrics
     * @param metricName Exceptions, Load, TotalHints or TotalHintsInProgress.
     */
    public long getStorageMetric(String metricName)
    {
        try
        {
            return JMX.newMBeanProxy(mbeanServerConn,
                    new ObjectName("org.apache.cassandra.metrics:type=Storage,name=" + metricName),
                    CassandraMetricsRegistry.JmxCounterMBean.class).getCount();
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxHistogramMBean metric)
    {
        return new double[]{ metric.get50thPercentile(),
                metric.get75thPercentile(),
                metric.get95thPercentile(),
                metric.get98thPercentile(),
                metric.get99thPercentile(),
                metric.getMin(),
                metric.getMax()};
    }

    public double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxTimerMBean metric)
    {
        return new double[]{ metric.get50thPercentile(),
                metric.get75thPercentile(),
                metric.get95thPercentile(),
                metric.get98thPercentile(),
                metric.get99thPercentile(),
                metric.getMin(),
                metric.getMax()};
    }

    public TabularData getCompactionHistory()
    {
        return compactionProxy.getCompactionHistory();
    }

    public void reloadTriggers()
    {
        spProxy.reloadTriggerClasses();
    }

    public void setLoggingLevel(String classQualifier, String level)
    {
        try
        {
            ssProxy.setLoggingLevel(classQualifier, level);
        }
        catch (Exception e)
        {
          throw new RuntimeException("Error setting log for " + classQualifier +" on level " + level +". Please check logback configuration and ensure to have <jmxConfigurator /> set", e); 
        }
    }

    public Map<String, String> getLoggingLevels()
    {
        return ssProxy.getLoggingLevels();
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
                int keyspaceNameCmp = e1.getKey().compareTo(e2.getKey());
                if(keyspaceNameCmp != 0)
                    return keyspaceNameCmp;

                // get CF name and split it for index name
                String e1CF[] = e1.getValue().getColumnFamilyName().split("\\.");
                String e2CF[] = e2.getValue().getColumnFamilyName().split("\\.");
                assert e1CF.length <= 2 && e2CF.length <= 2 : "unexpected split count for table name";

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
            String keyspaceName = n.getKeyProperty("keyspace");
            ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mbeanServerConn, n, ColumnFamilyStoreMBean.class);
            mbeans.add(new AbstractMap.SimpleImmutableEntry<String, ColumnFamilyStoreMBean>(keyspaceName, cfsProxy));
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
