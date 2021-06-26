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
import java.rmi.ConnectException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.fql.FullQueryLoggerOptionsCompositeData;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.hints.HintsServiceMBean;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.GCInspectorMXBean;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;

import com.codahale.metrics.JmxReporter;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.tools.nodetool.GetTimeout;
import org.apache.cassandra.utils.NativeLibrary;

/**
 * JMX client operations for Cassandra.
 */
public class NodeProbe implements AutoCloseable
{
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    private static final int defaultPort = 7199;

    static long JMX_NOTIFICATION_POLL_INTERVAL_SECONDS = Long.getLong("cassandra.nodetool.jmx_notification_poll_interval_seconds", TimeUnit.SECONDS.convert(5, TimeUnit.MINUTES));

    final String host;
    final int port;
    private String username;
    private String password;


    protected JMXConnector jmxc;
    protected MBeanServerConnection mbeanServerConn;
    protected CompactionManagerMBean compactionProxy;
    protected StorageServiceMBean ssProxy;
    protected GossiperMBean gossProxy;
    protected MemoryMXBean memProxy;
    protected GCInspectorMXBean gcProxy;
    protected RuntimeMXBean runtimeProxy;
    protected StreamManagerMBean streamProxy;
    protected MessagingServiceMBean msProxy;
    protected FailureDetectorMBean fdProxy;
    protected CacheServiceMBean cacheService;
    protected StorageProxyMBean spProxy;
    protected HintsServiceMBean hsProxy;
    protected BatchlogManagerMBean bmProxy;
    protected ActiveRepairServiceMBean arsProxy;
    protected Output output;
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
        this.output = Output.CONSOLE;
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
        this.output = Output.CONSOLE;
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
        this.output = Output.CONSOLE;
        connect();
    }

    protected NodeProbe()
    {
        // this constructor is only used for extensions to rewrite their own connect method
        this.host = "";
        this.port = 0;
        this.output = Output.CONSOLE;
    }

    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     *
     * @throws IOException on connection failures
     */
    protected void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
        Map<String, Object> env = new HashMap<String, Object>();
        if (username != null)
        {
            String[] creds = { username, password };
            env.put(JMXConnector.CREDENTIALS, creds);
        }

        env.put("com.sun.jndi.rmi.factory.socket", getRMIClientSocketFactory());

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
            name = new ObjectName(HintsService.MBEAN_NAME);
            hsProxy = JMX.newMBeanProxy(mbeanServerConn, name, HintsServiceMBean.class);
            name = new ObjectName(GCInspector.MBEAN_NAME);
            gcProxy = JMX.newMBeanProxy(mbeanServerConn, name, GCInspectorMXBean.class);
            name = new ObjectName(Gossiper.MBEAN_NAME);
            gossProxy = JMX.newMBeanProxy(mbeanServerConn, name, GossiperMBean.class);
            name = new ObjectName(BatchlogManager.MBEAN_NAME);
            bmProxy = JMX.newMBeanProxy(mbeanServerConn, name, BatchlogManagerMBean.class);
            name = new ObjectName(ActiveRepairServiceMBean.MBEAN_NAME);
            arsProxy = JMX.newMBeanProxy(mbeanServerConn, name, ActiveRepairServiceMBean.class);
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

    private RMIClientSocketFactory getRMIClientSocketFactory()
    {
        if (Boolean.parseBoolean(System.getProperty("ssl.enable")))
            return new SslRMIClientSocketFactory();
        else
            return RMISocketFactory.getDefaultSocketFactory();
    }

    public void close() throws IOException
    {
        try
        {
            jmxc.close();
        }
        catch (ConnectException e)
        {
            // result of 'stopdaemon' command - i.e. if close() call fails, the daemon is shutdown
            System.out.println("Cassandra has shutdown.");
        }
    }

    public void setOutput(Output output)
    {
        this.output = output;
    }

    public Output output()
    {
        return output;
    }

    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.forceKeyspaceCleanup(jobs, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.scrub(disableSnapshot, skipCorrupted, checkData, reinsertOverflowedTTL, jobs, keyspaceName, tables);
    }

    public int verify(boolean extendedVerify, boolean checkVersion, boolean diskFailurePolicy, boolean mutateRepairStatus, boolean checkOwnsTokens, boolean quick, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.verify(extendedVerify, checkVersion, diskFailurePolicy, mutateRepairStatus, checkOwnsTokens, quick, keyspaceName, tableNames);
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.upgradeSSTables(keyspaceName, excludeCurrentVersion, jobs, tableNames);
    }

    public int garbageCollect(String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return ssProxy.garbageCollect(tombstoneOption, jobs, keyspaceName, tableNames);
    }

    private void checkJobs(PrintStream out, int jobs)
    {
        // TODO this should get the configured number of concurrent_compactors via JMX and not using DatabaseDescriptor
        DatabaseDescriptor.toolInitialization(false); // if running in dtest, this would fail if true (default)
        if (jobs > DatabaseDescriptor.getConcurrentCompactors())
            out.println(String.format("jobs (%d) is bigger than configured concurrent_compactors (%d) on this host, using at most %d threads", jobs, DatabaseDescriptor.getConcurrentCompactors(), DatabaseDescriptor.getConcurrentCompactors()));
    }

    public void forceKeyspaceCleanup(PrintStream out, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        checkJobs(out, jobs);
        switch (forceKeyspaceCleanup(jobs, keyspaceName, tableNames))
        {
            case 1:
                failed = true;
                out.println("Aborted cleaning up at least one table in keyspace "+keyspaceName+", check server logs for more information.");
                break;
            case 2:
                failed = true;
                out.println("Failed marking some sstables compacting in keyspace "+keyspaceName+", check server logs for more information");
                break;
        }
    }

    public void scrub(PrintStream out, boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        checkJobs(out, jobs);
        switch (ssProxy.scrub(disableSnapshot, skipCorrupted, checkData, reinsertOverflowedTTL, jobs, keyspaceName, tables))
        {
            case 1:
                failed = true;
                out.println("Aborted scrubbing at least one table in keyspace "+keyspaceName+", check server logs for more information.");
                break;
            case 2:
                failed = true;
                out.println("Failed marking some sstables compacting in keyspace "+keyspaceName+", check server logs for more information");
                break;
        }
    }

    public void verify(PrintStream out, boolean extendedVerify, boolean checkVersion, boolean diskFailurePolicy, boolean mutateRepairStatus, boolean checkOwnsTokens, boolean quick, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        switch (verify(extendedVerify, checkVersion, diskFailurePolicy, mutateRepairStatus, checkOwnsTokens, quick, keyspaceName, tableNames))
        {
            case 1:
                failed = true;
                out.println("Aborted verifying at least one table in keyspace "+keyspaceName+", check server logs for more information.");
                break;
            case 2:
                failed = true;
                out.println("Failed marking some sstables compacting in keyspace "+keyspaceName+", check server logs for more information");
                break;
        }
    }


    public void upgradeSSTables(PrintStream out, String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        checkJobs(out, jobs);
        switch (upgradeSSTables(keyspaceName, excludeCurrentVersion, jobs, tableNames))
        {
            case 1:
                failed = true;
                out.println("Aborted upgrading sstables for at least one table in keyspace " + keyspaceName + ", check server logs for more information.");
                break;
            case 2:
                failed = true;
                out.println("Failed marking some sstables compacting in keyspace "+keyspaceName+", check server logs for more information");
                break;
        }
    }

    public void garbageCollect(PrintStream out, String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        if (garbageCollect(tombstoneOption, jobs, keyspaceName, tableNames) != 0)
        {
            failed = true;
            out.println("Aborted garbage collection for at least one table in keyspace " + keyspaceName + ", check server logs for more information.");
        }
    }

    public void forceUserDefinedCompaction(String datafiles) throws IOException, ExecutionException, InterruptedException
    {
        compactionProxy.forceUserDefinedCompaction(datafiles);
    }

    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceKeyspaceCompaction(splitOutput, keyspaceName, tableNames);
    }

    public void relocateSSTables(int jobs, String keyspace, String[] cfnames) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.relocateSSTables(jobs, keyspace, cfnames);
    }

    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, final String startToken, final String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceKeyspaceCompactionForTokenRange(keyspaceName, startToken, endToken, tableNames);
    }

    public void forceKeyspaceFlush(String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        ssProxy.forceKeyspaceFlush(keyspaceName, tableNames);
    }

    public String getKeyspaceReplicationInfo(String keyspaceName)
    {
        return ssProxy.getKeyspaceReplicationInfo(keyspaceName);
    }

    public void repairAsync(final PrintStream out, final String keyspace, Map<String, String> options) throws IOException
    {
        RepairRunner runner = new RepairRunner(out, ssProxy, keyspace, options);
        try
        {
            if (jmxc != null)
                jmxc.addConnectionNotificationListener(runner, null, null);
            ssProxy.addNotificationListener(runner, null, null);
            runner.run();
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
        finally
        {
            try
            {
                ssProxy.removeNotificationListener(runner);
                if (jmxc != null)
                    jmxc.removeConnectionNotificationListener(runner);
            }
            catch (Throwable e)
            {
                out.println("Exception occurred during clean-up. " + e);
            }
        }
    }
    public Map<String, List<CompositeData>> getPartitionSample(int capacity, int durationMillis, int count, List<String> samplers) throws OpenDataException
    {
        return ssProxy.samplePartitions(durationMillis, capacity, count, samplers);
    }

    public Map<String, List<CompositeData>> getPartitionSample(String ks, String cf, int capacity, int durationMillis, int count, List<String> samplers) throws OpenDataException
    {
        ColumnFamilyStoreMBean cfsProxy = getCfsProxy(ks, cf);
        for(String sampler : samplers)
        {
            cfsProxy.beginLocalSampling(sampler, capacity, durationMillis);
        }
        Uninterruptibles.sleepUninterruptibly(durationMillis, TimeUnit.MILLISECONDS);
        Map<String, List<CompositeData>> result = Maps.newHashMap();
        for(String sampler : samplers)
        {
            result.put(sampler, cfsProxy.finishLocalSampling(sampler, count));
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

    public Map<String, String> getTokenToEndpointMap(boolean withPort)
    {
        return withPort ? ssProxy.getTokenToEndpointWithPortMap() : ssProxy.getTokenToEndpointMap();
    }

    public List<String> getLiveNodes(boolean withPort)
    {
        return withPort ? ssProxy.getLiveNodesWithPort() : ssProxy.getLiveNodes();
    }

    public List<String> getJoiningNodes(boolean withPort)
    {
        return withPort ? ssProxy.getJoiningNodesWithPort() : ssProxy.getJoiningNodes();
    }

    public List<String> getLeavingNodes(boolean withPort)
    {
        return withPort ? ssProxy.getLeavingNodesWithPort() : ssProxy.getLeavingNodes();
    }

    public List<String> getMovingNodes(boolean withPort)
    {
        return withPort ? ssProxy.getMovingNodesWithPort() : ssProxy.getMovingNodes();
    }

    public List<String> getUnreachableNodes(boolean withPort)
    {
        return withPort ? ssProxy.getUnreachableNodesWithPort() : ssProxy.getUnreachableNodes();
    }

    public Map<String, String> getLoadMap(boolean withPort)
    {
        return withPort ? ssProxy.getLoadMapWithPort() : ssProxy.getLoadMap();
    }

    public Map<InetAddress, Float> getOwnership()
    {
        return ssProxy.getOwnership();
    }

    public Map<String, Float> getOwnershipWithPort()
    {
        return ssProxy.getOwnershipWithPort();
    }

    public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException
    {
        return ssProxy.effectiveOwnership(keyspace);
    }

    public Map<String, Float> effectiveOwnershipWithPort(String keyspace) throws IllegalStateException
    {
        return ssProxy.effectiveOwnershipWithPort(keyspace);
    }

    public MBeanServerConnection getMbeanServerConn()
    {
        return mbeanServerConn;
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

    public Map<String, String> getHostIdMap(boolean withPort)
    {
        return withPort ? ssProxy.getEndpointWithPortToHostId() : ssProxy.getEndpointToHostId();
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

    public long getSnapshotLinksPerSecond()
    {
        return ssProxy.getSnapshotLinksPerSecond();
    }

    public void setSnapshotLinksPerSecond(long throttle)
    {
        ssProxy.setSnapshotLinksPerSecond(throttle);
    }

    /**
     * Take a snapshot of all the keyspaces, optionally specifying only a specific column family.
     *
     * @param snapshotName the name of the snapshot.
     * @param table the table to snapshot or all on null
     * @param options Options (skipFlush for now)
     * @param keyspaces the keyspaces to snapshot
     */
    public void takeSnapshot(String snapshotName, String table, Map<String, String> options, String... keyspaces) throws IOException
    {
        if (table != null)
        {
            if (keyspaces.length != 1)
            {
                throw new IOException("When specifying the table for a snapshot, you must specify one and only one keyspace");
            }

            ssProxy.takeSnapshot(snapshotName, options, keyspaces[0] + "." + table);
        }
        else
            ssProxy.takeSnapshot(snapshotName, options, keyspaces);
    }

    /**
     * Take a snapshot of all column family from different keyspaces.
     *
     * @param snapshotName
     *            the name of the snapshot.
     * @param options
     *            Options (skipFlush for now)
     * @param tableList
     *            list of columnfamily from different keyspace in the form of ks1.cf1 ks2.cf2
     */
    public void takeMultipleTableSnapshot(String snapshotName, Map<String, String> options, String... tableList)
            throws IOException
    {
        if (null != tableList && tableList.length != 0)
        {
            ssProxy.takeSnapshot(snapshotName, options, tableList);
        }
        else
        {
            throw new IOException("The column family List  for a snapshot should not be empty or null");
        }
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

    public boolean isDrained()
    {
        return ssProxy.isDrained();
    }

    public boolean isDraining()
    {
        return ssProxy.isDraining();
    }

    public boolean isBootstrapMode()
    {
        return ssProxy.isBootstrapMode();
    }

    public void joinRing() throws IOException
    {
        ssProxy.joinRing();
    }

    public void decommission(boolean force) throws InterruptedException
    {
        ssProxy.decommission(force);
    }

    public void move(String newToken) throws IOException
    {
        ssProxy.move(newToken);
    }

    public void removeNode(String token)
    {
        ssProxy.removeNode(token);
    }

    public String getRemovalStatus(boolean withPort)
    {
        return withPort ? ssProxy.getRemovalStatusWithPort() : ssProxy.getRemovalStatus();
    }

    public void forceRemoveCompletion()
    {
        ssProxy.forceRemoveCompletion();
    }

    public void assassinateEndpoint(String address) throws UnknownHostException
    {
        gossProxy.assassinateEndpoint(address);
    }

    public List<String> reloadSeeds()
    {
        return gossProxy.reloadSeeds();
    }

    public List<String> getSeeds()
    {
        return gossProxy.getSeeds();
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

    public void disableAutoCompaction(String ks, String ... tables) throws IOException
    {
        ssProxy.disableAutoCompaction(ks, tables);
    }

    public void enableAutoCompaction(String ks, String ... tableNames) throws IOException
    {
        ssProxy.enableAutoCompaction(ks, tableNames);
    }

    public Map<String, Boolean> getAutoCompactionDisabled(String ks, String ... tableNames) throws IOException
    {
        return ssProxy.getAutoCompactionStatus(ks, tableNames);
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
        CacheServiceMBean cacheMBean = getCacheServiceMBean();
        cacheMBean.setKeyCacheCapacityInMB(keyCacheCapacity);
        cacheMBean.setRowCacheCapacityInMB(rowCacheCapacity);
        cacheMBean.setCounterCacheCapacityInMB(counterCacheCapacity);
    }

    public void setCacheKeysToSave(int keyCacheKeysToSave, int rowCacheKeysToSave, int counterCacheKeysToSave)
    {
        CacheServiceMBean cacheMBean = getCacheServiceMBean();
        cacheMBean.setKeyCacheKeysToSave(keyCacheKeysToSave);
        cacheMBean.setRowCacheKeysToSave(rowCacheKeysToSave);
        cacheMBean.setCounterCacheKeysToSave(counterCacheKeysToSave);
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        ssProxy.setHintedHandoffThrottleInKB(throttleInKB);
    }

    public List<String> getEndpointsWithPort(String keyspace, String cf, String key)
    {
        return ssProxy.getNaturalEndpointsWithPort(keyspace, cf, key);
    }

    public List<InetAddress> getEndpoints(String keyspace, String cf, String key)
    {
        return ssProxy.getNaturalEndpoints(keyspace, cf, key);
    }

    public List<String> getSSTables(String keyspace, String cf, String key, boolean hexFormat)
    {
        ColumnFamilyStoreMBean cfsProxy = getCfsProxy(keyspace, cf);
        return cfsProxy.getSSTablesForKey(key, hexFormat);
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

    public void truncate(String keyspaceName, String tableName)
    {
        try
        {
            ssProxy.truncate(keyspaceName, tableName);
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

    public DynamicEndpointSnitchMBean getDynamicEndpointSnitchInfoProxy()
    {
        try
        {
            return JMX.newMBeanProxy(mbeanServerConn, new ObjectName("org.apache.cassandra.db:type=DynamicEndpointSnitch"), DynamicEndpointSnitchMBean.class);
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

    public StorageServiceMBean getStorageService() {
        return ssProxy;
    }

    public GossiperMBean getGossProxy()
    {
        return gossProxy;
    }

    public String getEndpoint()
    {
        Map<String, String> hostIdToEndpoint = ssProxy.getHostIdToEndpoint();
        return hostIdToEndpoint.get(ssProxy.getLocalHostId());
    }

    public String getDataCenter()
    {
        return getEndpointSnitchInfoProxy().getDatacenter();
    }

    public String getRack()
    {
        return getEndpointSnitchInfoProxy().getRack();
    }

    public List<String> getKeyspaces()
    {
        return ssProxy.getKeyspaces();
    }

    public List<String> getNonSystemKeyspaces()
    {
        return ssProxy.getNonSystemKeyspaces();
    }

    public List<String> getNonLocalStrategyKeyspaces()
    {
        return ssProxy.getNonLocalStrategyKeyspaces();
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

    public void enableHintsForDC(String dc)
    {
        spProxy.enableHintsForDC(dc);
    }

    public void disableHintsForDC(String dc)
    {
        spProxy.disableHintsForDC(dc);
    }

    public Set<String> getHintedHandoffDisabledDCs()
    {
        return spProxy.getHintedHandoffDisabledDCs();
    }

    public Map<String, String> getViewBuildStatuses(String keyspace, String view)
    {
        return ssProxy.getViewBuildStatuses(keyspace, view);
    }

    public void pauseHintsDelivery()
    {
        hsProxy.pauseDispatch();
    }

    public void resumeHintsDelivery()
    {
        hsProxy.resumeDispatch();
    }

    public void truncateHints(final String host)
    {
        hsProxy.deleteAllHintsForEndpoint(host);
    }

    public void truncateHints()
    {
        hsProxy.deleteAllHints();
    }

    public void refreshSizeEstimates()
    {
        try
        {
            ssProxy.refreshSizeEstimates();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Error while refreshing system.size_estimates", e);
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

    public void setBatchlogReplayThrottle(int value)
    {
        ssProxy.setBatchlogReplayThrottleInKB(value);
    }

    public int getBatchlogReplayThrottle()
    {
        return ssProxy.getBatchlogReplayThrottleInKB();
    }

    public void setConcurrentCompactors(int value)
    {
        ssProxy.setConcurrentCompactors(value);
    }

    public int getConcurrentCompactors()
    {
        return ssProxy.getConcurrentCompactors();
    }

    public void setConcurrentViewBuilders(int value)
    {
        ssProxy.setConcurrentViewBuilders(value);
    }

    public int getConcurrentViewBuilders()
    {
        return ssProxy.getConcurrentViewBuilders();
    }

    public void setMaxHintWindow(int value)
    {
        spProxy.setMaxHintWindow(value);
    }

    public int getMaxHintWindow()
    {
        return spProxy.getMaxHintWindow();
    }

    public long getTimeout(String type)
    {
        switch (type)
        {
            case "misc":
                return ssProxy.getRpcTimeout();
            case "read":
                return ssProxy.getReadRpcTimeout();
            case "range":
                return ssProxy.getRangeRpcTimeout();
            case "write":
                return ssProxy.getWriteRpcTimeout();
            case "counterwrite":
                return ssProxy.getCounterWriteRpcTimeout();
            case "cascontention":
                return ssProxy.getCasContentionTimeout();
            case "truncate":
                return ssProxy.getTruncateRpcTimeout();
            case "internodeconnect":
                return ssProxy.getInternodeTcpConnectTimeoutInMS();
            case "internodeuser":
                return ssProxy.getInternodeTcpUserTimeoutInMS();
            case "internodestreaminguser":
                return ssProxy.getInternodeStreamingTcpUserTimeoutInMS();
            default:
                throw new RuntimeException("Timeout type requires one of (" + GetTimeout.TIMEOUT_TYPES + ")");
        }
    }

    public int getStreamThroughput()
    {
        return ssProxy.getStreamThroughputMbPerSec();
    }

    public int getInterDCStreamThroughput()
    {
        return ssProxy.getInterDCStreamThroughputMbPerSec();
    }

    public double getTraceProbability()
    {
        return ssProxy.getTraceProbability();
    }

    public int getExceptionCount()
    {
        return (int)StorageMetrics.uncaughtExceptions.getCount();
    }

    public Map<String, Integer> getDroppedMessages()
    {
        return msProxy.getDroppedMessages();
    }

    @Deprecated
    public void loadNewSSTables(String ksName, String cfName)
    {
        ssProxy.loadNewSSTables(ksName, cfName);
    }

    public List<String> importNewSSTables(String ksName, String cfName, Set<String> srcPaths, boolean resetLevel, boolean clearRepaired, boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches, boolean extendedVerify, boolean copyData)
    {
        return getCfsProxy(ksName, cfName).importNewSSTables(srcPaths, resetLevel, clearRepaired, verifySSTables, verifyTokens, invalidateCaches, extendedVerify, copyData);
    }

    public void rebuildIndex(String ksName, String cfName, String... idxNames)
    {
        ssProxy.rebuildSecondaryIndex(ksName, cfName, idxNames);
    }

    public String getGossipInfo(boolean withPort)
    {
        return withPort ? fdProxy.getAllEndpointStatesWithPort() : fdProxy.getAllEndpointStates();
    }

    public void stop(String string)
    {
        compactionProxy.stopCompaction(string);
    }

    public void setTimeout(String type, long value)
    {
        if (value < 0)
            throw new RuntimeException("timeout must be non-negative");

        switch (type)
        {
            case "misc":
                ssProxy.setRpcTimeout(value);
                break;
            case "read":
                ssProxy.setReadRpcTimeout(value);
                break;
            case "range":
                ssProxy.setRangeRpcTimeout(value);
                break;
            case "write":
                ssProxy.setWriteRpcTimeout(value);
                break;
            case "counterwrite":
                ssProxy.setCounterWriteRpcTimeout(value);
                break;
            case "cascontention":
                ssProxy.setCasContentionTimeout(value);
                break;
            case "truncate":
                ssProxy.setTruncateRpcTimeout(value);
                break;
            case "internodeconnect":
                ssProxy.setInternodeTcpConnectTimeoutInMS((int) value);
                break;
            case "internodeuser":
                ssProxy.setInternodeTcpUserTimeoutInMS((int) value);
                break;
            case "internodestreaminguser":
                ssProxy.setInternodeStreamingTcpUserTimeoutInMS((int) value);
                break;
            default:
                throw new RuntimeException("Timeout type requires one of (" + GetTimeout.TIMEOUT_TYPES + ")");
        }
    }

    public void stopById(String compactionId)
    {
        compactionProxy.stopCompactionById(compactionId);
    }

    public void setStreamThroughput(int value)
    {
        ssProxy.setStreamThroughputMbPerSec(value);
    }

    public void setInterDCStreamThroughput(int value)
    {
        ssProxy.setInterDCStreamThroughputMbPerSec(value);
    }

    public void setTraceProbability(double value)
    {
        ssProxy.setTraceProbability(value);
    }

    public String getSchemaVersion()
    {
        return ssProxy.getSchemaVersion();
    }

    public List<String> describeRing(String keyspaceName, boolean withPort) throws IOException
    {
        return withPort ? ssProxy.describeRingWithPortJMX(keyspaceName) : ssProxy.describeRingJMX(keyspaceName);
    }

    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources)
    {
        ssProxy.rebuild(sourceDc, keyspace, tokens, specificSources);
    }

    public List<String> sampleKeyRange()
    {
        return ssProxy.sampleKeyRange();
    }

    public void resetLocalSchema() throws IOException
    {
        ssProxy.resetLocalSchema();
    }

    public void reloadLocalSchema()
    {
        ssProxy.reloadLocalSchema();
    }

    public boolean isFailed()
    {
        return failed;
    }

    public void failed()
    {
        this.failed = true;
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
                case "Misses":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName),
                            CassandraMetricsRegistry.JmxMeterMBean.class).getCount();
                case "MissLatency":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName),
                            CassandraMetricsRegistry.JmxTimerMBean.class).getMean();
                case "MissLatencyUnit":
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=MissLatency"),
                            CassandraMetricsRegistry.JmxTimerMBean.class).getDurationUnit();
                default:
                    throw new RuntimeException("Unknown cache metric name.");

            }
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Multimap<String, String> getJmxThreadPools(MBeanServerConnection mbeanServerConn)
    {
        try
        {
            Multimap<String, String> threadPools = HashMultimap.create();

            Set<ObjectName> threadPoolObjectNames = mbeanServerConn.queryNames(
                    new ObjectName("org.apache.cassandra.metrics:type=ThreadPools,*"),
                    null);

            for (ObjectName oName : threadPoolObjectNames)
            {
                threadPools.put(oName.getKeyProperty("path"), oName.getKeyProperty("scope"));
            }

            return threadPools;
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException("Bad query to JMX server: ", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error getting threadpool names from JMX", e);
        }
    }

    public Object getThreadPoolMetric(String pathName, String poolName, String metricName)
    {
      String name = String.format("org.apache.cassandra.metrics:type=ThreadPools,path=%s,scope=%s,name=%s",
              pathName, poolName, metricName);

      try
      {
          ObjectName oName = new ObjectName(name);
          if (!mbeanServerConn.isRegistered(oName))
          {
              return "N/A";
          }

          switch (metricName)
          {
              case ThreadPoolMetrics.ACTIVE_TASKS:
              case ThreadPoolMetrics.PENDING_TASKS:
              case ThreadPoolMetrics.COMPLETED_TASKS:
              case ThreadPoolMetrics.MAX_POOL_SIZE:
                  return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.JmxGaugeMBean.class).getValue();
              case ThreadPoolMetrics.TOTAL_BLOCKED_TASKS:
              case ThreadPoolMetrics.CURRENTLY_BLOCKED_TASKS:
                  return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.JmxCounterMBean.class).getCount();
              default:
                  throw new AssertionError("Unknown metric name " + metricName);
          }
      }
      catch (Exception e)
      {
          throw new RuntimeException("Error reading: " + name, e);
      }
    }

    /**
     * Retrieve threadpool paths and names for threadpools with metrics.
     * @return Multimap from path (internal, request, etc.) to name
     */
    public Multimap<String, String> getThreadPools()
    {
        return getJmxThreadPools(mbeanServerConn);
    }

    public int getNumberOfTables()
    {
        return spProxy.getNumberOfTables();
    }

    /**
     * Retrieve ColumnFamily metrics
     * @param ks Keyspace for which stats are to be displayed or null for the global value
     * @param cf ColumnFamily for which stats are to be displayed or null for the keyspace value (if ks supplied)
     * @param metricName View {@link TableMetrics}.
     */
    public Object getColumnFamilyMetric(String ks, String cf, String metricName)
    {
        try
        {
            ObjectName oName = null;
            if (!Strings.isNullOrEmpty(ks) && !Strings.isNullOrEmpty(cf))
            {
                String type = cf.contains(".") ? "IndexTable" : "Table";
                oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=%s,keyspace=%s,scope=%s,name=%s", type, ks, cf, metricName));
            }
            else if (!Strings.isNullOrEmpty(ks))
            {
                oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=%s", ks, metricName));
            }
            else
            {
                oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=Table,name=%s", metricName));
            }
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
                case "EstimatedPartitionSizeHistogram":
                case "EstimatedPartitionCount":
                case "KeyCacheHitRate":
                case "LiveSSTableCount":
                case "OldVersionSSTableCount":
                case "MaxPartitionSize":
                case "MeanPartitionSize":
                case "MemtableColumnsCount":
                case "MemtableLiveDataSize":
                case "MemtableOffHeapSize":
                case "MinPartitionSize":
                case "PercentRepaired":
                case "BytesRepaired":
                case "BytesUnrepaired":
                case "BytesPendingRepair":
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
                case "DroppedMutations":
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
                    throw new RuntimeException("Unknown table metric " + metricName);
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

    public CassandraMetricsRegistry.JmxTimerMBean getMessagingQueueWaitMetrics(String verb)
    {
        try
        {
            return JMX.newMBeanProxy(mbeanServerConn,
                                     new ObjectName("org.apache.cassandra.metrics:name=" + verb + "-WaitLatency,type=Messaging"),
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
                case "PendingTasksByTableName":
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
     * @param metricName
     */
    public Object getClientMetric(String metricName)
    {
        try
        {
            switch(metricName)
            {
                case "connections": // List<Map<String,String>> - list of all native connections and their properties
                case "connectedNativeClients": // number of connected native clients
                case "connectedNativeClientsByUser": // number of native clients by username
                case "clientsByProtocolVersion": // number of native clients by username
                    return JMX.newMBeanProxy(mbeanServerConn,
                            new ObjectName("org.apache.cassandra.metrics:type=Client,name=" + metricName),
                            CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                default:
                    throw new RuntimeException("Unknown client metric " + metricName);
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

    public Double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxHistogramMBean metric)
    {
        return new Double[]{ metric.get50thPercentile(),
                Double.valueOf(metric.get75thPercentile()),
                Double.valueOf(metric.get95thPercentile()),
                Double.valueOf(metric.get98thPercentile()),
                Double.valueOf(metric.get99thPercentile()),
                Double.valueOf(metric.getMin()),
                Double.valueOf(metric.getMax())};
    }

    public Double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxTimerMBean metric)
    {
        return new Double[]{ Double.valueOf(metric.get50thPercentile()),
                             Double.valueOf(metric.get75thPercentile()),
                             Double.valueOf(metric.get95thPercentile()),
                             Double.valueOf(metric.get98thPercentile()),
                             Double.valueOf(metric.get99thPercentile()),
                             Double.valueOf(metric.getMin()),
                             Double.valueOf(metric.getMax())};
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
            throw new RuntimeException("Error setting log for " + classQualifier + " on level " + level + ". Please check logback configuration and ensure to have <jmxConfigurator /> set", e);
        }
    }

    public Map<String, String> getLoggingLevels()
    {
        return ssProxy.getLoggingLevels();
    }

    public long getPid()
    {
        return NativeLibrary.getProcessID();
    }

    public void resumeBootstrap(PrintStream out) throws IOException
    {
        BootstrapMonitor monitor = new BootstrapMonitor(out);
        try
        {
            if (jmxc != null)
                jmxc.addConnectionNotificationListener(monitor, null, null);
            ssProxy.addNotificationListener(monitor, null, null);
            if (ssProxy.resumeBootstrap())
            {
                out.println("Resuming bootstrap");
                monitor.awaitCompletion();
            }
            else
            {
                out.println("Node is already bootstrapped.");
            }
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
        finally
        {
            try
            {
                ssProxy.removeNotificationListener(monitor);
                if (jmxc != null)
                    jmxc.removeConnectionNotificationListener(monitor);
            }
            catch (Throwable e)
            {
                out.println("Exception occurred during clean-up. " + e);
            }
        }
    }

    public Map<String, List<Integer>> getMaximumPoolSizes(List<String> stageNames)
    {
        return ssProxy.getConcurrency(stageNames);
    }

    public void setConcurrency(String stageName, int coreThreads, int maxConcurrency)
    {
        ssProxy.setConcurrency(stageName, coreThreads, maxConcurrency);
    }

    public void replayBatchlog() throws IOException
    {
        try
        {
            bmProxy.forceBatchlogReplay();
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }

    public TabularData getFailureDetectorPhilValues(boolean withPort)
    {
        try
        {
            return withPort ? fdProxy.getPhiValuesWithPort() : fdProxy.getPhiValues();
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ActiveRepairServiceMBean getRepairServiceProxy()
    {
        return arsProxy;
    }

    public void reloadSslCerts() throws IOException
    {
        msProxy.reloadSslCertificates();
    }

    public void clearConnectionHistory()
    {
        ssProxy.clearConnectionHistory();
    }

    public void disableAuditLog()
    {
        ssProxy.disableAuditLog();
    }

    public void enableAuditLog(String loggerName, Map<String, String> parameters, String includedKeyspaces ,String excludedKeyspaces ,String includedCategories ,String excludedCategories ,String includedUsers ,String excludedUsers)
    {
        ssProxy.enableAuditLog(loggerName, parameters, includedKeyspaces, excludedKeyspaces, includedCategories, excludedCategories, includedUsers, excludedUsers);
    }

    public void enableAuditLog(String loggerName, String includedKeyspaces ,String excludedKeyspaces ,String includedCategories ,String excludedCategories ,String includedUsers ,String excludedUsers)
    {
        this.enableAuditLog(loggerName, Collections.emptyMap(), includedKeyspaces, excludedKeyspaces, includedCategories, excludedCategories, includedUsers, excludedUsers);
    }

    public void enableOldProtocolVersions()
    {
        ssProxy.enableNativeTransportOldProtocolVersions();
    }

    public void disableOldProtocolVersions()
    {
        ssProxy.disableNativeTransportOldProtocolVersions();
	}

    public MessagingServiceMBean getMessagingServiceProxy()
    {
        return msProxy;
    }

    public void enableFullQueryLogger(String path, String rollCycle, Boolean blocking, int maxQueueWeight, long maxLogSize, @Nullable String archiveCommand, int maxArchiveRetries)
    {
        ssProxy.enableFullQueryLogger(path, rollCycle, blocking, maxQueueWeight, maxLogSize, archiveCommand, maxArchiveRetries);
    }

    public void stopFullQueryLogger()
    {
        ssProxy.stopFullQueryLogger();
    }

    public void resetFullQueryLogger()
    {
        ssProxy.resetFullQueryLogger();
    }

    public FullQueryLoggerOptions getFullQueryLoggerOptions()
    {
        return FullQueryLoggerOptionsCompositeData.fromCompositeData(ssProxy.getFullQueryLoggerOptions());
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
                String e1CF[] = e1.getValue().getTableName().split("\\.");
                String e2CF[] = e2.getValue().getTableName().split("\\.");
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
