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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.rmi.ConnectException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.audit.AuditLogManagerMBean;
import org.apache.cassandra.auth.AuthCache;
import org.apache.cassandra.auth.CIDRGroupsMappingManager;
import org.apache.cassandra.auth.CIDRGroupsMappingManagerMBean;
import org.apache.cassandra.auth.CIDRPermissionsManager;
import org.apache.cassandra.auth.CIDRPermissionsManagerMBean;
import org.apache.cassandra.auth.NetworkPermissionsCache;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCache;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.RolesCache;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compression.CompressionDictionaryManagerMBean;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.db.virtual.CIDRFilteringMetricsTable;
import org.apache.cassandra.db.virtual.CIDRFilteringMetricsTableMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.hints.HintsServiceMBean;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.LocationInfoMBean;
import org.apache.cassandra.management.MBeanAccessor;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.profiler.AsyncProfilerMBean;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.AutoRepairServiceMBean;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.GCInspectorMXBean;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.service.accord.AccordOperations;
import org.apache.cassandra.service.accord.AccordOperationsMBean;
import org.apache.cassandra.service.snapshot.SnapshotManagerMBean;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.CMSOperationsMBean;
import org.apache.cassandra.tools.nodetool.strategy.NodetoolConnectionException;

import static org.apache.cassandra.config.CassandraRelevantProperties.SSL_ENABLE;

public class RemoteJmxMBeanAccessor implements MBeanAccessor
{
    private final Map<Class<?>, Object> clazzMBanRegistry = new HashMap<>();
    private final Map<String, Object> namedMBeanRegistry = new ConcurrentHashMap<>();

    public static final int defaultPort = 7199;

    private static final Logger logger = LoggerFactory.getLogger(RemoteJmxMBeanAccessor.class);
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    final String host;
    final int port;
    private String username;
    private String password;

    protected JMXConnector jmxc;
    protected MBeanServerConnection mbeanServerConn;

    private volatile boolean connected = false;

    /**
     * Creates a NodeProbe using the specified JMX host, port, username, and password.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     */
    public RemoteJmxMBeanAccessor(String host, int port, String username, String password)
    {
        assert username != null && !username.isEmpty() && password != null && !password.isEmpty()
        : "neither username nor password can be blank";

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    /**
     * Creates a NodeProbe using the specified JMX host and port.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     */
    public RemoteJmxMBeanAccessor(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    /**
     * Creates a NodeProbe using the specified JMX host and default port.
     *
     * @param host hostname or IP address of the JMX agent
     */
    public RemoteJmxMBeanAccessor(String host)
    {
        this(host, defaultPort);
    }

    /**
     * Create a connection to the JMX agent and set up the M[X]Bean proxies.
     */
    protected void connect()
    {
        if (connected)
            return;

        synchronized (this)
        {
            if (connected)
                return;

            try
            {
                String host = this.host;
                if (host.contains(":"))
                {
                    // Use square brackets to surround IPv6 addresses to fix CASSANDRA-7669 and CASSANDRA-17581
                    host = '[' + host + ']';
                }
                JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
                Map<String, Object> env = new HashMap<>();
                if (username != null)
                {
                    String[] creds = { username, password };
                    env.put(JMXConnector.CREDENTIALS, creds);
                }

                env.put("com.sun.jndi.rmi.factory.socket", getRMIClientSocketFactory());

                jmxc = JMXConnectorFactory.connect(jmxUrl, env);
                mbeanServerConn = jmxc.getMBeanServerConnection();

                registerMBeanProxy(StorageServiceMBean.class, "org.apache.cassandra.db:type=StorageService");
                registerMBeanProxy(SnapshotManagerMBean.class, SnapshotManagerMBean.MBEAN_NAME);
                registerMBeanProxy(CMSOperationsMBean.class, CMSOperations.MBEAN_OBJECT_NAME);
                registerMBeanProxy(AccordOperationsMBean.class, AccordOperations.MBEAN_OBJECT_NAME);
                registerMBeanProxy(MessagingServiceMBean.class, MessagingService.MBEAN_NAME);
                registerMBeanProxy(StreamManagerMBean.class, StreamManagerMBean.OBJECT_NAME);
                registerMBeanProxy(CompactionManagerMBean.class, CompactionManager.MBEAN_OBJECT_NAME);
                registerMBeanProxy(FailureDetectorMBean.class, FailureDetector.MBEAN_NAME);
                registerMBeanProxy(CacheServiceMBean.class, CacheService.MBEAN_NAME);
                registerMBeanProxy(StorageProxyMBean.class, StorageProxy.MBEAN_NAME);
                registerMBeanProxy(HintsServiceMBean.class, HintsService.MBEAN_NAME);
                registerMBeanProxy(GCInspectorMXBean.class, GCInspector.MBEAN_NAME);
                registerMBeanProxy(GossiperMBean.class, Gossiper.MBEAN_NAME);
                registerMBeanProxy(BatchlogManagerMBean.class, BatchlogManager.MBEAN_NAME);
                registerMBeanProxy(ActiveRepairServiceMBean.class, ActiveRepairServiceMBean.MBEAN_NAME);
                registerMBeanProxy(AuditLogManagerMBean.class, AuditLogManager.MBEAN_NAME);
                registerMBeanProxy(PasswordAuthenticator.CredentialsCacheMBean.class,
                                   AuthCache.MBEAN_NAME_BASE + PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME);
                registerMBeanProxy(AuthorizationProxy.JmxPermissionsCacheMBean.class,
                                   AuthCache.MBEAN_NAME_BASE + AuthorizationProxy.JmxPermissionsCacheMBean.CACHE_NAME);
                registerMBeanProxy(NetworkPermissionsCacheMBean.class,
                                   AuthCache.MBEAN_NAME_BASE + NetworkPermissionsCache.CACHE_NAME);
                registerMBeanProxy(PermissionsCacheMBean.class,
                                   AuthCache.MBEAN_NAME_BASE + PermissionsCache.CACHE_NAME);
                registerMBeanProxy(RolesCacheMBean.class,
                                   AuthCache.MBEAN_NAME_BASE + RolesCache.CACHE_NAME);
                registerMBeanProxy(CIDRPermissionsManagerMBean.class, CIDRPermissionsManager.MBEAN_NAME);
                registerMBeanProxy(CIDRGroupsMappingManagerMBean.class, CIDRGroupsMappingManager.MBEAN_NAME);
                registerMBeanProxy(CIDRFilteringMetricsTableMBean.class, CIDRFilteringMetricsTable.MBEAN_NAME);
                registerMBeanProxy(AutoRepairServiceMBean.class, AutoRepairService.MBEAN_NAME);
                registerMBeanProxy(GuardrailsMBean.class, Guardrails.MBEAN_NAME);

                registerPlatformMBeanProxy(MemoryMXBean.class, ManagementFactory.MEMORY_MXBEAN_NAME);
                registerPlatformMBeanProxy(RuntimeMXBean.class, ManagementFactory.RUNTIME_MXBEAN_NAME);

                registerMBeanProxy(EndpointSnitchInfoMBean.class, "org.apache.cassandra.db:type=EndpointSnitchInfo");
                registerMBeanProxy(DynamicEndpointSnitchMBean.class, "org.apache.cassandra.db:type=DynamicEndpointSnitch");
                registerMBeanProxy(LocationInfoMBean.class, "org.apache.cassandra.db:type=LocationInfo");
                registerMBeanProxy(AsyncProfilerMBean.class, AsyncProfilerMBean.MBEAN_NAME);
            }
            catch (MalformedObjectNameException e)
            {
                close();
                throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", e);
            }
            catch (IOException | SecurityException e)
            {
                close();
                Throwable rootCause = Throwables.getRootCause(e);
                throw new NodetoolConnectionException(String.format("Failed to connect to '%s:%s' - %s: '%s'.",
                                                                    host, port,
                                                                    rootCause.getClass().getSimpleName(),
                                                                    rootCause.getMessage()),
                                                      e);
            }

            connected = true;
        }
    }

    protected <T> void registerMBean(Class<T> clazz, T mbean)
    {
        clazzMBanRegistry.put(clazz, mbean);
    }

    @Override
    public <T> T findMBean(Class<T> clazz)
    {
        connect();
        return clazzMBanRegistry.get(clazz) == null ? null : clazz.cast(clazzMBanRegistry.get(clazz));
    }

    @SuppressWarnings("unchecked")
    public <T> T findMBeanMetric(Class<T> clazz, Props props)
    {
        return withExceptionHandling(() -> {
            connect();
            ObjectName objectName = new ObjectName("org.apache.cassandra.metrics", new Hashtable<>(props.toMap()));
            T result = (T) namedMBeanRegistry.computeIfAbsent(objectName.getCanonicalName(),
                                                              ignore -> JMX.newMBeanProxy(mbeanServerConn, objectName, clazz));
            return clazz.cast(result);
        });
    }

    @Override
    public boolean isMBeanMetricRegistered(Props props)
    {
        return withExceptionHandling(() -> {
            connect();
            ObjectName objectName = new ObjectName("org.apache.cassandra.metrics", new Hashtable<>(props.toMap()));
            return mbeanServerConn.isRegistered(objectName);
        });
    }

    @Override
    public CassandraMetricsRegistry.JmxCounterMBean findMBeanCounter(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxCounterMBean.class, props);
    }

    @Override
    public CassandraMetricsRegistry.JmxGaugeMBean findMBeanGauge(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxGaugeMBean.class, props);
    }

    @Override
    public CassandraMetricsRegistry.JmxMeterMBean findMBeanMeter(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxMeterMBean.class, props);
    }

    @Override
    public CassandraMetricsRegistry.JmxTimerMBean findMBeanTimer(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxTimerMBean.class, props);
    }

    @Override
    public CassandraMetricsRegistry.JmxHistogramMBean findMBeanHistogram(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxHistogramMBean.class, props);
    }

    @Override
    public ColumnFamilyStoreMBean findColumnFamily(String type, String keyspace, String columnFamily)
    {
        return withExceptionHandling(() -> {
            connect();
            Set<ObjectName> beans = mbeanServerConn.queryNames(new ObjectName("org.apache.cassandra.db:type=*" + type +
                                                                              ",keyspace=" + keyspace +
                                                                              ",columnfamily=" + columnFamily), null);
            if (beans.isEmpty())
                throw new MalformedObjectNameException("couldn't find that bean");

            assert beans.size() == 1;
            return JMX.newMBeanProxy(mbeanServerConn, beans.iterator().next(), ColumnFamilyStoreMBean.class);
        });
    }

    @Override
    public CompressionDictionaryManagerMBean findCompressionDictionary(String keyspace, String table)
    {
        List<Map.Entry<String, ColumnFamilyStoreMBean>> keyspaces = findColumnFamilies("ColumnFamilies");
        Optional<ColumnFamilyStoreMBean> cfsMBean = keyspaces.stream()
                                                             .filter(e -> e.getKey().equals(keyspace))
                                                             .map(Map.Entry::getValue)
                                                             .filter(mbean -> mbean.getTableName().equals(table))
                                                             .findAny();
        if  (keyspaces.isEmpty() || cfsMBean.isEmpty())
            throw new IllegalArgumentException(String.format("Table %s.%s does not exist", keyspace, table));

        return withExceptionHandling(() -> {
            connect();
            String mbeanName = CompressionDictionaryManagerMBean.MBEAN_NAME + ",keyspace=" + keyspace + ",table=" + table;
            if (!mbeanServerConn.isRegistered(new ObjectName(mbeanName)))
                throw new IllegalStateException("The compression on table " + keyspace + '.' + table + " is not enabled or SSTable compressor is not a dictionary compressor.");
            return JMX.newMBeanProxy(mbeanServerConn, new ObjectName(mbeanName), CompressionDictionaryManagerMBean.class);
        });
    }

    @Override
    public List<ThreadPoolInfo> threadPoolInfos()
    {
        return withExceptionHandling(() -> {
            connect();
            Set<ObjectName> threadPoolObjectNames = mbeanServerConn.queryNames(new ObjectName("org.apache.cassandra.metrics:type=ThreadPools,*"), null);
            return threadPoolObjectNames.stream()
                                        .map(oName -> new ThreadPoolInfo(oName.getKeyProperty("path"), oName.getKeyProperty("scope")))
                                        .collect(Collectors.toList());
        });
    }

    @Override
    public List<Map.Entry<String, ColumnFamilyStoreMBean>> findColumnFamilies(String type)
    {
        return withExceptionHandling(() -> {
            assert type.equals("IndexColumnFamilies") || type.equals("ColumnFamilies");
            connect();

            ObjectName query = new ObjectName("org.apache.cassandra.db:type=" + type + ",*");
            Set<ObjectName> cfObjects = mbeanServerConn.queryNames(query, null);

            List<Map.Entry<String, ColumnFamilyStoreMBean>> mbeans = new ArrayList<>(cfObjects.size());
            for (ObjectName objectName : cfObjects)
            {
                ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mbeanServerConn, objectName, ColumnFamilyStoreMBean.class);
                mbeans.add(new AbstractMap.SimpleImmutableEntry<>(objectName.getKeyProperty("keyspace"), cfsProxy));
            }
            return mbeans;
        });
    }

    public JMXConnector getJmxConnector()
    {
        connect();
        return jmxc;
    }

    public MBeanServerConnection getMBeanServerConnection()
    {
        connect();
        return mbeanServerConn;
    }

    @Override
    public void close()
    {
        if (jmxc == null)
            return;

        try
        {
            jmxc.close();
        }
        catch (ConnectException e)
        {
            // result of stopdaemon command, if close() call fails, the daemon is shutdown
            logger.error("Cassandra has shutdown.");
        }
        catch (IOException e)
        {
            logger.error("Failed to close connection to '{}:{}'.", host, port, e);
        }
        finally
        {
            jmxc = null;
            mbeanServerConn = null;
            connected = false;
            clazzMBanRegistry.clear();
            namedMBeanRegistry.clear();
        }
    }

    private <T> void registerMBeanProxy(Class<T> clazz, String objectName) throws MalformedObjectNameException
    {
        registerMBean(clazz, JMX.newMBeanProxy(mbeanServerConn, new ObjectName(objectName), clazz));
    }

    private <T> void registerPlatformMBeanProxy(Class<T> clazz, String objectName) throws IOException
    {
        registerMBean(clazz, ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn, objectName, clazz));
    }

    private RMIClientSocketFactory getRMIClientSocketFactory()
    {
        if (SSL_ENABLE.getBoolean())
            return new SslRMIClientSocketFactory();
        else
            return RMISocketFactory.getDefaultSocketFactory();
    }

    private static <T> T withExceptionHandling(MBeanSupplier<T> op)
    {
        try
        {
            return op.get();
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException("Invalid ObjectName? Requested MBean may not exist. " +
                                       "Please check the parameters e.g. keyspace name, table name and try again.", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not connect to MBean server. Please check that the JMX port is correct and open.", e);
        }
    }

    @FunctionalInterface
    private interface MBeanSupplier<T>
    {
        T get() throws MalformedObjectNameException, IOException;
    }
}
