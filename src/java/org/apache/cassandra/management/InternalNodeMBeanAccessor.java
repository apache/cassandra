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

package org.apache.cassandra.management;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.audit.AuditLogManagerMBean;
import org.apache.cassandra.auth.AbstractCIDRAuthorizer;
import org.apache.cassandra.auth.AuthCache;
import org.apache.cassandra.auth.AuthCacheMBean;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.CIDRGroupsMappingManagerMBean;
import org.apache.cassandra.auth.CIDRPermissionsManagerMBean;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
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
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.LocationInfo;
import org.apache.cassandra.locator.LocationInfoMBean;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.profiler.AsyncProfilerMBean;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.AsyncProfilerService;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.AutoRepairServiceMBean;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.GCInspectorMXBean;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.service.accord.AccordOperations;
import org.apache.cassandra.service.accord.AccordOperationsMBean;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.SnapshotManagerMBean;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.CMSOperationsMBean;
import org.apache.cassandra.tools.RemoteJmxMBeanAccessor;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.service.CassandraDaemon.SKIP_GC_INSPECTOR;

/**
 * Server-side implementation of {@link MBeanAccessor} for in-process execution.
 *
 * <p>
 * Returns MBean instances without going through JMX, for the CEP-38 management API, where commands
 * run in the same JVM as the Cassandra daemon.
 *
 * <p>
 * Unlike {@link RemoteJmxMBeanAccessor}, this implementation:
 * <ul>
 *   <li>Reads singleton instances directly (e.g., {@code StorageService.instance})</li>
 *   <li>Needs no network connection or JMX connector</li>
 *   <li>Serializes nothing: arguments and results stay as objects</li>
 *   <li>Has no connection state to manage</li>
 *   <li>Works directly with {@link Keyspace} and {@link ColumnFamilyStore} instances</li>
 * </ul>
 *
 * <p>
 * Providers for the known MBeans are registered up front, but each one resolves its MBean only on
 * first access.
 *
 * @see MBeanAccessor
 * @see RemoteJmxMBeanAccessor
 */
public class InternalNodeMBeanAccessor implements MBeanAccessor
{
    private final Map<Class<?>, MBeanProvider<?>> mBeanProviders = new ConcurrentHashMap<>();
    private final Map<Class<?>, Object> mBeanCache = new ConcurrentHashMap<>();
    private final Map<String, Object> metricCache = new ConcurrentHashMap<>();

    /**
     * Creates a new InternalNodeMBeanAccessor using direct instance access.
     */
    public InternalNodeMBeanAccessor()
    {
        initializeMBeanProviders();
    }

    /**
     * Initializes all statically known MBean instances.
     */
    private void initializeMBeanProviders()
    {
        registerMBeanProvider(AccordOperationsMBean.class, () -> AccordOperations.instance);
        registerMBeanProvider(ActiveRepairServiceMBean.class, ActiveRepairService::instance);
        registerMBeanProvider(AuditLogManagerMBean.class, () -> AuditLogManager.instance);
        registerMBeanProvider(AutoRepairServiceMBean.class, () -> AutoRepairService.instance);
        registerMBeanProvider(BatchlogManagerMBean.class, () -> BatchlogManager.instance);
        registerMBeanProvider(CMSOperationsMBean.class, () -> CMSOperations.instance);
        registerMBeanProvider(CacheServiceMBean.class, () -> CacheService.instance);
        registerMBeanProvider(CompactionManagerMBean.class, () -> CompactionManager.instance);
        registerMBeanProvider(DynamicEndpointSnitchMBean.class, this::resolveDynamicEndpointSnitch);
        registerMBeanProvider(FailureDetectorMBean.class, () -> (FailureDetectorMBean) FailureDetector.instance);
        registerMBeanProvider(GCInspectorMXBean.class, this::resolveGCInspector);
        registerMBeanProvider(GossiperMBean.class, () -> Gossiper.instance);
        registerMBeanProvider(GuardrailsMBean.class, () -> Guardrails.instance);
        registerMBeanProvider(HintsServiceMBean.class, () -> HintsService.instance);
        registerMBeanProvider(MemoryMXBean.class, ManagementFactory::getMemoryMXBean);
        registerMBeanProvider(MessagingServiceMBean.class, MessagingService::instance);
        registerMBeanProvider(RuntimeMXBean.class, ManagementFactory::getRuntimeMXBean);
        registerMBeanProvider(SnapshotManagerMBean.class, () -> SnapshotManager.instance);
        registerMBeanProvider(StorageProxyMBean.class, () -> StorageProxy.instance);
        registerMBeanProvider(StorageServiceMBean.class, () -> StorageService.instance);
        registerMBeanProvider(StreamManagerMBean.class, () -> StreamManager.instance);
        registerMBeanProvider(AsyncProfilerMBean.class, AsyncProfilerService::instance);

        // Utility MBeans are stateless and can be created on demand.
        // They query DatabaseDescriptor for the current state, so new instances are fine
        registerMBeanProvider(EndpointSnitchInfoMBean.class, EndpointSnitchInfo::new);
        registerMBeanProvider(LocationInfoMBean.class, LocationInfo::new);

        // AuthCache MBeans
        registerMBeanProvider(AuthorizationProxy.JmxPermissionsCacheMBean.class,
                              () -> findAuthCache(AuthorizationProxy.JmxPermissionsCacheMBean.class));
        registerMBeanProvider(NetworkPermissionsCacheMBean.class,
                              () -> findAuthCache(NetworkPermissionsCacheMBean.class));
        registerMBeanProvider(PasswordAuthenticator.CredentialsCacheMBean.class,
                              () -> findAuthCache(PasswordAuthenticator.CredentialsCacheMBean.class));
        registerMBeanProvider(PermissionsCacheMBean.class,
                              () -> findAuthCache(PermissionsCacheMBean.class));
        registerMBeanProvider(RolesCacheMBean.class,
                              () -> findAuthCache(RolesCacheMBean.class));

        // CIDR Auth MBeans
        registerMBeanProvider(CIDRFilteringMetricsTableMBean.class, () -> CIDRFilteringMetricsTable.instance);
        registerMBeanProvider(CIDRGroupsMappingManagerMBean.class, () -> AbstractCIDRAuthorizer.cidrGroupsMappingManager);
        registerMBeanProvider(CIDRPermissionsManagerMBean.class, () -> AbstractCIDRAuthorizer.cidrPermissionsManager);
    }

    /**
     * Gets DynamicEndpointSnitch from DatabaseDescriptor if it's a DynamicEndpointSnitch,
     * otherwise returns null.
     */
    private DynamicEndpointSnitchMBean resolveDynamicEndpointSnitch()
    {
        if (!DatabaseDescriptor.isDynamicEndpointSnitch())
            throw new IllegalStateException("DynamicEndpointSnitch has been requested but is not enabled");

        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        assert proximity instanceof DynamicEndpointSnitch;

        return (DynamicEndpointSnitchMBean) proximity;
    }

    private GCInspectorMXBean resolveGCInspector()
    {
        if (SKIP_GC_INSPECTOR)
            throw new IllegalStateException("GCInspector has been requested but is disabled via SKIP_GC_INSPECTOR flag");

        try
        {
            MBeanServer mbs = MBeanWrapper.instance.getMBeanServer();
            if (mbs == null)
                return null;

            ObjectName name = new ObjectName(GCInspector.MBEAN_NAME);
            if (mbs.isRegistered(name))
                return JMX.newMBeanProxy(mbs, name, GCInspectorMXBean.class);
        }
        catch (Exception e)
        {
            // Fall through to create a new instance
        }

        return null;
    }

    /** Finds an auth cache MBean instance from AuthCacheService. */
    private <T> T findAuthCache(Class<T> clazz)
    {
        Set<AuthCache<?, ?>> caches = AuthCacheService.instance.getCaches();
        if (caches.isEmpty())
            return null;

        AuthCacheFinder visitor = new AuthCacheFinder(clazz);
        for (AuthCache<?, ?> cache : caches)
        {
            cache.accept(visitor);
            Object found = visitor.getCache();
            if (found == null)
                continue;
            return clazz.cast(found);
        }
        return null;
    }

    private <T> void registerMBeanProvider(Class<T> clazz, MBeanProvider<T> locator)
    {
        Object prev = mBeanProviders.putIfAbsent(clazz, locator);
        assert prev == null : "MBean locator for " + clazz.getName() + " is already registered";
    }

    @Override
    public <T> T findMBean(Class<T> clazz)
    {
        Object cached = mBeanCache.get(clazz);
        if (cached != null)
            return clazz.cast(cached);

        Object created = mBeanCache.computeIfAbsent(clazz, k -> {
            MBeanProvider<?> provider = mBeanProviders.get(k);
            return provider == null ? null : provider.provide();
        });
        return created == null ? null : clazz.cast(created);
    }

    private MBeanServer mbeanServer()
    {
        MBeanServer mbs = MBeanWrapper.instance.getMBeanServer();
        if (mbs == null)
            throw new IllegalStateException("The MBean server is not available (MBean registration is disabled on this node)");
        return mbs;
    }

    @Override
    public <T> T findMBeanMetric(Class<T> clazz, Props props)
    {
        try
        {
            assert clazz.isInterface() && CassandraMetricsRegistry.MetricMBean.class.isAssignableFrom(clazz);

            ObjectName objectName = buildObjectNameFromProps(props);
            String cacheKey = objectName.getCanonicalName();

            @SuppressWarnings("unchecked")
            T cached = (T) metricCache.get(cacheKey);
            if (cached != null)
                return cached;

            MBeanServer mbs = mbeanServer();

            return clazz.cast(metricCache.computeIfAbsent(cacheKey, k -> JMX.newMBeanProxy(mbs, objectName, clazz)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error accessing metric MBean: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isMBeanMetricRegistered(Props props)
    {
        try
        {
            // Look the MBean up in the internal MBean server by ObjectName, which reuses the names the
            // metric factories already built at registration time.
            //
            // The alternative is CassandraMetricsRegistry, which looks metrics up by metric name
            // (e.g., "org.apache.cassandra.metrics.Keyspace.ReadLatency.mykeyspace"). That means rebuilding
            // the full metric name from Props, and every MetricNameFactory builds its scope differently:
            // - KeyspaceMetrics: scope = keyspace property
            // - TableMetrics: scope = keyspace + '.' + scope property
            // - DefaultNameFactory: scope = scope property
            // - SAI AbstractMetrics: scope = keyspace.table.index.scope (all combined)
            //
            // So that path would duplicate scope construction from each factory, or require refactoring the
            // factories to share it. Worth revisiting: it would make in-process lookups cheaper and let us
            // drop the JMX dependency here.

            ObjectName objectName = buildObjectNameFromProps(props);
            return mbeanServer().isRegistered(objectName);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error checking metric MBean registration: " + e.getMessage(), e);
        }
    }

    private static ObjectName buildObjectNameFromProps(Props props) throws MalformedObjectNameException
    {
        return new ObjectName("org.apache.cassandra.metrics", new Hashtable<>(props.toMap()));
    }

    @Override
    public ColumnFamilyStoreMBean findColumnFamily(String type, String keyspace, String columnFamily)
    {
        Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
        if (ks == null)
            throw new IllegalArgumentException("Keyspace not found: " + keyspace);

        if (!SecondaryIndexManager.isIndexColumnFamily(columnFamily))
            return ks.getColumnFamilyStore(columnFamily);

        // Secondary-index stores are registered under "base.index" names and are only
        // reachable through the base table's index manager, not by keyspace lookup.
        ColumnFamilyStore base = ks.getColumnFamilyStore(SecondaryIndexManager.getParentCfsName(columnFamily));
        Index index = base.indexManager.getIndexByName(SecondaryIndexManager.getIndexName(columnFamily));
        if (index == null)
            throw new IllegalArgumentException(String.format("Index not found: %s.%s", keyspace, columnFamily));
        return index.getBackingTable()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Index %s.%s has no backing table",
                                                                                  keyspace, columnFamily)));
    }

    @Override
    public CompressionDictionaryManagerMBean findCompressionDictionary(String keyspace, String table)
    {
        Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
        if (ks == null || Schema.instance.getTableMetadata(keyspace, table) == null)
            throw new IllegalArgumentException(String.format("Table %s.%s does not exist", keyspace, table));

        CompressionDictionaryManager manager = ks.getColumnFamilyStore(table).compressionDictionaryManager();
        if (!manager.isEnabled())
            throw new IllegalStateException("The compression on table " + keyspace + '.' + table +
                                            " is not enabled or SSTable compressor is not a dictionary compressor.");
        return manager;
    }

    @Override
    public List<ThreadPoolInfo> threadPoolInfos()
    {
        List<ThreadPoolInfo> infos = new ArrayList<>();
        for (ThreadPoolMetrics metrics : Metrics.allThreadPoolMetrics())
            infos.add(new ThreadPoolInfo(metrics.path, metrics.poolName));
        return infos;
    }

    @Override
    public List<Map.Entry<String, ColumnFamilyStoreMBean>> findColumnFamilies(String type)
    {
        try
        {
            assert type.equals("IndexColumnFamilies") || type.equals("ColumnFamilies");

            List<Map.Entry<String, ColumnFamilyStoreMBean>> mbeans = new ArrayList<>();

            for (Keyspace keyspace : Keyspace.all())
            {
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                {
                    // Secondary-index backing stores are not registered in the keyspace,
                    // they are only reachable through the base table's index manager.
                    if (type.equals("IndexColumnFamilies"))
                        for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
                            mbeans.add(new AbstractMap.SimpleImmutableEntry<>(keyspace.getName(), indexCfs));
                    else
                        mbeans.add(new AbstractMap.SimpleImmutableEntry<>(keyspace.getName(), cfs));
                }
            }

            return mbeans;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error accessing column families", e);
        }
    }

    @Override
    public void close()
    {
        metricCache.clear();
        mBeanCache.clear();
    }

    /**
     * Functional interface for providing MBean instances lazily.
     * Used to defer MBean initialization until the MBean is actually accessed.
     *
     * @param <T> the MBean interface type
     */
    @FunctionalInterface
    public interface MBeanProvider<T>
    {
        /**
         * @return the MBean instance, or {@code null} if the MBean is not available
         * @throws RuntimeException if the MBean cannot be provided (e.g., not initialized yet)
         */
        T provide();
    }

    /** Visitor that finds a specific auth cache MBean type from AuthCacheService. */
    private static class AuthCacheFinder implements AuthCache.MBeanVisitor
    {
        private final Class<?> targetType;
        private Object foundCache;

        AuthCacheFinder(Class<?> targetType)
        {
            this.targetType = targetType;
        }

        @Override
        public void visitCredentials(PasswordAuthenticator.CredentialsCacheMBean cache)
        {
            if (targetType.equals(PasswordAuthenticator.CredentialsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitJmxPermissions(AuthorizationProxy.JmxPermissionsCacheMBean cache)
        {
            if (targetType.equals(AuthorizationProxy.JmxPermissionsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitPermissions(PermissionsCacheMBean cache)
        {
            if (targetType.equals(PermissionsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitNetwork(NetworkPermissionsCacheMBean cache)
        {
            if (targetType.equals(NetworkPermissionsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitRoles(RolesCacheMBean cache)
        {
            if (targetType.equals(RolesCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visit(AuthCacheMBean cache)
        {
            // No-op. Used for caches without specific MBean types.
        }

        Object getCache()
        {
            return foundCache;
        }
    }
}