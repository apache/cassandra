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

package org.apache.cassandra.distributed.mock.nodetool;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Multimap;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compression.CompressionDictionaryManagerMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.hints.HintsServiceMBean;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.SnitchAdapter;
import org.apache.cassandra.management.MBeanAccessor;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.profiler.AsyncProfilerMBean;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.AsyncProfilerService;
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
import org.apache.cassandra.tools.NodeProbe;

public class InternalNodeProbe extends NodeProbe
{
    private boolean previousSkipNotificationListeners = false;

    public InternalNodeProbe(boolean withNotifications)
    {
        super(new TestMockMBeanAccessor()); // host/port are unused in InternalNodeProbe
        previousSkipNotificationListeners = StorageService.instance.skipNotificationListeners;
        StorageService.instance.skipNotificationListeners = !withNotifications;
    }

    @Override
    public void close()
    {
        StorageService.instance.skipNotificationListeners = previousSkipNotificationListeners;
    }

    @Override
    // overrides all the methods referenced mbeanServerConn/jmxc in super
    public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy()
    {
        return new EndpointSnitchInfo();
    }

	@Override
    public DynamicEndpointSnitchMBean getDynamicEndpointSnitchInfoProxy()
    {
        // TODO At some point we should change this to use modern config e.g. Locator and InitialLocationProvider
        return new DynamicEndpointSnitch(new SnitchAdapter(DatabaseDescriptor.createEndpointSnitch(DatabaseDescriptor.getRawConfig().endpoint_snitch)));
    }

    public CacheServiceMBean getCacheServiceMBean()
    {
        return cacheService;
    }

    @Override
    public ColumnFamilyStoreMBean getCfsProxy(String ks, String cf)
    {
        return Keyspace.open(ks).getColumnFamilyStore(cf);
    }

    // The below methods are only used by the commands (i.e. Info, TableHistogram, TableStats, etc.) that display informations. Not useful for dtest, so disable it.
    @Override
    public Object getCacheMetric(String cacheType, String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Multimap<String, String> getThreadPools()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getThreadPoolMetric(String pathName, String poolName, String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getColumnFamilyMetric(String ks, String cf, String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CassandraMetricsRegistry.JmxTimerMBean getProxyMetric(String scope)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CassandraMetricsRegistry.JmxTimerMBean getMessagingQueueWaitMetrics(String verb)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getCompactionMetric(String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getCQLMetric(String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getClientMetric(String metricName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getStorageMetric(String metricName)
    {
        throw new UnsupportedOperationException();
    }

    private static class TestMockMBeanAccessor implements MBeanAccessor
    {
        private final Map<Class<?>, Object> mbeanRegistry = new HashMap<>();

        public TestMockMBeanAccessor()
        {
            registerMBean(StorageServiceMBean.class, StorageService.instance);
            registerMBean(SnapshotManagerMBean.class, SnapshotManager.instance);
            registerMBean(CMSOperationsMBean.class, CMSOperations.instance);
            registerMBean(AccordOperationsMBean.class, AccordOperations.instance);
            registerMBean(MessagingServiceMBean.class, MessagingService.instance());
            registerMBean(StreamManagerMBean.class, StreamManager.instance);
            registerMBean(CompactionManagerMBean.class, CompactionManager.instance);
            registerMBean(FailureDetectorMBean.class, (FailureDetectorMBean) FailureDetector.instance);
            registerMBean(CacheServiceMBean.class, CacheService.instance);
            registerMBean(StorageProxyMBean.class, StorageProxy.instance);
            registerMBean(HintsServiceMBean.class, HintsService.instance);
            registerMBean(GCInspectorMXBean.class, new GCInspector());
            registerMBean(GossiperMBean.class, Gossiper.instance);
            registerMBean(BatchlogManagerMBean.class, BatchlogManager.instance);
            registerMBean(ActiveRepairServiceMBean.class, ActiveRepairService.instance());
            registerMBean(MemoryMXBean.class, ManagementFactory.getMemoryMXBean());
            registerMBean(RuntimeMXBean.class, ManagementFactory.getRuntimeMXBean());
            registerMBean(AsyncProfilerMBean.class, AsyncProfilerService.instance());
        }

        protected <T> void registerMBean(Class<T> clazz, T mbean)
        {
            mbeanRegistry.put(clazz, mbean);
        }

        @Override
        public <T> T findMBean(Class<T> clazz)
        {
            return mbeanRegistry.get(clazz) == null ? null : clazz.cast(mbeanRegistry.get(clazz));
        }

        @Override
        public <T> T findMBeanMetric(Class<T> clazz, Props props)
        {
            return null;
        }

        @Override
        public boolean isMBeanMetricRegistered(Props props)
        {
            return false;
        }

        @Override
        public ColumnFamilyStoreMBean findColumnFamily(String type, String keyspace, String columnFamily)
        {
            return null;
        }

        @Override
        public CompressionDictionaryManagerMBean findCompressionDictionary(String keyspace, String table)
        {
            return null;
        }

        @Override
        public List<ThreadPoolInfo> threadPoolInfos()
        {
            return List.of();
        }

        @Override
        public List<Map.Entry<String, ColumnFamilyStoreMBean>> findColumnFamilies(String type)
        {
            return List.of();
        }
    }
}
