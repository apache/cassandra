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


import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compression.CompressionDictionaryManagerMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

public interface MBeanAccessor extends AutoCloseable
{
    /**
     * Finds statically known MBeans by MBean class name.
     * @param clazz the MBean class.
     * @param <T> the MBean type.
     * @return the MBean instance, or {@code null} if not found.
     */
    @Nullable
    <T> T findMBean(Class<T> clazz);
    <T> T findMBeanMetric(Class<T> clazz, Props props);
    boolean isMBeanMetricRegistered(Props props);

    ColumnFamilyStoreMBean findColumnFamily(String type, String keyspace, String columnFamily);
    CompressionDictionaryManagerMBean findCompressionDictionary(String keyspace, String table);

    /** List of thread pool MBeans with associated path and pool name. */
    List<ThreadPoolInfo> threadPoolInfos();
    /** List of column family MBeans with associated keyspace name. */
    List<Map.Entry<String, ColumnFamilyStoreMBean>> findColumnFamilies(String type);

    /** {@inheritDoc} */
    @Override
    default void close() { }

    default CassandraMetricsRegistry.JmxCounterMBean findMBeanCounter(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxCounterMBean.class, props);
    }

    default CassandraMetricsRegistry.JmxGaugeMBean findMBeanGauge(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxGaugeMBean.class, props);
    }

    default CassandraMetricsRegistry.JmxMeterMBean findMBeanMeter(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxMeterMBean.class, props);
    }

    default CassandraMetricsRegistry.JmxTimerMBean findMBeanTimer(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxTimerMBean.class, props);
    }

    default CassandraMetricsRegistry.JmxHistogramMBean findMBeanHistogram(Props props)
    {
        return findMBeanMetric(CassandraMetricsRegistry.JmxHistogramMBean.class, props);
    }

    class Props
    {
        private final Map<String, String> values = Maps.newLinkedHashMap();

        public Props(String type, String path, String keyspace, String table, String scope, String name)
        {
            if (type == null || type.isEmpty())
                throw new IllegalArgumentException("type is required");
            values.put("type", type);
            if (path != null)
                values.put("path", path);
            if (keyspace != null)
                values.put("keyspace", keyspace);
            if (table != null)
                values.put("table", table);
            if (scope != null)
                values.put("scope", scope);
            if (name != null)
                values.put("name", name);
        }

        public static Props metric(String type, String name)
        {
            return new Props(type, null, null, null, null, name);
        }

        public static Props scoped(String type, String scope, String name)
        {
            return new Props(type, null, null, null, scope, name);
        }

        public static Props threadPool(String type, String path, String scope, String name)
        {
            return new Props(type, path, null, null, scope, name);
        }

        public static Props sai(String type, String keyspace, String table, String scope, String name)
        {
            return new Props(type, null, keyspace, table, scope, name);
        }

        public static Props columnFamily(String type, String keyspace, String scope, String name)
        {
            return new Props(type, null, keyspace, null, scope, name);
        }

        public static Props keyspace(String type, String keyspace, String name)
        {
            return new Props(type, null, keyspace, null, null, name);
        }

        public Map<String, String> toMap()
        {
            return ImmutableMap.copyOf(values);
        }
    }

    class ThreadPoolInfo
    {
        private final String path;
        private final String poolName;

        public ThreadPoolInfo(String path, String poolName)
        {
            this.path = path;
            this.poolName = poolName;
        }

        public String path()
        {
            return path;
        }
        public String poolName()
        {
            return poolName;
        }
        public String scope()
        {
            return path + '.' + poolName;
        }
    }
}
