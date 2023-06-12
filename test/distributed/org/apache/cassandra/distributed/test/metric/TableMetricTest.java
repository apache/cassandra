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

package org.apache.cassandra.distributed.test.metric;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.QueryExp;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.MBEAN_REGISTRATION_CLASS;

public class TableMetricTest extends TestBaseImpl
{
    static
    {
        MBEAN_REGISTRATION_CLASS.setString(MapMBeanWrapper.class.getName());
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(false);
    }
    private static volatile Map<String, Collection<String>> SYSTEM_TABLES = null;
    private static Set<String> TABLE_METRIC_NAMES = ImmutableSet.of("WriteLatency");

    /**
     * Makes sure that all system tables have the expected metrics
     * @throws IOException
     */
    @Test
    public void systemTables() throws IOException
    {
        try (Cluster cluster = Cluster.build(1).start())
        {
            loadSystemTables(cluster);
            assertSystemTableMetrics(cluster);
        }
    }

    /**
     * Tests that other table metrics are not modified when a single table is modified/deleted.
     *
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-16095">CASSANDRA-16095</a>
     */
    @Test
    public void userTables() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            loadSystemTables(cluster);
            assertSystemTableMetrics(cluster);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk bigint PRIMARY KEY)"));
            cluster.forEach(i -> assertTableMetricsExist(i, KEYSPACE, "tbl"));

            // alter table can change metrics, so monitor for it
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH comment = 'testing'"));
            cluster.forEach(i -> assertTableMetricsExist(i, KEYSPACE, "tbl"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl ADD (value bigint)"));
            cluster.forEach(i -> assertTableMetricsExist(i, KEYSPACE, "tbl"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl RENAME pk TO pk2"));
            cluster.forEach(i -> assertTableMetricsExist(i, KEYSPACE, "tbl"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl DROP value"));
            cluster.forEach(i -> assertTableMetricsExist(i, KEYSPACE, "tbl"));

            // drop and make sure table no longer exists
            cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl"));
            cluster.forEach(i -> assertTableMetricsDoesNotExist(i, KEYSPACE, "tbl"));

            cluster.schemaChange(withKeyspace("DROP KEYSPACE %s"));
            cluster.forEach(i -> assertKeyspaceMetricDoesNotExists(i, KEYSPACE));

            // no other table impacted?
            assertSystemTableMetrics(cluster);
        }
    }

    private static void loadSystemTables(Cluster cluster)
    {
        SYSTEM_TABLES = cluster.get(1).callOnInstance(() -> {
            Map<String, Collection<String>> map = new HashMap<>();
            Arrays.asList(SystemKeyspace.metadata(), AuthKeyspace.metadata(), SystemDistributedKeyspace.metadata(),
                          SchemaKeyspace.metadata(), TraceKeyspace.metadata())
                  .forEach(meta -> {
                      Set<String> tables = meta.tables.stream().map(t -> t.name).collect(Collectors.toSet());
                      map.put(meta.name, tables);
                  });
            return map;
        });
    }

    private static void assertSystemTableMetrics(Cluster cluster)
    {
        for (String keyspace : SYSTEM_TABLES.keySet())
        {
            for (String table : SYSTEM_TABLES.get(keyspace))
            {
                cluster.forEach(i -> assertTableMetricsExist(i, keyspace, table));
            }
        }
    }

    private static void assertTableMetricsExist(IInvokableInstance inst, String keyspace, String table)
    {
        assertTableMBeanExists(inst, keyspace, table);
        for (String metric : TABLE_METRIC_NAMES)
            assertTableMetricExists(inst, keyspace, table, metric);
    }

    private static void assertTableMetricsDoesNotExist(IInvokableInstance inst, String keyspace, String table)
    {
        assertTableMBeanDoesNotExists(inst, keyspace, table);
        for (String metric : TABLE_METRIC_NAMES)
            assertTableMetricDoesNotExists(inst, keyspace, table, metric);
    }

    private static void assertKeyspaceMetricDoesNotExists(IInvokableInstance inst, String keyspace)
    {
        for (String metric : TABLE_METRIC_NAMES)
            assertKeyspaceMetricDoesNotExists(inst, keyspace, metric);
    }

    private static void assertTableMBeanExists(IInvokableInstance inst, String keyspace, String table)
    {
        inst.runOnInstance(() -> {
            // cast only to make sure it linked properly
            MapMBeanWrapper mbeans = getMapMBeanWrapper();
            Assert.assertTrue("Unable to find table mbean for " + keyspace + "." + table,
                              mbeans.isRegistered(ColumnFamilyStore.getTableMBeanName(keyspace, table, false)));
            Assert.assertTrue("Unable to find column family mbean for " + keyspace + "." + table,
                              mbeans.isRegistered(ColumnFamilyStore.getColumnFamilieMBeanName(keyspace, table, false)));
        });
    }

    private static void assertTableMBeanDoesNotExists(IInvokableInstance inst, String keyspace, String table)
    {
        inst.runOnInstance(() -> {
            // cast only to make sure it linked properly
            MapMBeanWrapper mbeans = getMapMBeanWrapper();
            Assert.assertFalse("Found table mbean for " + keyspace + "." + table,
                               mbeans.isRegistered(ColumnFamilyStore.getTableMBeanName(keyspace, table, false)));
            Assert.assertFalse("Found column family mbean for " + keyspace + "." + table,
                               mbeans.isRegistered(ColumnFamilyStore.getColumnFamilieMBeanName(keyspace, table, false)));
        });
    }

    private static void assertTableMetricExists(IInvokableInstance inst, String keyspace, String table, String name)
    {
        inst.runOnInstance(() -> {
            // cast only to make sure it linked properly
            MapMBeanWrapper mbeans = getMapMBeanWrapper();
            String mbean = getTableMetricName(keyspace, table, name);
            Assert.assertTrue("Unable to find metric " + name + " for " + keyspace + "." + table, mbeans.isRegistered(mbean));

            // verify replicated to keyspace
            String keyspaceMBean = getKeyspaceMetricName(keyspace, name);
            Assert.assertTrue("Unable to find keyspace metric " + keyspaceMBean + " for " + keyspace, mbeans.isRegistered(keyspaceMBean));
        });
    }

    private static void assertTableMetricDoesNotExists(IInvokableInstance inst, String keyspace, String table, String name)
    {
        inst.runOnInstance(() -> {
            // cast only to make sure it linked properly
            MapMBeanWrapper mbeans = getMapMBeanWrapper();
            String mbean = getTableMetricName(keyspace, table, name);
            Assert.assertFalse("Found metric " + name + " for " + keyspace + "." + table, mbeans.isRegistered(mbean));

            // validate keyspace metric
            assertKeyspaceMetricMayExists(mbeans, keyspace, name);
        });
    }

    private static void assertKeyspaceMetricMayExists(MapMBeanWrapper mbeans, String keyspace, String name)
    {
        String keyspaceMBean = getKeyspaceMetricName(keyspace, name);
        boolean keyspaceExists = Schema.instance.getKeyspaceMetadata(keyspace) != null;
        String errorMessage = keyspaceExists ?
                              "Unable to find keyspace metric " + keyspaceMBean + " for " + keyspace :
                              "Found keyspace metric " + keyspaceMBean + " for " + keyspace;
        Assert.assertEquals(errorMessage, keyspaceExists, mbeans.isRegistered(keyspaceMBean));
    }

    private static void assertKeyspaceMetricDoesNotExists(IInvokableInstance inst, String keyspace, String name)
    {
        inst.runOnInstance(() -> {
            // cast only to make sure it linked properly
            MapMBeanWrapper mbeans = getMapMBeanWrapper();

            String keyspaceMBean = getKeyspaceMetricName(keyspace, name);
            Assert.assertFalse("Found keyspace metric " + keyspaceMBean + " for " + keyspace, mbeans.isRegistered(keyspaceMBean));
        });
    }

    private static String getKeyspaceMetricName(String keyspace, String name)
    {
        return String.format("org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=%s", keyspace, name);
    }

    private static String getTableMetricName(String keyspace, String table, String name)
    {
        return String.format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=%s", keyspace, table, name);
    }

    private static MapMBeanWrapper getMapMBeanWrapper()
    {
        return (MapMBeanWrapper) ((MBeanWrapper.DelegatingMbeanWrapper)MBeanWrapper.instance).getDelegate();
    }
    public static final class MapMBeanWrapper implements MBeanWrapper
    {
        private final ConcurrentMap<ObjectName, Object> map = new ConcurrentHashMap<>();

        @Override
        public void registerMBean(Object obj, ObjectName mbeanName, OnException onException)
        {
            Object current = map.putIfAbsent(mbeanName, obj);
            if (current != null)
                onException.handler.accept(new InstanceAlreadyExistsException("MBean " + mbeanName + " already exists"));
        }

        @Override
        public boolean isRegistered(ObjectName mbeanName, OnException onException)
        {
            return map.containsKey(mbeanName);
        }

        @Override
        public void unregisterMBean(ObjectName mbeanName, OnException onException)
        {
            Object previous = map.remove(mbeanName);
            if (previous == null)
                onException.handler.accept(new InstanceNotFoundException("MBean " + mbeanName + " was not found"));
        }

        @Override
        public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
        {
            return null;
        }

        @Override
        public MBeanServer getMBeanServer()
        {
            return null;
        }
    }
}
