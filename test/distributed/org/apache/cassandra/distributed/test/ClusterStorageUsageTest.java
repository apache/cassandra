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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableFunction;
import org.apache.cassandra.metrics.DefaultNameFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class verifies the behavior of our global disk usage metrics across different cluster and replication
 * configurations. In addition, it verifies that they are properly exposed via the public metric registry.
 *
 * Disk usage metrics are characterized by how they handle compression and replication:
 *
 * "compressed" -> indicates raw disk usage
 * "uncompressed" -> indicates uncompressed file size (which is equivalent to "compressed" with no compression enabled)
 * "replicated" -> includes disk usage for data outside the node's primary range
 * "unreplicated" -> indicates disk usage scaled down by replication factor across the entire cluster
 */
public class ClusterStorageUsageTest extends TestBaseImpl
{
    private static final DefaultNameFactory FACTORY = new DefaultNameFactory("Storage");
    private static final int MUTATIONS = 1000;

    @Test
    public void testNoReplication() throws Throwable
    {
        // With a replication factor of 1 for our only user keyspace, system keyspaces using local replication, and
        // empty distributed system tables, replicated and unreplicated versions of our compressed and uncompressed
        // metrics should be equivalent.

        try (Cluster cluster = init(builder().withNodes(2).start(), 1))
        {
            populateUserKeyspace(cluster);
            verifyLoadMetricsWithoutReplication(cluster.get(1));
            verifyLoadMetricsWithoutReplication(cluster.get(2));
        }
    }

    private void verifyLoadMetricsWithoutReplication(IInvokableInstance node)
    {
        long compressedLoad = getLoad(node);
        long uncompressedLoad = getUncompressedLoad(node);
        assertThat(compressedLoad).isEqualTo(getUnreplicatedLoad(node));
        assertThat(uncompressedLoad).isEqualTo(getUnreplicatedUncompressedLoad(node));
        assertThat(uncompressedLoad).isGreaterThan(compressedLoad);
    }

    @Test
    public void testSimpleReplication() throws Throwable
    {
        // With a replication factor of 2 for our only user keyspace, disk space used by that keyspace should
        // be scaled down by a factor of 2, while contributions from system keyspaces are unaffected.

        try (Cluster cluster = init(builder().withNodes(3).start(), 2))
        {
            populateUserKeyspace(cluster);

            verifyLoadMetricsWithReplication(cluster.get(1));
            verifyLoadMetricsWithReplication(cluster.get(2));
            verifyLoadMetricsWithReplication(cluster.get(3));
        }
    }

    @Test
    public void testMultiDatacenterReplication() throws Throwable
    {
        // With a replication factor of 1 for our only user keyspace in two DCs, disk space used by that keyspace should
        // be scaled down by a factor of 2, while contributions from system keyspaces are unaffected.

        try (Cluster cluster = builder().withDC("DC1", 2).withDC("DC2", 2).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1, 'DC2': 1};");
            populateUserKeyspace(cluster);

            verifyLoadMetricsWithReplication(cluster.get(1));
            verifyLoadMetricsWithReplication(cluster.get(2));
            verifyLoadMetricsWithReplication(cluster.get(3));
            verifyLoadMetricsWithReplication(cluster.get(4));
        }
    }

    private void populateUserKeyspace(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, v text, PRIMARY KEY (pk));"));

        for (int i = 0; i < MUTATIONS; i++) {
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?,?)"), ConsistencyLevel.ALL, i, "compressable");
        }

        cluster.forEach((i) -> i.flush(KEYSPACE));
    }

    private void verifyLoadMetricsWithReplication(IInvokableInstance node)
    {
        long unreplicatedLoad = getUnreplicatedLoad(node);
        long expectedUnreplicatedLoad = computeUnreplicatedMetric(node, table -> table.metric.liveDiskSpaceUsed.getCount());
        assertThat(expectedUnreplicatedLoad).isEqualTo(unreplicatedLoad);
        assertThat(getLoad(node)).isGreaterThan(unreplicatedLoad);

        long unreplicatedUncompressedLoad = getUnreplicatedUncompressedLoad(node);
        long expectedUnreplicatedUncompressedLoad = computeUnreplicatedMetric(node, table -> table.metric.uncompressedLiveDiskSpaceUsed.getCount());
        assertThat(expectedUnreplicatedUncompressedLoad).isEqualTo(unreplicatedUncompressedLoad);
        assertThat(getUncompressedLoad(node)).isGreaterThan(unreplicatedUncompressedLoad);
    }

    private long getLoad(IInvokableInstance node)
    {
        return node.metrics().getCounter(FACTORY.createMetricName("Load").getMetricName());
    }

    private long getUncompressedLoad(IInvokableInstance node1)
    {
        return node1.metrics().getCounter(FACTORY.createMetricName("UncompressedLoad").getMetricName());
    }

    private long getUnreplicatedLoad(IInvokableInstance node)
    {
        return (Long) node.metrics().getGauge(FACTORY.createMetricName("UnreplicatedLoad").getMetricName());
    }

    private long getUnreplicatedUncompressedLoad(IInvokableInstance node)
    {
        return (Long) node.metrics().getGauge(FACTORY.createMetricName("UnreplicatedUncompressedLoad").getMetricName());
    }

    private long computeUnreplicatedMetric(IInvokableInstance node, SerializableFunction<ColumnFamilyStore, Long> metric)
    {
        return node.callOnInstance(() ->
                                   {
                                       long sum = 0;

                                       for (Keyspace keyspace : Keyspace.all())
                                           for (ColumnFamilyStore table : keyspace.getColumnFamilyStores())
                                               sum += metric.apply(table) / keyspace.getReplicationStrategy().getReplicationFactor().fullReplicas;

                                       return sum;
                                   });
    }
}