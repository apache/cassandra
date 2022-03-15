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

import java.net.InetAddress;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.DistributedTestSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadCoordinationMetrics;
import org.apache.cassandra.service.reads.AbstractReadExecutor;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;

public class ReadCoordinationMetricsTest extends TestBaseImpl
{
    private static final int NUM_ROWS = 100;

    private static long countNonreplicaRequests(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ReadCoordinationMetrics.nonreplicaRequests.getCount());
    }

    private static long countPreferredOtherReplicas(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ReadCoordinationMetrics.preferredOtherReplicas.getCount());
    }

    /**
     * Two nodes with RF=1 so half the data will be owned by each node and the coordinator for queries is not
     * always a replica in the list of candidates in {@link AbstractReadExecutor#getReadExecutor} where
     * {@link ReadCoordinationMetrics#nonreplicaRequests} will be incremented.
     *
     * @throws Throwable
     */
    @Test
    public void testNonReplicaRequests() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2), 1))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < NUM_ROWS; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"), ALL, i, i, i);

            long nonReplicaRequests1 = countNonreplicaRequests(cluster.get(1));
            long nonReplicaRequests2 = countNonreplicaRequests(cluster.get(2));

            for (int i = 0; i < NUM_ROWS; i++)
            {
                // When the coordinator is not a candidate replica, which will be half the time due to RF=1,
                // the non-replica count metric will be incremented.
                cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ? and ck = ?"), ALL, i, i);
                cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ? and ck = ?"), ALL, i, i);
            }

            nonReplicaRequests1 = countNonreplicaRequests(cluster.get(1)) - nonReplicaRequests1;
            nonReplicaRequests2 = countNonreplicaRequests(cluster.get(2)) - nonReplicaRequests2;
            Assert.assertEquals(NUM_ROWS, nonReplicaRequests1 + nonReplicaRequests2);
        }
    }

    /**
     * Two nodes with RF=2 so that both nodes are replicas for all data; this ensures that the coordinator node for
     * queries will be a replica in the list of candidates in {@link AbstractReadExecutor#getReadExecutor}.
     * <p>
     * When the candidates collection is created, the sort order is changed so that the coordinator node is last. Then,
     * using with CL=1 in the query, the resulting contacts collection will not contain the coordinator node, causing
     * {@link ReadCoordinationMetrics#preferredOtherReplicas} to be incremented.
     *
     * @throws Throwable
     */
    @Test
    public void testPreferredOtherReplicas() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < NUM_ROWS; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"), ALL, i, i, i);

            long preferredOtherReplicas1 = countPreferredOtherReplicas(cluster.get(1));

            // Replica nodes are normally sorted by distance from the coordinator; override the test snitch to
            // sort with respect to another node so that the coordinator node is last in the list of replicas.
            // This will be used together with CL=1 to drop the coordinator node from the list of contacts, while
            // remaining in the list of candidates.
            InetAddress address2 = cluster.get(2).broadcastAddress().getAddress();
            cluster.get(1).acceptsOnInstance((IIsolatedExecutor.SerializableConsumer<InetAddress>) (ks) -> {
                DistributedTestSnitch.sortByProximityAddressOverride = InetAddressAndPort.getByAddress(ks);
            }).accept(address2);

            for (int i = 0; i < NUM_ROWS; i++)
            {
                // Query using CL=1 so that the subset of "candidate" replcas selected for the "contacts" collection
                // will have just one node; since the "candidate" list was sorted with respect to the non-coordinator
                // node, this will cause the preferredOtherReplicas count to be incremented.
                cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ? and ck = ?"), ONE, i, i);
            }

            preferredOtherReplicas1 = countPreferredOtherReplicas(cluster.get(1)) - preferredOtherReplicas1;
            Assert.assertEquals(NUM_ROWS, preferredOtherReplicas1);
        }
    }
}
