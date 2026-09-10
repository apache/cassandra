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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Counter writes on a keyspace with witnesses (transient replicas). The witness-free equivalent is
 * {@link TrackedCounterForwardingTest}.
 *
 * A counter leader resolves the increment against its local data
 * ({@link org.apache.cassandra.db.CounterMutation#processModifications}) and replicates the resolved
 * absolute value. A witness holds no data for the range it witnesses, so a witness elected leader resolves
 * against nothing and writes a value discarding every prior increment.
 *
 * The proximity implementation sorts witnesses first, which is what
 * {@link ReplicaPlans#findCounterLeaderReplica} falls back on when no replica is in the local
 * datacenter. See {@link WitnessAlwaysReadsFullReplicaTest.TransientFirstProximity}.
 */
public class TrackedCounterWitnessForwardingTest extends TestBaseImpl
{
    private static final String TABLE = "counters";
    private static final String QUALIFIED_TABLE = KEYSPACE + '.' + TABLE;
    private static final String KEY = "1";

    private static Cluster witnessCluster() throws IOException
    {
        Cluster cluster = Cluster.build(3)
                                 .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                       .with(Feature.GOSSIP)
                                                       // NATIVE_PROTOCOL so Instance startup reaches
                                                       // CassandraDaemon#startNativeTransport, which sets
                                                       // rpc-readiness. findCounterLeaderReplica filters on it.
                                                       .with(Feature.NATIVE_PROTOCOL)
                                                       .set("transient_replication_enabled", "true")
                                                       .set("dynamic_snitch", false)
                                                       .set("node_proximity", WitnessAlwaysReadsFullReplicaTest.TransientFirstProximity.class.getName()))
                                 .start();
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} AND replication_type='tracked'");
        cluster.schemaChange("CREATE TABLE " + QUALIFIED_TABLE + " (k text PRIMARY KEY, c counter)");
        return cluster;
    }

    @Test
    public void testElectedLeaderIsAlwaysFull() throws IOException
    {
        try (Cluster cluster = witnessCluster())
        {
            // Election picks at random among candidates, so one election proves nothing
            int elections = 500;
            int transientElections = cluster.get(1).callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                DecoratedKey key = Schema.instance.getTableMetadata(KEYSPACE, TABLE)
                                                  .partitioner.decorateKey(ByteBufferUtil.bytes(KEY));
                String localDc = DatabaseDescriptor.getLocator().local().datacenter;

                int transientCount = 0;
                for (int i = 0; i < elections; i++)
                {
                    if (ReplicaPlans.findCounterLeaderReplica(metadata, KEYSPACE, key, localDc, ConsistencyLevel.ONE).isTransient())
                        transientCount++;
                }
                return transientCount;
            });

            assertThat(transientElections)
                .describedAs("a witness was elected counter leader; it holds no data for the range it witnesses")
                .isZero();
        }
    }

    @Test
    public void testIncrementsAreNotLost() throws IOException
    {
        try (Cluster cluster = witnessCluster())
        {
            int increments = 50;
            for (int i = 0; i < increments; i++)
                cluster.coordinator(1).execute("UPDATE " + QUALIFIED_TABLE + " SET c = c + 1 WHERE k = ?",
                                               org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, KEY);

            Object[][] result = cluster.coordinator(1).execute("SELECT c FROM " + QUALIFIED_TABLE + " WHERE k = ?",
                                                              org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, KEY);
            assertThat(result).hasNumberOfRows(1);
            assertThat((long) result[0][0])
                .describedAs("an increment resolved against a witness's empty data would reset the counter")
                .isEqualTo(increments);
        }
    }

    /**
     * With every full replica for the token shut down, the write has no candidate leader. It must fail
     * rather than fall back on the witness, which would reset the counter to the increment's own value.
     */
    @Test
    public void testUnavailableWhenNoFullReplicaIsUp() throws IOException
    {
        try (Cluster cluster = witnessCluster())
        {
            cluster.coordinator(1).execute("UPDATE " + QUALIFIED_TABLE + " SET c = c + 7 WHERE k = ?",
                                           org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, KEY);

            List<Integer> fullReplicaNodes = fullReplicaNodesFor(cluster, KEY);
            assertThat(fullReplicaNodes).describedAs("a 3/1 keyspace has two full replicas").hasSize(2);
            int coordinator = nodeOtherThan(cluster, fullReplicaNodes);

            // Shut down rather than filtering messages: a message filter does not convict the node in the
            // failure detector, so a leader would still be elected and the write would time out instead
            for (int node : fullReplicaNodes)
                cluster.get(node).shutdown().get();

            assertThatThrownBy(() ->
                cluster.coordinator(coordinator).execute("UPDATE " + QUALIFIED_TABLE + " SET c = c + 1 WHERE k = ?",
                                                         org.apache.cassandra.distributed.api.ConsistencyLevel.ONE, KEY))
                .describedAs("with no full replica up the counter write must fail rather than be led by the witness")
                .isInstanceOf(Throwable.class);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static List<Integer> fullReplicaNodesFor(Cluster cluster, String key)
    {
        String endpoints = cluster.get(1).callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            Token token = Schema.instance.getTableMetadata(KEYSPACE, TABLE)
                                         .partitioner.getToken(ByteBufferUtil.bytes(key));
            StringBuilder sb = new StringBuilder();
            metadata.placements.get(Schema.instance.getKeyspaceMetadata(KEYSPACE).params.replication)
                    .reads.forToken(token).get()
                    .stream()
                    .filter(Replica::isFull)
                    .forEach(r -> sb.append(sb.length() == 0 ? "" : ",").append(r.endpoint().getHostAddress(false)));
            return sb.toString();
        });

        List<Integer> nodes = new ArrayList<>();
        for (String endpoint : endpoints.split(","))
            nodes.add(Integer.parseInt(endpoint.substring(endpoint.lastIndexOf('.') + 1)));
        return nodes;
    }

    private static int nodeOtherThan(Cluster cluster, List<Integer> excluded)
    {
        for (int node = 1; node <= cluster.size(); node++)
            if (!excluded.contains(node))
                return node;
        throw new AssertionError("no node available outside " + excluded);
    }
}
