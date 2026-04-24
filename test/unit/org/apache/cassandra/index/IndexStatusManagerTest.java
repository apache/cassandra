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

package org.apache.cassandra.index;

import java.io.UTFDataFormatException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class IndexStatusManagerTest
{
    @Parameterized.Parameter
    public boolean legacyStatusFormat;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        parameters.add(new Object[] { true });
        parameters.add(new Object[] { false });
        return parameters;
    }

    static class Testcase
    {
        String keyspace;
        int numRequired;
        AbstractReplicationStrategy replicationStrategy;
        Map<InetAddressAndPort, Map<String, Index.Status>> indexStatus;
        EndpointsForRange expected;

        Testcase(Builder builder)
        {
            keyspace = builder.keyspace;
            numRequired = builder.numRequired;
            replicationStrategy = builder.replicationStrategy;
            indexStatus = builder.indexStatus;
            expected = builder.expected;
        }

        static class Builder
        {
            String keyspace;
            int numRequired;
            AbstractReplicationStrategy replicationStrategy;
            Map<InetAddressAndPort, Map<String, Index.Status>> indexStatus;
            EndpointsForRange expected;

            Builder keyspace(String ks)
            {
                keyspace = ks;
                return this;
            }

            Builder numRequired(int required)
            {
                numRequired = required;
                return this;
            }

            Builder replicationStrategy(AbstractReplicationStrategy strategy)
            {
                replicationStrategy = strategy;
                return this;
            }

            Builder indexStatus(Map<InetAddressAndPort, Map<String, Index.Status>> status)
            {
                indexStatus = status;
                return this;
            }

            Builder expected(EndpointsForRange endpoints)
            {
                expected = endpoints;
                return this;
            }

            Testcase build()
            {
                return new Testcase(this);
            }
        }
    }

    @Test
    public void shouldPrioritizeSuccessfulEndpoints()
    {
        runTest(new Testcase.Builder()
                .keyspace("ks1")
                .replicationStrategy(new NetworkTopologyStrategy("ks1", Map.of("DC", "5")))
                .indexStatus(Map.of(
                        InetAddressAndPort.getByNameUnchecked("127.0.0.251"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.252"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.253"),
                        Map.of(
                                "ks1.idx1", Index.Status.UNKNOWN,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.254"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.255"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.UNKNOWN
                        )
                ))
                .numRequired(2)
                .expected(EndpointsForRange.of(
                        // successful
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.251")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.252")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.254")),

                        // queryable, but unknown
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.253")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.255"))
                ))
                .build()
        );
    }

    @Test
    public void shouldNotPrioritizeWhenNoSuccessfulEndpoints()
    {
        runTest(new Testcase.Builder()
                .keyspace("ks1")
                .replicationStrategy(new NetworkTopologyStrategy("ks1", Map.of("DC", "5")))
                .indexStatus(Map.of(
                        InetAddressAndPort.getByNameUnchecked("127.0.0.251"),
                        Map.of(
                                "ks1.idx1", Index.Status.UNKNOWN,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.UNKNOWN
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.252"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.UNKNOWN,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.253"),
                        Map.of(
                                "ks1.idx1", Index.Status.UNKNOWN,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.254"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.UNKNOWN
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.255"),
                        Map.of(
                                "ks1.idx1", Index.Status.UNKNOWN,
                                "ks1.idx2", Index.Status.UNKNOWN,
                                "ks1.idx3", Index.Status.UNKNOWN
                        )
                ))
                .numRequired(2)
                .expected(EndpointsForRange.of(
                        // unmodified order
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.251")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.252")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.253")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.254")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.255"))
                ))
                .build()
        );
    }

    @Test
    public void shouldFilterOutNonQueryableEndpoints()
    {
        runTest(new Testcase.Builder()
                .keyspace("ks1")
                .replicationStrategy(new NetworkTopologyStrategy("ks1", Map.of("DC", "5")))
                .indexStatus(Map.of(
                        InetAddressAndPort.getByNameUnchecked("127.0.0.251"),
                        Map.of(
                                "ks1.idx1", Index.Status.UNKNOWN,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.FULL_REBUILD_STARTED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.252"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.UNKNOWN,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.253"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.254"),
                        Map.of(
                                "ks1.idx1", Index.Status.BUILD_FAILED,
                                "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                "ks1.idx3", Index.Status.UNKNOWN
                        ),
                        InetAddressAndPort.getByNameUnchecked("127.0.0.255"),
                        Map.of(
                                "ks1.idx1", Index.Status.UNKNOWN,
                                "ks1.idx2", Index.Status.UNKNOWN,
                                "ks1.idx3", Index.Status.UNKNOWN
                        )
                ))
                .numRequired(1)
                .expected(EndpointsForRange.of(
                        // successful
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.253")),

                        // queryable, but unknown
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.252")),
                        full(InetAddressAndPort.getByNameUnchecked("127.0.0.255"))
                ))
                .build()
        );
    }

    @Test
    public void shouldThrowWhenNotEnoughQueryableEndpoints()
    {
        assertThatThrownBy(() ->
                runTest(new Testcase.Builder()
                        .keyspace("ks1")
                        .replicationStrategy(new NetworkTopologyStrategy("ks1", Map.of("DC", "5")))
                        .indexStatus(Map.of(
                                InetAddressAndPort.getByNameUnchecked("127.0.0.251"),
                                Map.of(
                                        "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx2", Index.Status.UNKNOWN,
                                        "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                                ),
                                InetAddressAndPort.getByNameUnchecked("127.0.0.252"),
                                Map.of(
                                        "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx2", Index.Status.BUILD_FAILED,
                                        "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                                ),
                                InetAddressAndPort.getByNameUnchecked("127.0.0.253"),
                                Map.of(
                                        "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                                ),
                                InetAddressAndPort.getByNameUnchecked("127.0.0.254"),
                                Map.of(
                                        "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx3", Index.Status.BUILD_FAILED
                                ),
                                InetAddressAndPort.getByNameUnchecked("127.0.0.255"),
                                Map.of(
                                        "ks1.idx1", Index.Status.UNKNOWN,
                                        "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                                )
                        ))
                        .numRequired(4)
                        .build()))
                .isInstanceOf(ReadFailureException.class)
                .hasMessageStartingWith("Operation failed")
                .hasMessageContaining("INDEX_NOT_AVAILABLE from /127.0.0.252:7000")
                .hasMessageContaining("INDEX_NOT_AVAILABLE from /127.0.0.254:7000");
    }

    @Test
    public void shouldThrowWhenNoQueryableEndpoints()
    {
        assertThatThrownBy(() ->
                runTest(new Testcase.Builder()
                        .keyspace("ks1")
                        .replicationStrategy(new NetworkTopologyStrategy("ks1", Map.of("DC", "3")))
                        .indexStatus(Map.of(
                                InetAddressAndPort.getByNameUnchecked("127.0.0.253"),
                                Map.of(
                                        "ks1.idx1", Index.Status.DROPPED,
                                        "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                                ),
                                InetAddressAndPort.getByNameUnchecked("127.0.0.254"),
                                Map.of(
                                        "ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx2", Index.Status.BUILD_SUCCEEDED,
                                        "ks1.idx3", Index.Status.BUILD_FAILED
                                ),
                                InetAddressAndPort.getByNameUnchecked("127.0.0.255"),
                                Map.of(
                                        "ks1.idx1", Index.Status.UNKNOWN,
                                        "ks1.idx2", Index.Status.FULL_REBUILD_STARTED,
                                        "ks1.idx3", Index.Status.BUILD_SUCCEEDED
                                )
                        ))
                        .numRequired(1)
                        .build()))
                .isInstanceOf(ReadFailureException.class)
                .hasMessageStartingWith("Operation failed")
                .hasMessageContaining("INDEX_NOT_AVAILABLE from /127.0.0.253:7000")
                .hasMessageContaining("INDEX_NOT_AVAILABLE from /127.0.0.254:7000")
                .hasMessageContaining("INDEX_BUILD_IN_PROGRESS from /127.0.0.255:7000");
    }

    void runTest(Testcase testcase)
    {
        // get all endpoints from indexStatus
        Set<Replica> replicas = testcase.indexStatus.keySet()
                .stream()
                .map(ReplicaUtils::full)
                .collect(Collectors.toSet());

        // get all indexes from one of the entries of indexStatus
        Set<Index> indexes = testcase.indexStatus.entrySet().iterator().next()
                .getValue()
                .keySet()
                .stream()
                .map(identifier -> mockedIndex(identifier.split("\\.")[1]))
                .collect(Collectors.toSet());

        // send indexStatus for each endpoint
        testcase.indexStatus.forEach((endpoint, indexStatusMap) ->
        {
            String serialized = legacyStatusFormat ? JsonUtils.writeAsJsonString(indexStatusMap) 
                                                   : IndexStatusManager.toSerializedFormat(indexStatusMap);
            IndexStatusManager.instance.receivePeerIndexStatus(endpoint, VersionedValue.unsafeMakeVersionedValue(serialized, 1));
        });

        // sort the replicas here, so that we can assert the order later
        EndpointsForRange endpoints = EndpointsForRange.copyOf(new TreeSet<>(replicas));
        Keyspace ks = mockedKeyspace(testcase.keyspace, testcase.replicationStrategy);
        Index.QueryPlan qp = mockedQueryPlan(indexes);
        ConsistencyLevel cl = mockedConsistencyLevel(testcase.numRequired);

        EndpointsForRange actual = IndexStatusManager.instance.filterForQuery(endpoints, ks, qp, cl);

        assertArrayEquals(
                testcase.expected.stream().toArray(),
                actual.stream().toArray()
        );
    }

    Keyspace mockedKeyspace(String name, AbstractReplicationStrategy replicationStrategy)
    {
        Keyspace mock = Mockito.mock(Keyspace.class);
        Mockito.when(mock.getName()).thenReturn(name);
        Mockito.when(mock.getReplicationStrategy()).thenReturn(replicationStrategy);
        return mock;
    }

    Index mockedIndex(String name)
    {
        Index mock = Mockito.mock(Index.class);

        Mockito.when(mock.getIndexMetadata())
                .thenReturn(IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.KEYS, null));

        Mockito.when(mock.isQueryable(Mockito.any()))
                .thenAnswer(invocationOnMock ->
                {
                    Index.Status status = invocationOnMock.getArgument(0);
                    return (status == Index.Status.BUILD_SUCCEEDED || status == Index.Status.UNKNOWN);
                });

        return mock;
    }

    Index.QueryPlan mockedQueryPlan(Set<Index> indexes)
    {
        Index.QueryPlan mock = Mockito.mock(Index.QueryPlan.class);
        Mockito.when(mock.getIndexes()).thenReturn(indexes);
        return mock;
    }

    ConsistencyLevel mockedConsistencyLevel(int required)
    {
        ConsistencyLevel mock = Mockito.mock(ConsistencyLevel.class);
        Mockito.when(mock.blockFor(Mockito.any())).thenReturn(required);
        return mock;
    }

    @Test
    public void shouldFailWhenTooManyIndexesExceedGossipLimit()
    {
        Map<String, Index.Status> statusMap = new HashMap<>();
        for (int ks = 0; ks < 100; ks++)
        {
            for (int idx = 0; idx < 200; idx++)
            {
                statusMap.put("keyspace_" + ks + ".my_table_index_name" + idx, Index.Status.BUILD_SUCCEEDED);
            }
        }

        String serialized = IndexStatusManager.toSerializedFormat(statusMap);
        byte[] utf8Bytes = serialized.getBytes(StandardCharsets.UTF_8);

        assertTrue("Serialized size " + utf8Bytes.length + " should exceed 65535",
                   utf8Bytes.length > 65535);

        VersionedValue value = VersionedValue.unsafeMakeVersionedValue(serialized, 1);
        DataOutputBuffer out = new DataOutputBuffer();

        assertThatThrownBy(() -> VersionedValue.serializer.serialize(value, out, 0))
                .isInstanceOf(UTFDataFormatException.class);
    }

    @Test
    public void testVersionGatingAllowsWriteFor6x()
    {
        assertTrue(IndexStatusManager.shouldWriteToIndexTablesForTesting(new CassandraVersion("6.0.0")));
        assertTrue(IndexStatusManager.shouldWriteToIndexTablesForTesting(new CassandraVersion("6.1.0")));
        assertTrue(IndexStatusManager.shouldWriteToIndexTablesForTesting(new CassandraVersion("7.0.0")));
    }

    @Test
    public void testVersionGatingBlocksWriteForPre6x()
    {
        assertFalse(IndexStatusManager.shouldWriteToIndexTablesForTesting(new CassandraVersion("5.0.0")));
        assertFalse(IndexStatusManager.shouldWriteToIndexTablesForTesting(new CassandraVersion("5.0.2")));
        assertFalse(IndexStatusManager.shouldWriteToIndexTablesForTesting(new CassandraVersion("4.1.0")));
    }

    @Test
    public void testProcessEventsUpdatesPeerIndexStatus()
    {
        IndexStatusManager manager = IndexStatusManager.instance;
        InetAddressAndPort peer = InetAddressAndPort.getByNameUnchecked("127.0.0.100");

        Map<String, Index.Status> peerStatuses = new HashMap<>();
        manager.peerIndexStatus.put(peer, peerStatuses);

        assertEquals(Index.Status.UNKNOWN, manager.getIndexStatus(peer, "ks1", "idx1"));
        peerStatuses.put("ks1.idx1", Index.Status.BUILD_SUCCEEDED);
        assertEquals(Index.Status.BUILD_SUCCEEDED, manager.getIndexStatus(peer, "ks1", "idx1"));
    }

    @Test
    public void testProcessEventsHandlesDropped()
    {
        IndexStatusManager manager = IndexStatusManager.instance;
        InetAddressAndPort peer = InetAddressAndPort.getByNameUnchecked("127.0.0.101");

        Map<String, Index.Status> peerStatuses = new HashMap<>();
        peerStatuses.put("ks1.idx1", Index.Status.BUILD_SUCCEEDED);
        manager.peerIndexStatus.put(peer, peerStatuses);

        assertEquals(Index.Status.BUILD_SUCCEEDED, manager.getIndexStatus(peer, "ks1", "idx1"));

        peerStatuses.remove("ks1.idx1");

        assertEquals(Index.Status.UNKNOWN, manager.getIndexStatus(peer, "ks1", "idx1"));
    }

    @Test
    public void testResetLastPollTimestamp()
    {
        IndexStatusManager manager = IndexStatusManager.instance;
        manager.resetLastPollTimestamp();
    }
}
