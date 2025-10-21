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

package org.apache.cassandra.distributed.test.log;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FetchLogFromPeersTest extends TestBaseImpl
{
    public enum ClusterState { COORDINATOR_BEHIND, REPLICA_BEHIND }
    public enum Operation { READ, WRITE }

    public static int coordinator(ClusterState clusterState)
    {
        switch (clusterState)
        {
            case COORDINATOR_BEHIND:
                return 2;
            case REPLICA_BEHIND:
                return 3;
        }
        throw new IllegalStateException();
    }

    @Test
    public void catchupCoordinatorBehindTestPlacements() throws Exception
    {
        // Only runs in non-vnode configuration, because whether node2 is a replica or not before/after
        // depends on the number of tokens but this is set externally to the test. The actual behaviour
        // under test is completely orthogonal to the actual number of tokens though.
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));

            // Create divergence between node 1 and 2
            cluster.filters().inbound().from(1).to(2).drop();

            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup(cluster);

            cluster.get(1).shutdown().get();

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(3), 1, 2);
            Assert.assertEquals(2,
                                cluster.stream().filter(i -> i.config().num() != 1).map(i -> {
                                    return i.callOnInstance(() -> {
                                        return ClusterMetadata.current().epoch.getEpoch();
                                    });
                                }).collect(Collectors.toSet()).size());

            // node2 is behind, writing to it will cause a failure, but it will then catch up
            try
            {
                cluster.coordinator(2).execute(withKeyspace("insert into %s.tbl (id) values (3)"), ConsistencyLevel.QUORUM);
                fail("writing should fail");
            }
            catch (Exception writeTimeout)
            {
                Assert.assertTrue(writeTimeout.getMessage().contains("Operation timed out"));
            }
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(3), 1);
            Assert.assertEquals(1,
                                cluster.stream().filter(i -> i.config().num() != 1).map(i -> {
                                    return i.callOnInstance(() -> {
                                        return ClusterMetadata.current().epoch.getEpoch();
                                    });
                                }).collect(Collectors.toSet()).size());
            cluster.coordinator(2).execute(withKeyspace("insert into %s.tbl (id) values (3)"), ConsistencyLevel.QUORUM);
        }
    }

    @Test
    public void catchupCoordinatorAheadPlacementsReadTest() throws Exception
    {
        // Only runs in non-vnode configuration, because whether node2 is a replica or not before/after
        // depends on the number of tokens but this is set externally to the test. The actual behaviour
        // under test is completely orthogonal to the actual number of tokens though.
        try (Cluster cluster = init(builder().withNodes(4)
                                             .withoutVNodes()
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.filters().inbound().from(1).to(2).drop();
            AtomicInteger fetchedFromPeer = new AtomicInteger();
            cluster.filters().inbound().from(2).to(4).messagesMatching((from, to, msg) -> {
                if (msg.verb() == Verb.TCM_FETCH_PEER_LOG_REQ.id)
                    fetchedFromPeer.getAndIncrement();
                return false;
            }).drop().on();
            cluster.get(3).shutdown().get();
            cluster.get(4).nodetoolResult("assassinate", "127.0.0.3").asserts().success();

            cluster.get(1).shutdown().get();
            int before = fetchedFromPeer.get();
            cluster.coordinator(4).execute(withKeyspace("select * from %s.tbl where id = 6"), ConsistencyLevel.QUORUM);
            assertTrue(fetchedFromPeer.get() > before);
        }
    }

    @Test
    public void catchupCoordinatorAheadPlacementsWriteTest() throws Throwable
    {
        // Only runs in non-vnode configuration, because whether node2 is a replica or not before/after
        // depends on the number of tokens but this is set externally to the test. The actual behaviour
        // under test is completely orthogonal to the actual number of tokens though.
        try (Cluster cluster = init(builder().withNodes(4)
                                             .withoutVNodes()
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key)"));
            cluster.filters().inbound().from(1).to(2).drop();
            AtomicInteger fetchedFromPeer = new AtomicInteger();
            cluster.filters().inbound().from(2).to(4).messagesMatching((from, to, msg) -> {
                if (msg.verb() == Verb.TCM_FETCH_PEER_LOG_REQ.id)
                    fetchedFromPeer.getAndIncrement();
                return false;
            }).drop().on();

            cluster.get(3).shutdown().get();
            cluster.get(4).nodetoolResult("assassinate", "127.0.0.3").asserts().success();

            cluster.get(1).shutdown().get();
            // node4 is ahead - node2 should catch up and allow the write
            int before = fetchedFromPeer.get();
            long mark = cluster.get(2).logs().mark();
            cluster.coordinator(4).execute(withKeyspace("insert into %s.tbl (id) values (6)"), ConsistencyLevel.QUORUM);
            assertTrue(cluster.get(2).logs().grep(mark, "Routing is correct, but coordinator needs to catch-up to maintain consistency.").getResult().isEmpty());
            assertTrue(fetchedFromPeer.get() > before);

            // Should succeed after blocking catch-up.
            cluster.coordinator(4).execute(withKeyspace("insert into %s.tbl (id) values (6)"), ConsistencyLevel.QUORUM);
        }
    }


    @Test
    public void catchupWithSnapshot() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            executeAlters(cluster);

            cluster.filters().inbound().to(2).messagesMatching((from, to, message) ->
                cluster.get(2).callOnInstance(() -> {
                    Message<?> decoded = Instance.deserializeMessage(message);
                    if (decoded.payload instanceof LogState)
                    {
                        LogState logState = (LogState) decoded.payload;
                        // drop every other replication message to make sure pending buffer is non-consecutive
                        if (decoded.epoch().getEpoch() % 2 == 0 &&
                            logState.entries.stream().noneMatch((e) -> e.transform instanceof TriggerSnapshot))
                            return false;
                    }
                    return true;
                })
            ).drop();
            executeAlters(cluster);
            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().triggerSnapshot());
            executeAlters(cluster);
            cluster.filters().reset();

            long epoch = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().fetchLogFromPeerAsync(InetAddressAndPort.getByNameUnchecked("127.0.0.1"), Epoch.create(epoch)));

            long epochNode2 = 0;
            while (epochNode2 < epoch)
            {
                epochNode2 = cluster.get(2).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
                if (epochNode2 < epoch)
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Test
    public void skipEntryAndCatchupWithSnapshot() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));

            IMessageFilters.Filter filter = cluster.filters().inbound().to(2).drop();
            cluster.coordinator(1).execute(withKeyspace("alter table %s.tbl with comment='"+UUID.randomUUID()+"'"), ConsistencyLevel.QUORUM);
            filter.off();

            filter = cluster.filters().inbound().to(2).verbs(Verb.TCM_FETCH_PEER_LOG_RSP.id, Verb.TCM_FETCH_CMS_LOG_REQ.id).drop();
            executeAlters(cluster);
            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().triggerSnapshot());
            executeAlters(cluster);
            filter.off();

            try
            {
                // Trigger a single read to make sure fetch or CMS logs can be fetched
                cluster.coordinator(2).execute(withKeyspace("select * from %s.tbl"), ConsistencyLevel.ALL);
            }
            catch (Throwable t)
            {
                // ignore coordinator behind
            }
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
        }
    }

    private static void executeAlters(Cluster cluster)
    {
        for (int i = 0; i < 10; i++)
            cluster.coordinator(1).execute(withKeyspace("alter table %s.tbl with comment='"+UUID.randomUUID()+"'"), ConsistencyLevel.QUORUM);
    }
}
