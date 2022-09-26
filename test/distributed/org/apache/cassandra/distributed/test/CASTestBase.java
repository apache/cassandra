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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_PROPOSE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_REPAIR_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PROPOSE_REQ;
import static org.apache.cassandra.net.Verb.READ_REQ;

public abstract class CASTestBase extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(CASTestBase.class);

    static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);

    static String tableName()
    {
        return tableName("tbl");
    }

    static String tableName(String prefix)
    {
        return prefix + TABLE_COUNTER.getAndIncrement();
    }

    static void repair(Cluster cluster, String tableName, int pk, int repairWith, int repairWithout)
    {
        IMessageFilters.Filter filter = cluster.filters().verbs(
                PAXOS2_REPAIR_REQ.id,
                PAXOS2_PREPARE_REQ.id, PAXOS_PREPARE_REQ.id, READ_REQ.id).from(repairWith).to(repairWithout).drop();
        cluster.get(repairWith).runOnInstance(() -> {
            TableMetadata schema = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).metadata.get();
            DecoratedKey key = schema.partitioner.decorateKey(Int32Type.instance.decompose(pk));
            try
            {
                PaxosRepair.create(SERIAL, key, null, schema).start().await();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        });
        filter.off();
    }

    static int pk(Cluster cluster, int lb, int ub)
    {
        return pk(cluster.get(lb), cluster.get(ub));
    }

    static int pk(IInstance lb, IInstance ub)
    {
        return pk(Murmur3Partitioner.instance.getTokenFactory().fromString(lb.config().getString("initial_token")),
                Murmur3Partitioner.instance.getTokenFactory().fromString(ub.config().getString("initial_token")));
    }

    static int pk(Token lb, Token ub)
    {
        int pk = 0;
        Token pkt;
        while (lb.compareTo(pkt = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk))) >= 0 || ub.compareTo(pkt) < 0)
            ++pk;
        return pk;
    }

    int[] to(int ... nodes)
    {
        return nodes;
    }


    private static final IMessageFilters.Matcher LOG_DROPPED = (from, to, message) -> { logger.info("Dropping {} from {} to {}", Verb.fromId(message.verb()), from, to); return true; };
    AutoCloseable drop(Cluster cluster, int from, int[] toPrepareAndRead, int[] toPropose, int[] toCommit)
    {
        IMessageFilters.Filter filter1 = cluster.filters().verbs(PAXOS2_PREPARE_REQ.id, PAXOS_PREPARE_REQ.id, READ_REQ.id).from(from).to(toPrepareAndRead).messagesMatching(LOG_DROPPED).drop();
        IMessageFilters.Filter filter2 = cluster.filters().verbs(PAXOS2_PROPOSE_REQ.id, PAXOS_PROPOSE_REQ.id).from(from).to(toPropose).messagesMatching(LOG_DROPPED).drop();
        IMessageFilters.Filter filter3 = cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(from).to(toCommit).messagesMatching(LOG_DROPPED).drop();
        return () -> {
            filter1.off();
            filter2.off();
            filter3.off();
        };
    }

    AutoCloseable drop(Cluster cluster, int from, int[] toPrepare, int[] toRead, int[] toPropose, int[] toCommit)
    {
        IMessageFilters.Filter filter1 = cluster.filters().verbs(PAXOS2_PREPARE_REQ.id, PAXOS_PREPARE_REQ.id).from(from).to(toPrepare).drop();
        IMessageFilters.Filter filter2 = cluster.filters().verbs(READ_REQ.id).from(from).to(toRead).drop();
        IMessageFilters.Filter filter3 = cluster.filters().verbs(PAXOS2_PROPOSE_REQ.id, PAXOS_PROPOSE_REQ.id).from(from).to(toPropose).drop();
        IMessageFilters.Filter filter4 = cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(from).to(toCommit).drop();
        return () -> {
            filter1.off();
            filter2.off();
            filter3.off();
            filter4.off();
        };
    }

    public static void addToRing(boolean bootstrapping, IInstance peer)
    {
        try
        {
            IInstanceConfig config = peer.config();
            IPartitioner partitioner = FBUtilities.newPartitioner(config.getString("partitioner"));
            Token token = partitioner.getTokenFactory().fromString(config.getString("initial_token"));
            InetAddressAndPort address = InetAddressAndPort.getByAddress(peer.broadcastAddress());

            UUID hostId = config.hostId();
            Gossiper.runInGossipStageBlocking(() -> {
                Gossiper.instance.initializeNodeUnsafe(address, hostId, 1);
                Gossiper.instance.injectApplicationState(address,
                                                         ApplicationState.TOKENS,
                                                         new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(token)));
                VersionedValue status = bootstrapping
                                        ? new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(Collections.singleton(token))
                                        : new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token));
                Gossiper.instance.injectApplicationState(address, ApplicationState.STATUS, status);
                StorageService.instance.onChange(address, ApplicationState.STATUS, status);
                Gossiper.instance.realMarkAlive(address, Gossiper.instance.getEndpointStateForEndpoint(address));
            });
            int version = Math.min(MessagingService.current_version, peer.getMessagingVersion());
            MessagingService.instance().versions.set(address, version);

            if (!bootstrapping)
                assert StorageService.instance.getTokenMetadata().isMember(address);
            PendingRangeCalculatorService.instance.blockUntilFinished();
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public static void assertVisibleInRing(IInstance peer)
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByAddress(peer.broadcastAddress());
        Assert.assertTrue(Gossiper.instance.isAlive(endpoint));
    }

    // reset gossip state so we know of the node being alive only
    public static void removeFromRing(IInstance peer)
    {
        try
        {
            IInstanceConfig config = peer.config();
            IPartitioner partitioner = FBUtilities.newPartitioner(config.getString("partitioner"));
            Token token = partitioner.getTokenFactory().fromString(config.getString("initial_token"));
            InetAddressAndPort address = InetAddressAndPort.getByAddress(config.broadcastAddress());

            Gossiper.runInGossipStageBlocking(() -> {
                StorageService.instance.onChange(address,
                                                 ApplicationState.STATUS,
                                                 new VersionedValue.VersionedValueFactory(partitioner).left(Collections.singleton(token), 0L, 0));
                Gossiper.instance.unsafeAnnulEndpoint(address);
                Gossiper.instance.realMarkAlive(address, new EndpointState(new HeartBeatState(0, 0)));
            });
            PendingRangeCalculatorService.instance.blockUntilFinished();
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public static void assertNotVisibleInRing(IInstance peer)
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByAddress(peer.broadcastAddress());
        Assert.assertFalse(Gossiper.instance.isAlive(endpoint));
    }

    public static void addToRingNormal(IInstance peer)
    {
        addToRing(false, peer);
        assert StorageService.instance.getTokenMetadata().isMember(InetAddressAndPort.getByAddress(peer.broadcastAddress()));
    }

    public static void addToRingBootstrapping(IInstance peer)
    {
        addToRing(true, peer);
    }
}
