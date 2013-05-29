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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.Sets;

import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.repair.*;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.utils.FBUtilities;

/**
 * ActiveRepairService encapsulates "validating" (hashing) individual column families,
 * exchanging MerkleTrees with remote nodes via a tree request/response conversation,
 * and then triggering repairs for disagreeing ranges.
 *
 * The node where repair was invoked acts as the 'initiator,' where valid trees are sent after generation
 * and where the local and remote tree will rendezvous in rendezvous().
 * Once the trees rendezvous, a Differencer is executed and the service can trigger repairs
 * for disagreeing ranges.
 *
 * Tree comparison and repair triggering occur in the single threaded Stage.ANTI_ENTROPY.
 *
 * The steps taken to enact a repair are as follows:
 * 1. A repair is requested via JMX/nodetool:
 *   * The initiator sends TreeRequest messages to all neighbors of the target node: when a node
 *     receives a TreeRequest, it will perform a validation (read-only) compaction to immediately validate
 *     the column family.  This is performed on the CompactionManager ExecutorService.
 * 2. The validation process builds the merkle tree by:
 *   * Calling Validator.prepare(), which samples the column family to determine key distribution,
 *   * Calling Validator.add() in order for rows in repair range in the column family,
 *   * Calling Validator.complete() to indicate that all rows have been added.
 *     * Calling complete() indicates that a valid MerkleTree has been created for the column family.
 *     * The valid tree is returned to the requesting node via a TreeResponse.
 * 3. When a node receives a tree response, it passes the tree to rendezvous() to see if all responses are
 *    received. Once the initiator receives all responses, it creates Differencers on every tree pair combination.
 * 4. Differencers are executed in Stage.ANTI_ENTROPY, to compare the given two trees, and perform repair via the
 *    streaming api.
 */
public class ActiveRepairService
{
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService();

    private static final ThreadPoolExecutor executor;
    static
    {
        executor = new JMXConfigurableThreadPoolExecutor(4,
                                                         60,
                                                         TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<Runnable>(),
                                                         new NamedThreadFactory("AntiEntropySessions"),
                                                         "internal");
    }

    public static enum Status
    {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }

    /**
     * A map of active session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions;

    /**
     * Protected constructor. Use ActiveRepairService.instance.
     */
    protected ActiveRepairService()
    {
        sessions = new ConcurrentHashMap<>();
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairFuture submitRepairSession(Range<Token> range, String keyspace, boolean isSequential, boolean isLocal, String... cfnames)
    {
        RepairSession session = new RepairSession(range, keyspace, isSequential, isLocal, cfnames);
        if (session.endpoints.isEmpty())
            return null;
        RepairFuture futureTask = new RepairFuture(session);
        executor.execute(futureTask);
        return futureTask;
    }

    public void addToActiveSessions(RepairSession session)
    {
        sessions.put(session.getId(), session);
        Gossiper.instance.register(session);
        FailureDetector.instance.registerFailureDetectionEventListener(session);
    }

    public void removeFromActiveSessions(RepairSession session)
    {
        FailureDetector.instance.unregisterFailureDetectionEventListener(session);
        Gossiper.instance.unregister(session);
        sessions.remove(session.getId());
    }

    public void terminateSessions()
    {
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown();
        }
    }

    // for testing only. Create a session corresponding to a fake request and
    // add it to the sessions (avoid NPE in tests)
    RepairFuture submitArtificialRepairSession(RepairJobDesc desc)
    {
        RepairSession session = new RepairSession(desc.sessionId, desc.range, desc.keyspace, false, false, new String[]{desc.columnFamily});
        sessions.put(session.getId(), session);
        RepairFuture futureTask = new RepairFuture(session);
        executor.execute(futureTask);
        return futureTask;
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param table table to repair
     * @param toRepair token to repair
     * @param isLocal need to use only nodes from local datacenter
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getNeighbors(String table, Range<Token> toRepair, boolean isLocal)
    {
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(table);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : ss.getLocalRanges(table))
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException("Requested range intersects a local range but is not fully contained in one; this would lead to imprecise repair");
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

        Set<InetAddress> neighbors = new HashSet<>(replicaSets.get(rangeSuperSet));
        neighbors.remove(FBUtilities.getBroadcastAddress());

        if (isLocal)
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
            return Sets.intersection(neighbors, localEndpoints);
        }

        return neighbors;
    }

    public void handleMessage(InetAddress endpoint, RepairMessage message)
    {
        RepairJobDesc desc = message.desc;
        RepairSession session = sessions.get(desc.sessionId);
        if (session == null)
            return;
        switch (message.messageType)
        {
            case VALIDATION_COMPLETE:
                ValidationComplete validation = (ValidationComplete) message;
                session.validationComplete(desc, endpoint, validation.tree);
                break;
            case SYNC_COMPLETE:
                // one of replica is synced.
                SyncComplete sync = (SyncComplete) message;
                session.syncComplete(desc, sync.nodes, sync.success);
                break;
            default:
                break;
        }
    }
}
