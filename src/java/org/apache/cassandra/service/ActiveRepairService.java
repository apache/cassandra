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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Ref;

import org.apache.cassandra.utils.concurrent.Refs;

/**
 * ActiveRepairService is the starting point for manual "active" repairs.
 *
 * Each user triggered repair will correspond to one or multiple repair session,
 * one for each token range to repair. On repair session might repair multiple
 * column families. For each of those column families, the repair session will
 * request merkle trees for each replica of the range being repaired, diff those
 * trees upon receiving them, schedule the streaming ofthe parts to repair (based on
 * the tree diffs) and wait for all those operation. See RepairSession for more
 * details.
 *
 * The creation of a repair session is done through the submitRepairSession that
 * returns a future on the completion of that session.
 */
public class ActiveRepairService
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService(FailureDetector.instance, Gossiper.instance);

    public static final long UNREPAIRED_SSTABLE = 0;

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions = new ConcurrentHashMap<>();

    private final ConcurrentMap<UUID, ParentRepairSession> parentRepairSessions = new ConcurrentHashMap<>();

    private final IFailureDetector failureDetector;
    private final Gossiper gossiper;

    public ActiveRepairService(IFailureDetector failureDetector, Gossiper gossiper)
    {
        this.failureDetector = failureDetector;
        this.gossiper = gossiper;
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairSession submitRepairSession(UUID parentRepairSession,
                                             Range<Token> range,
                                             String keyspace,
                                             RepairParallelism parallelismDegree,
                                             Set<InetAddress> endpoints,
                                             long repairedAt,
                                             ListeningExecutorService executor,
                                             String... cfnames)
    {
        if (endpoints.isEmpty())
            return null;

        if (cfnames.length == 0)
            return null;

        final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, endpoints, repairedAt, cfnames);

        sessions.put(session.getId(), session);
        // register listeners
        gossiper.register(session);
        failureDetector.registerFailureDetectionEventListener(session);

        // unregister listeners at completion
        session.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                failureDetector.unregisterFailureDetectionEventListener(session);
                gossiper.unregister(session);
                sessions.remove(session.getId());
            }
        }, MoreExecutors.sameThreadExecutor());
        session.start(executor);
        return session;
    }

    public synchronized void terminateSessions()
    {
        Throwable cause = new IOException("Terminate session is called");
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown(cause);
        }
        parentRepairSessions.clear();
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param keyspaceName keyspace to repair
     * @param toRepair token to repair
     * @param dataCenters the data centers to involve in the repair
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getNeighbors(String keyspaceName, Range<Token> toRepair, Collection<String> dataCenters, Collection<String> hosts)
    {
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : ss.getLocalRanges(keyspaceName))
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

        if (dataCenters != null && !dataCenters.isEmpty())
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String,InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            for (String dc : dataCenters)
            {
                Collection<InetAddress> c = dcEndpointsMap.get(dc);
                if (c != null)
                   dcEndpoints.addAll(c);
            }
            return Sets.intersection(neighbors, dcEndpoints);
        }
        else if (hosts != null && !hosts.isEmpty())
        {
            Set<InetAddress> specifiedHost = new HashSet<>();
            for (final String host : hosts)
            {
                try
                {
                    final InetAddress endpoint = InetAddress.getByName(host.trim());
                    if (endpoint.equals(FBUtilities.getBroadcastAddress()) || neighbors.contains(endpoint))
                        specifiedHost.add(endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalArgumentException("Unknown host specified " + host, e);
                }
            }

            if (!specifiedHost.contains(FBUtilities.getBroadcastAddress()))
                throw new IllegalArgumentException("The current host must be part of the repair");

            if (specifiedHost.size() <= 1)
            {
                String msg = "Repair requires at least two endpoints that are neighbours before it can continue, the endpoint used for this repair is %s, " +
                             "other available neighbours are %s but these neighbours were not part of the supplied list of hosts to use during the repair (%s).";
                throw new IllegalArgumentException(String.format(msg, specifiedHost, neighbors, hosts));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddress());
            return specifiedHost;

        }

        return neighbors;
    }

    public synchronized UUID prepareForRepair(Set<InetAddress> endpoints, RepairOption options, List<ColumnFamilyStore> columnFamilyStores)
    {
        UUID parentRepairSession = UUIDGen.getTimeUUID();
        registerParentRepairSession(parentRepairSession, columnFamilyStores, options.getRanges(), options.isIncremental());
        final CountDownLatch prepareLatch = new CountDownLatch(endpoints.size());
        final AtomicBoolean status = new AtomicBoolean(true);
        final Set<String> failedNodes = Collections.synchronizedSet(new HashSet<String>());
        IAsyncCallbackWithFailure callback = new IAsyncCallbackWithFailure()
        {
            public void response(MessageIn msg)
            {
                prepareLatch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }

            public void onFailure(InetAddress from)
            {
                status.set(false);
                failedNodes.add(from.getHostAddress());
                prepareLatch.countDown();
            }
        };

        List<UUID> cfIds = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores)
            cfIds.add(cfs.metadata.cfId);

        for (InetAddress neighbour : endpoints)
        {
            PrepareMessage message = new PrepareMessage(parentRepairSession, cfIds, options.getRanges(), options.isIncremental());
            MessageOut<RepairMessage> msg = message.createMessage();
            MessagingService.instance().sendRRWithFailure(msg, neighbour, callback);
        }
        try
        {
            prepareLatch.await(1, TimeUnit.HOURS);
        }
        catch (InterruptedException e)
        {
            parentRepairSessions.remove(parentRepairSession);
            throw new RuntimeException("Did not get replies from all endpoints. List of failed endpoint(s): " + failedNodes.toString(), e);
        }

        if (!status.get())
        {
            parentRepairSessions.remove(parentRepairSession);
            throw new RuntimeException("Did not get positive replies from all endpoints. List of failed endpoint(s): " + failedNodes.toString());
        }

        return parentRepairSession;
    }

    public void registerParentRepairSession(UUID parentRepairSession, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental)
    {
        parentRepairSessions.put(parentRepairSession, new ParentRepairSession(columnFamilyStores, ranges, isIncremental, System.currentTimeMillis()));
    }

    public Set<SSTableReader> currentlyRepairing(UUID cfId, UUID parentRepairSession)
    {
        Set<SSTableReader> repairing = new HashSet<>();
        for (Map.Entry<UUID, ParentRepairSession> entry : parentRepairSessions.entrySet())
        {
            Collection<SSTableReader> sstables = entry.getValue().sstableMap.get(cfId);
            if (sstables != null && !entry.getKey().equals(parentRepairSession))
                repairing.addAll(sstables);
        }
        return repairing;
    }

    public void finishParentSession(UUID parentSession, Set<InetAddress> neighbors, Collection<Range<Token>> successfulRanges)
    {
        try
        {
            for (InetAddress neighbor : neighbors)
            {
                AnticompactionRequest acr = new AnticompactionRequest(parentSession, successfulRanges);
                MessageOut<RepairMessage> req = acr.createMessage();
                MessagingService.instance().sendOneWay(req, neighbor);
            }
            List<Future<?>> futures = doAntiCompaction(parentSession, successfulRanges);
            FBUtilities.waitOnFutures(futures);
        }
        finally
        {
            parentRepairSessions.remove(parentSession);
        }
    }

    public ParentRepairSession getParentRepairSession(UUID parentSessionId)
    {
        return parentRepairSessions.get(parentSessionId);
    }

    public synchronized ParentRepairSession removeParentRepairSession(UUID parentSessionId)
    {
        return parentRepairSessions.remove(parentSessionId);
    }

    public List<Future<?>> doAntiCompaction(UUID parentRepairSession, Collection<Range<Token>> successfulRanges)
    {
        assert parentRepairSession != null;
        ParentRepairSession prs = getParentRepairSession(parentRepairSession);
        assert prs.ranges.containsAll(successfulRanges) : "Trying to perform anticompaction on unknown ranges";

        List<Future<?>> futures = new ArrayList<>();
        // if we don't have successful repair ranges, then just skip anticompaction
        if (successfulRanges.isEmpty())
            return futures;
        for (Map.Entry<UUID, ColumnFamilyStore> columnFamilyStoreEntry : prs.columnFamilyStores.entrySet())
        {
            Refs<SSTableReader> sstables = prs.getAndReferenceSSTables(columnFamilyStoreEntry.getKey());
            ColumnFamilyStore cfs = columnFamilyStoreEntry.getValue();
            futures.add(CompactionManager.instance.submitAntiCompaction(cfs, successfulRanges, sstables, prs.repairedAt));
        }

        return futures;
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

    public static class ParentRepairSession
    {
        private final Map<UUID, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        private final Collection<Range<Token>> ranges;
        private final Map<UUID, Set<SSTableReader>> sstableMap = new HashMap<>();
        public final long repairedAt;
        public final boolean isIncremental;

        public ParentRepairSession(List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt)
        {
            for (ColumnFamilyStore cfs : columnFamilyStores)
                this.columnFamilyStores.put(cfs.metadata.cfId, cfs);
            this.ranges = ranges;
            this.repairedAt = repairedAt;
            this.isIncremental = isIncremental;
        }

        public void addSSTables(UUID cfId, Set<SSTableReader> sstables)
        {
            Set<SSTableReader> existingSSTables = this.sstableMap.get(cfId);
            if (existingSSTables == null)
                existingSSTables = new HashSet<>();
            existingSSTables.addAll(sstables);
            this.sstableMap.put(cfId, existingSSTables);
        }

        public synchronized Refs<SSTableReader> getAndReferenceSSTables(UUID cfId)
        {
            Set<SSTableReader> sstables = sstableMap.get(cfId);
            Iterator<SSTableReader> sstableIterator = sstables.iterator();
            ImmutableMap.Builder<SSTableReader, Ref<SSTableReader>> references = ImmutableMap.builder();
            while (sstableIterator.hasNext())
            {
                SSTableReader sstable = sstableIterator.next();
                if (!new File(sstable.descriptor.filenameFor(Component.DATA)).exists())
                {
                    sstableIterator.remove();
                }
                else
                {
                    Ref<SSTableReader> ref = sstable.tryRef();
                    if (ref == null)
                        sstableIterator.remove();
                    else
                        references.put(sstable, ref);
                }
            }
            return new Refs<>(references.build());
        }

        @Override
        public String toString()
        {
            return "ParentRepairSession{" +
                    "columnFamilyStores=" + columnFamilyStores +
                    ", ranges=" + ranges +
                    ", sstableMap=" + sstableMap +
                    ", repairedAt=" + repairedAt +
                    '}';
        }
    }
}
