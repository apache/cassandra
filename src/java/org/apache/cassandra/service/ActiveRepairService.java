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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.AnticompactionTask;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Clock;
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
public class ActiveRepairService implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    /**
     * @deprecated this statuses are from the previous JMX notification service,
     * which will be deprecated on 4.0. For statuses of the new notification
     * service, see {@link org.apache.cassandra.streaming.StreamEvent.ProgressEvent}
     */
    @Deprecated
    public static enum Status
    {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }
    private boolean registeredForEndpointChanges = false;

    public static CassandraVersion SUPPORTS_GLOBAL_PREPARE_FLAG_VERSION = new CassandraVersion("2.2.1");

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
                                             Collection<Range<Token>> range,
                                             String keyspace,
                                             RepairParallelism parallelismDegree,
                                             Set<InetAddress> endpoints,
                                             long repairedAt,
                                             boolean pullRepair,
                                             ListeningExecutorService executor,
                                             String... cfnames)
    {
        if (endpoints.isEmpty())
            return null;

        if (cfnames.length == 0)
            return null;

        final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, endpoints, repairedAt, pullRepair, cfnames);

        sessions.put(session.getId(), session);
        // register listeners
        registerOnFdAndGossip(session);

        // remove session at completion
        session.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                sessions.remove(session.getId());
            }
        }, MoreExecutors.directExecutor());
        session.start(executor);
        return session;
    }

    private <T extends AbstractFuture &
               IEndpointStateChangeSubscriber &
               IFailureDetectionEventListener> void registerOnFdAndGossip(final T task)
    {
        gossiper.register(task);
        failureDetector.registerFailureDetectionEventListener(task);

        // unregister listeners at completion
        task.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                failureDetector.unregisterFailureDetectionEventListener(task);
                gossiper.unregister(task);
            }
        }, MoreExecutors.sameThreadExecutor());
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
     * @param keyspaceLocalRanges local-range for given keyspaceName
     * @param toRepair token to repair
     * @param dataCenters the data centers to involve in the repair
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getNeighbors(String keyspaceName, Collection<Range<Token>> keyspaceLocalRanges,
                                                Range<Token> toRepair, Collection<String> dataCenters,
                                                Collection<String> hosts)
    {
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : keyspaceLocalRanges)
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException(String.format("Requested range %s intersects a local range (%s) " +
                                                                 "but is not fully contained in one; this would lead to " +
                                                                 "imprecise repair. keyspace: %s", toRepair.toString(),
                                                                 range.toString(), keyspaceName));
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
                String msg = "Specified hosts %s do not share range %s needed for repair. Either restrict repair ranges " +
                             "with -st/-et options, or specify one of the neighbors that share this range with " +
                             "this node: %s.";
                throw new IllegalArgumentException(String.format(msg, hosts, toRepair, neighbors));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddress());
            return specifiedHost;

        }

        return neighbors;
    }

    public UUID prepareForRepair(UUID parentRepairSession, InetAddress coordinator, Set<InetAddress> endpoints, RepairOption options, List<ColumnFamilyStore> columnFamilyStores)
    {
        long timestamp = Clock.instance.currentTimeMillis();
        registerParentRepairSession(parentRepairSession, coordinator, columnFamilyStores, options.getRanges(), options.isIncremental(), timestamp, options.isGlobal());
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

            public void onFailure(InetAddress from, RequestFailureReason failureReason)
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
            if (FailureDetector.instance.isAlive(neighbour))
            {
                PrepareMessage message = new PrepareMessage(parentRepairSession, cfIds, options.getRanges(), options.isIncremental(), timestamp, options.isGlobal());
                MessageOut<RepairMessage> msg = message.createMessage();
                MessagingService.instance().sendRR(msg, neighbour, callback, TimeUnit.HOURS.toMillis(1), true);
            }
            else
            {
                // bailout early to avoid potentially waiting for a long time.
                failRepair(parentRepairSession, "Endpoint not alive: " + neighbour);
            }
        }

        try
        {
            // Failed repair is expensive so we wait for longer time.
            if (!prepareLatch.await(1, TimeUnit.HOURS)) {
                failRepair(parentRepairSession, "Did not get replies from all endpoints.");
            }
        }
        catch (InterruptedException e)
        {
            failRepair(parentRepairSession, "Interrupted while waiting for prepare repair response.");
        }

        if (!status.get())
        {
            failRepair(parentRepairSession, "Got negative replies from endpoints " + failedNodes);
        }

        return parentRepairSession;
    }

    private void failRepair(UUID parentRepairSession, String errorMsg) {
        removeParentRepairSession(parentRepairSession);
        throw new RuntimeException(errorMsg);
    }

    public synchronized void registerParentRepairSession(UUID parentRepairSession, InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long timestamp, boolean isGlobal)
    {
        if (!registeredForEndpointChanges)
        {
            Gossiper.instance.register(this);
            FailureDetector.instance.registerFailureDetectionEventListener(this);
            registeredForEndpointChanges = true;
        }

        if (!parentRepairSessions.containsKey(parentRepairSession))
        {
            parentRepairSessions.put(parentRepairSession, new ParentRepairSession(coordinator, columnFamilyStores, ranges, isIncremental, timestamp, isGlobal));
        }
    }

    public Set<SSTableReader> currentlyRepairing(UUID cfId, UUID parentRepairSession)
    {
        Set<SSTableReader> repairing = new HashSet<>();
        for (Map.Entry<UUID, ParentRepairSession> entry : parentRepairSessions.entrySet())
        {
            Collection<SSTableReader> sstables = entry.getValue().getActiveSSTables(cfId);
            if (sstables != null && !entry.getKey().equals(parentRepairSession))
                repairing.addAll(sstables);
        }
        return repairing;
    }

    /**
     * Run final process of repair.
     * This removes all resources held by parent repair session, after performing anti compaction if necessary.
     *
     * @param parentSession Parent session ID
     * @param neighbors Repair participants (not including self)
     * @param successfulRanges Ranges that repaired successfully
     */
    public synchronized ListenableFuture finishParentSession(UUID parentSession, Set<InetAddress> neighbors, Collection<Range<Token>> successfulRanges)
    {
        List<ListenableFuture<?>> tasks = new ArrayList<>(neighbors.size() + 1);
        for (InetAddress neighbor : neighbors)
        {
            AnticompactionTask task = new AnticompactionTask(parentSession, neighbor, successfulRanges);
            registerOnFdAndGossip(task);
            tasks.add(task);
            task.run(); // 'run' is just sending message
        }
        tasks.add(doAntiCompaction(parentSession, successfulRanges));
        return Futures.successfulAsList(tasks);
    }

    public ParentRepairSession getParentRepairSession(UUID parentSessionId)
    {
        ParentRepairSession session = parentRepairSessions.get(parentSessionId);
        // this can happen if a node thinks that the coordinator was down, but that coordinator got back before noticing
        // that it was down itself.
        if (session == null)
            throw new RuntimeException("Parent repair session with id = " + parentSessionId + " has failed.");

        return session;
    }

    /**
     * called when the repair session is done - either failed or anticompaction has completed
     *
     * clears out any snapshots created by this repair
     *
     * @param parentSessionId
     * @return
     */
    public synchronized ParentRepairSession removeParentRepairSession(UUID parentSessionId)
    {
        String snapshotName = parentSessionId.toString();
        for (ColumnFamilyStore cfs : getParentRepairSession(parentSessionId).columnFamilyStores.values())
        {
            if (cfs.snapshotExists(snapshotName))
                cfs.clearSnapshot(snapshotName);
        }
        return parentRepairSessions.remove(parentSessionId);
    }

    /**
     * Submit anti-compaction jobs to CompactionManager.
     * When all jobs are done, parent repair session is removed whether those are suceeded or not.
     *
     * @param parentRepairSession parent repair session ID
     * @return Future result of all anti-compaction jobs.
     */
    @SuppressWarnings("resource")
    public ListenableFuture<List<Object>> doAntiCompaction(final UUID parentRepairSession, Collection<Range<Token>> successfulRanges)
    {
        assert parentRepairSession != null;
        ParentRepairSession prs = getParentRepairSession(parentRepairSession);
        //A repair will be marked as not global if it is a subrange repair to avoid many small anti-compactions
        //in addition to other scenarios such as repairs not involving all DCs or hosts
        if (!prs.isGlobal)
        {
            logger.info("[repair #{}] Not a global repair, will not do anticompaction", parentRepairSession);
            removeParentRepairSession(parentRepairSession);
            return Futures.immediateFuture(Collections.emptyList());
        }
        assert prs.ranges.containsAll(successfulRanges) : "Trying to perform anticompaction on unknown ranges";

        List<ListenableFuture<?>> futures = new ArrayList<>();
        // if we don't have successful repair ranges, then just skip anticompaction
        if (!successfulRanges.isEmpty())
        {
            for (Map.Entry<UUID, ColumnFamilyStore> columnFamilyStoreEntry : prs.columnFamilyStores.entrySet())
            {
                Refs<SSTableReader> sstables = prs.getActiveRepairedSSTableRefsForAntiCompaction(columnFamilyStoreEntry.getKey(), parentRepairSession);
                ColumnFamilyStore cfs = columnFamilyStoreEntry.getValue();
                futures.add(CompactionManager.instance.submitAntiCompaction(cfs, successfulRanges, sstables, prs.repairedAt, parentRepairSession));
            }
        }

        ListenableFuture<List<Object>> allAntiCompactionResults = Futures.successfulAsList(futures);
        allAntiCompactionResults.addListener(new Runnable()
        {
            @Override
            public void run()
            {
                removeParentRepairSession(parentRepairSession);
            }
        }, MoreExecutors.directExecutor());

        return allAntiCompactionResults;
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
                session.validationComplete(desc, endpoint, validation.trees);
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

    /**
     * We keep a ParentRepairSession around for the duration of the entire repair, for example, on a 256 token vnode rf=3 cluster
     * we would have 768 RepairSession but only one ParentRepairSession. We use the PRS to avoid anticompacting the sstables
     * 768 times, instead we take all repaired ranges at the end of the repair and anticompact once.
     *
     * We do an optimistic marking of sstables - when we start an incremental repair we mark all unrepaired sstables as
     * repairing (@see markSSTablesRepairing), then while the repair is ongoing compactions might remove those sstables,
     * and when it is time for anticompaction we will only anticompact the sstables that are still on disk.
     *
     * Note that validation and streaming do not care about which sstables we have marked as repairing - they operate on
     * all unrepaired sstables (if it is incremental), otherwise we would not get a correct repair.
     */
    public static class ParentRepairSession
    {
        private final Map<UUID, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        private final Collection<Range<Token>> ranges;
        public final Map<UUID, Set<String>> sstableMap = new HashMap<>();
        public final boolean isIncremental;
        public final boolean isGlobal;
        public final long repairedAt;
        public final InetAddress coordinator;
        /**
         * Indicates whether we have marked sstables as repairing. Can only be done once per table per ParentRepairSession
         */
        private final Set<UUID> marked = new HashSet<>();

        public ParentRepairSession(InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal)
        {
            this.coordinator = coordinator;
            for (ColumnFamilyStore cfs : columnFamilyStores)
            {
                this.columnFamilyStores.put(cfs.metadata.cfId, cfs);
                sstableMap.put(cfs.metadata.cfId, new HashSet<String>());
            }
            this.ranges = ranges;
            this.repairedAt = repairedAt;
            this.isIncremental = isIncremental;
            this.isGlobal = isGlobal;
        }

        /**
         * Mark sstables repairing - either all sstables or only the unrepaired ones depending on
         *
         * whether this is an incremental or full repair
         *
         * @param cfId the column family
         * @param parentSessionId the parent repair session id, used to make sure we don't start multiple repairs over the same sstables
         */
        public synchronized void markSSTablesRepairing(UUID cfId, UUID parentSessionId)
        {
            if (!marked.contains(cfId))
            {
                List<SSTableReader> sstables = columnFamilyStores.get(cfId).select(View.select(SSTableSet.CANONICAL, (s) -> !isIncremental || !s.isRepaired())).sstables;
                Set<SSTableReader> currentlyRepairing = ActiveRepairService.instance.currentlyRepairing(cfId, parentSessionId);
                if (!Sets.intersection(currentlyRepairing, Sets.newHashSet(sstables)).isEmpty())
                {
                    logger.error("Cannot start multiple repair sessions over the same sstables");
                    throw new RuntimeException("Cannot start multiple repair sessions over the same sstables");
                }
                addSSTables(cfId, sstables);
                marked.add(cfId);
            }
        }

        /**
         * Get the still active sstables we should run anticompaction on
         *
         * note that validation and streaming do not call this method - they have to work on the actual active sstables on the node, we only call this
         * to know which sstables are still there that were there when we started the repair
         *
         * @param cfId
         * @param parentSessionId for checking if there exists a snapshot for this repair
         * @return
         */
        @SuppressWarnings("resource")
        public synchronized Refs<SSTableReader> getActiveRepairedSSTableRefsForAntiCompaction(UUID cfId, UUID parentSessionId)
        {
            assert marked.contains(cfId);
            if (!columnFamilyStores.containsKey(cfId))
                throw new RuntimeException("Not possible to get sstables for anticompaction for " + cfId);
            boolean isSnapshotRepair = columnFamilyStores.get(cfId).snapshotExists(parentSessionId.toString());
            ImmutableMap.Builder<SSTableReader, Ref<SSTableReader>> references = ImmutableMap.builder();
            Iterable<SSTableReader> sstables = isSnapshotRepair ? getSSTablesForSnapshotRepair(cfId, parentSessionId) : getActiveSSTables(cfId);
            // we check this above - if columnFamilyStores contains the cfId sstables will not be null
            assert sstables != null;
            for (SSTableReader sstable : sstables)
            {
                Ref<SSTableReader> ref = sstable.tryRef();
                if (ref == null)
                    sstableMap.get(cfId).remove(sstable.getFilename());
                else
                    references.put(sstable, ref);
            }
            return new Refs<>(references.build());
        }

        /**
         * If we are running a snapshot repair we need to find the 'real' sstables when we start anticompaction
         *
         * We use the generation of the sstables as identifiers instead of the file name to avoid having to parse out the
         * actual filename.
         *
         * @param cfId
         * @param parentSessionId
         * @return
         */
        private Set<SSTableReader> getSSTablesForSnapshotRepair(UUID cfId, UUID parentSessionId)
        {
            Set<SSTableReader> activeSSTables = new HashSet<>();
            ColumnFamilyStore cfs = columnFamilyStores.get(cfId);
            if (cfs == null)
                return null;

            Set<Integer> snapshotGenerations = new HashSet<>();
            try (Refs<SSTableReader> snapshottedSSTables = cfs.getSnapshotSSTableReader(parentSessionId.toString()))
            {
                for (SSTableReader sstable : snapshottedSSTables)
                {
                    snapshotGenerations.add(sstable.descriptor.generation);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                if (snapshotGenerations.contains(sstable.descriptor.generation))
                    activeSSTables.add(sstable);
            return activeSSTables;
        }

        public synchronized void maybeSnapshot(UUID cfId, UUID parentSessionId)
        {
            String snapshotName = parentSessionId.toString();
            if (!columnFamilyStores.get(cfId).snapshotExists(snapshotName))
            {
                Set<SSTableReader> snapshottedSSTables = columnFamilyStores.get(cfId).snapshot(snapshotName, new Predicate<SSTableReader>()
                {
                    public boolean apply(SSTableReader sstable)
                    {
                        return sstable != null &&
                               (!isIncremental || !sstable.isRepaired()) &&
                               !(sstable.metadata.isIndex()) && // exclude SSTables from 2i
                               new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges);
                    }
                }, true, false);

                if (isAlreadyRepairing(cfId, parentSessionId, snapshottedSSTables))
                {
                    columnFamilyStores.get(cfId).clearSnapshot(parentSessionId.toString());
                    logger.error("Cannot start multiple repair sessions over the same sstables");
                    throw new RuntimeException("Cannot start multiple repair sessions over the same sstables");
                }
                addSSTables(cfId, snapshottedSSTables);
                marked.add(cfId);
            }
        }


        /**
         * Compares other repairing sstables *generation* to the ones we just snapshotted
         *
         * we compare generations since the sstables have different paths due to snapshot names
         *
         * @param cfId id of the column family store
         * @param parentSessionId parent repair session
         * @param sstables the newly snapshotted sstables
         * @return
         */
        private boolean isAlreadyRepairing(UUID cfId, UUID parentSessionId, Collection<SSTableReader> sstables)
        {
            Set<SSTableReader> currentlyRepairing = ActiveRepairService.instance.currentlyRepairing(cfId, parentSessionId);
            Set<Integer> currentlyRepairingGenerations = new HashSet<>();
            Set<Integer> newRepairingGenerations = new HashSet<>();
            for (SSTableReader sstable : currentlyRepairing)
                currentlyRepairingGenerations.add(sstable.descriptor.generation);
            for (SSTableReader sstable : sstables)
                newRepairingGenerations.add(sstable.descriptor.generation);

            return !Sets.intersection(currentlyRepairingGenerations, newRepairingGenerations).isEmpty();
        }

        private Set<SSTableReader> getActiveSSTables(UUID cfId)
        {
            if (!columnFamilyStores.containsKey(cfId))
                return null;

            Set<String> repairedSSTables = sstableMap.get(cfId);
            Set<SSTableReader> activeSSTables = new HashSet<>();
            Set<String> activeSSTableNames = new HashSet<>();
            ColumnFamilyStore cfs = columnFamilyStores.get(cfId);
            for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            {
                if (repairedSSTables.contains(sstable.getFilename()))
                {
                    activeSSTables.add(sstable);
                    activeSSTableNames.add(sstable.getFilename());
                }
            }
            sstableMap.put(cfId, activeSSTableNames);
            return activeSSTables;
        }

        private void addSSTables(UUID cfId, Collection<SSTableReader> sstables)
        {
            for (SSTableReader sstable : sstables)
                sstableMap.get(cfId).add(sstable.getFilename());
        }


        public long getRepairedAt()
        {
            if (isGlobal)
                return repairedAt;
            return ActiveRepairService.UNREPAIRED_SSTABLE;
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

    /*
    If the coordinator node dies we should remove the parent repair session from the other nodes.
    This uses the same notifications as we get in RepairSession
     */
    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    /**
     * Something has happened to a remote node - if that node is a coordinator, we mark the parent repair session id as failed.
     *
     * The fail marker is kept in the map for 24h to make sure that if the coordinator does not agree
     * that the repair failed, we need to fail the entire repair session
     *
     * @param ep  endpoint to be convicted
     * @param phi the value of phi with with ep was convicted
     */
    public void convict(InetAddress ep, double phi)
    {
        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold() || parentRepairSessions.isEmpty())
            return;

        Set<UUID> toRemove = new HashSet<>();

        for (Map.Entry<UUID, ParentRepairSession> repairSessionEntry : parentRepairSessions.entrySet())
        {
            if (repairSessionEntry.getValue().coordinator.equals(ep))
            {
                toRemove.add(repairSessionEntry.getKey());
            }
        }

        if (!toRemove.isEmpty())
        {
            logger.debug("Removing {} in parent repair sessions", toRemove);
            for (UUID id : toRemove)
                removeParentRepairSession(id);
        }
    }

}
