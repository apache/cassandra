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
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.openmbean.CompositeData;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.repair.Scheduler;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.repair.state.CoordinatorState;
import org.apache.cassandra.repair.state.ParticipateState;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.CommonRange;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanup;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.consistent.CoordinatorSessions;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.repair.consistent.admin.PendingStats;
import org.apache.cassandra.repair.consistent.admin.RepairStats;
import org.apache.cassandra.repair.consistent.RepairedState;
import org.apache.cassandra.repair.consistent.admin.SchemaArgsParser;
import org.apache.cassandra.repair.messages.CleanupMessage;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.messages.SyncResponse;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.Config.RepairCommandPoolFullStrategy.reject;
import static org.apache.cassandra.config.DatabaseDescriptor.*;
import static org.apache.cassandra.net.Message.out;
import static org.apache.cassandra.net.Verb.PREPARE_MSG;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

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
@Simulate(with = MONITORS)
public class ActiveRepairService implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener, ActiveRepairServiceMBean
{
    public enum ParentRepairStatus
    {
        IN_PROGRESS, COMPLETED, FAILED
    }

    public static class ConsistentSessions
    {
        public final LocalSessions local = new LocalSessions();
        public final CoordinatorSessions coordinated = new CoordinatorSessions();
    }

    public final ConsistentSessions consistent = new ConsistentSessions();

    private boolean registeredForEndpointChanges = false;

    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService(FailureDetector.instance, Gossiper.instance);

    public static final long UNREPAIRED_SSTABLE = 0;
    public static final TimeUUID NO_PENDING_REPAIR = null;

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<TimeUUID, RepairSession> sessions = new ConcurrentHashMap<>();

    private final ConcurrentMap<TimeUUID, ParentRepairSession> parentRepairSessions = new ConcurrentHashMap<>();
    // map of top level repair id (parent repair id) -> state
    private final Cache<TimeUUID, CoordinatorState> repairs;
    // map of top level repair id (parent repair id) -> participate state
    private final Cache<TimeUUID, ParticipateState> participates;

    private volatile ScheduledFuture<?> irCleanup;

    static
    {
        RepairMetrics.init();
    }

    public static class RepairCommandExecutorHandle
    {
        private static final ExecutorPlus repairCommandExecutor = initializeExecutor(getRepairCommandPoolSize(), getRepairCommandPoolFullStrategy());
    }

    @VisibleForTesting
    static ExecutorPlus initializeExecutor(int maxPoolSize, Config.RepairCommandPoolFullStrategy strategy)
    {
        return executorFactory()
               .localAware()       // we do trace repair sessions, and seem to rely on local aware propagation (though could do with refactoring)
               .withJmxInternal()
               .configurePooled("Repair-Task", maxPoolSize)
               .withKeepAlive(1, TimeUnit.HOURS)
               .withQueueLimit(strategy == reject ? 0 : Integer.MAX_VALUE)
               .withRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy())
               .build();
    }

    public static ExecutorPlus repairCommandExecutor()
    {
        return RepairCommandExecutorHandle.repairCommandExecutor;
    }

    private final IFailureDetector failureDetector;
    private final Gossiper gossiper;
    private final Cache<Integer, Pair<ParentRepairStatus, List<String>>> repairStatusByCmd;

    public final ExecutorPlus snapshotExecutor = executorFactory().configurePooled("RepairSnapshotExecutor", 1)
                                                                  .withKeepAlive(1, TimeUnit.HOURS)
                                                                  .build();

    public ActiveRepairService(IFailureDetector failureDetector, Gossiper gossiper)
    {
        this.failureDetector = failureDetector;
        this.gossiper = gossiper;
        this.repairStatusByCmd = CacheBuilder.newBuilder()
                                             .expireAfterWrite(
                                             Long.getLong("cassandra.parent_repair_status_expiry_seconds",
                                                          TimeUnit.SECONDS.convert(1, TimeUnit.DAYS)), TimeUnit.SECONDS)
                                             // using weight wouldn't work so well, since it doesn't reflect mutation of cached data
                                             // see https://github.com/google/guava/wiki/CachesExplained
                                             // We assume each entry is unlikely to be much more than 100 bytes, so bounding the size should be sufficient.
                                             .maximumSize(Long.getLong("cassandra.parent_repair_status_cache_size", 100_000))
                                             .build();

        DurationSpec.LongNanosecondsBound duration = getRepairStateExpires();
        int numElements = getRepairStateSize();
        logger.info("Storing repair state for {} or for {} elements", duration, numElements);
        repairs = CacheBuilder.newBuilder()
                              .expireAfterWrite(duration.quantity(), duration.unit())
                              .maximumSize(numElements)
                              .build();
        participates = CacheBuilder.newBuilder()
                                   .expireAfterWrite(duration.quantity(), duration.unit())
                                   .maximumSize(numElements)
                                   .build();

        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    public void start()
    {
        consistent.local.start();
        this.irCleanup = ScheduledExecutors.optionalTasks.scheduleAtFixedRate(consistent.local::cleanup, 0,
                                                                              LocalSessions.CLEANUP_INTERVAL,
                                                                              TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public void clearLocalRepairState()
    {
        // .cleanUp() doesn't clear, it looks to only run gc on things that could be removed... this method should remove all state
        repairs.asMap().clear();
        participates.asMap().clear();
    }

    public void stop()
    {
        ScheduledFuture<?> irCleanup = this.irCleanup;
        if (irCleanup != null)
            irCleanup.cancel(false);
        consistent.local.stop();
    }

    @Override
    public List<Map<String, String>> getSessions(boolean all, String rangesStr)
    {
        Set<Range<Token>> ranges = RepairOption.parseRanges(rangesStr, DatabaseDescriptor.getPartitioner());
        return consistent.local.sessionInfo(all, ranges);
    }

    @Override
    public void failSession(String session, boolean force)
    {
        TimeUUID sessionID = TimeUUID.fromString(session);
        consistent.local.cancelSession(sessionID, force);
    }

    @Deprecated
    public void setRepairSessionSpaceInMegabytes(int sizeInMegabytes)
    {
        DatabaseDescriptor.setRepairSessionSpaceInMiB(sizeInMegabytes);
    }

    @Deprecated
    public int getRepairSessionSpaceInMegabytes()
    {
        return DatabaseDescriptor.getRepairSessionSpaceInMiB();
    }

    @Override
    public void setRepairSessionSpaceInMebibytes(int sizeInMebibytes)
    {
        DatabaseDescriptor.setRepairSessionSpaceInMiB(sizeInMebibytes);
    }

    @Override
    public int getRepairSessionSpaceInMebibytes()
    {
        return DatabaseDescriptor.getRepairSessionSpaceInMiB();
    }

    @Override
    public int getConcurrentMerkleTreeRequests()
    {
        return DatabaseDescriptor.getConcurrentMerkleTreeRequests();
    }

    @Override
    public void setConcurrentMerkleTreeRequests(int value)
    {
        logger.info("Setting concurrent_merkle_tree_requests to {}", value);
        DatabaseDescriptor.setConcurrentMerkleTreeRequests(value);
    }

    public List<CompositeData> getRepairStats(List<String> schemaArgs, String rangeString)
    {
        List<CompositeData> stats = new ArrayList<>();
        Collection<Range<Token>> userRanges = rangeString != null
                                              ? RepairOption.parseRanges(rangeString, DatabaseDescriptor.getPartitioner())
                                              : null;

        for (ColumnFamilyStore cfs : SchemaArgsParser.parse(schemaArgs))
        {
            String keyspace = cfs.keyspace.getName();
            Collection<Range<Token>> ranges = userRanges != null
                                              ? userRanges
                                              : StorageService.instance.getLocalReplicas(keyspace).ranges();
            RepairedState.Stats cfStats = consistent.local.getRepairedStats(cfs.metadata().id, ranges);
            stats.add(RepairStats.fromRepairState(keyspace, cfs.name, cfStats).toComposite());
        }

        return stats;
    }

    @Override
    public List<CompositeData> getPendingStats(List<String> schemaArgs, String rangeString)
    {
        List<CompositeData> stats = new ArrayList<>();
        Collection<Range<Token>> userRanges = rangeString != null
                                              ? RepairOption.parseRanges(rangeString, DatabaseDescriptor.getPartitioner())
                                              : null;
        for (ColumnFamilyStore cfs : SchemaArgsParser.parse(schemaArgs))
        {
            String keyspace = cfs.keyspace.getName();
            Collection<Range<Token>> ranges = userRanges != null
                                              ? userRanges
                                              : StorageService.instance.getLocalReplicas(keyspace).ranges();
            PendingStats cfStats = consistent.local.getPendingStats(cfs.metadata().id, ranges);
            stats.add(cfStats.toComposite());
        }

        return stats;
    }

    @Override
    public List<CompositeData> cleanupPending(List<String> schemaArgs, String rangeString, boolean force)
    {
        List<CompositeData> stats = new ArrayList<>();
        Collection<Range<Token>> userRanges = rangeString != null
                                              ? RepairOption.parseRanges(rangeString, DatabaseDescriptor.getPartitioner())
                                              : null;
        for (ColumnFamilyStore cfs : SchemaArgsParser.parse(schemaArgs))
        {
            String keyspace = cfs.keyspace.getName();
            Collection<Range<Token>> ranges = userRanges != null
                                              ? userRanges
                                              : StorageService.instance.getLocalReplicas(keyspace).ranges();
            CleanupSummary summary = consistent.local.cleanup(cfs.metadata().id, ranges, force);
            stats.add(summary.toComposite());
        }
        return stats;
    }

    @Override
    public int parentRepairSessionsCount()
    {
        return parentRepairSessions.size();
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairSession submitRepairSession(TimeUUID parentRepairSession,
                                             CommonRange range,
                                             String keyspace,
                                             RepairParallelism parallelismDegree,
                                             boolean isIncremental,
                                             boolean pullRepair,
                                             PreviewKind previewKind,
                                             boolean optimiseStreams,
                                             boolean repairPaxos,
                                             boolean paxosOnly,
                                             ExecutorPlus executor,
                                             Scheduler validationScheduler,
                                             String... cfnames)
    {
        if (repairPaxos && previewKind != PreviewKind.NONE)
            throw new IllegalArgumentException("cannot repair paxos in a preview repair");

        if (range.endpoints.isEmpty())
            return null;

        if (cfnames.length == 0)
            return null;

        final RepairSession session = new RepairSession(parentRepairSession, validationScheduler, range, keyspace,
                                                        parallelismDegree, isIncremental, pullRepair,
                                                        previewKind, optimiseStreams, repairPaxos, paxosOnly, cfnames);
        repairs.getIfPresent(parentRepairSession).register(session.state);

        sessions.put(session.getId(), session);
        // register listeners
        registerOnFdAndGossip(session);

        if (session.previewKind == PreviewKind.REPAIRED)
            LocalSessions.registerListener(session);

        // remove session at completion
        session.addListener(() -> {
            sessions.remove(session.getId());
            LocalSessions.unregisterListener(session);
        });
        session.start(executor);
        return session;
    }

    public boolean getUseOffheapMerkleTrees()
    {
        return DatabaseDescriptor.useOffheapMerkleTrees();
    }

    public void setUseOffheapMerkleTrees(boolean value)
    {
        DatabaseDescriptor.useOffheapMerkleTrees(value);
    }

    private <T extends Future &
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
        });
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

    public void recordRepairStatus(int cmd, ParentRepairStatus parentRepairStatus, List<String> messages)
    {
        repairStatusByCmd.put(cmd, Pair.create(parentRepairStatus, messages));
    }


    @VisibleForTesting
    public Pair<ParentRepairStatus, List<String>> getRepairStatus(Integer cmd)
    {
        return repairStatusByCmd.getIfPresent(cmd);
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param keyspaceName        keyspace to repair
     * @param keyspaceLocalRanges local-range for given keyspaceName
     * @param toRepair            token to repair
     * @param dataCenters         the data centers to involve in the repair
     * @return neighbors with whom we share the provided range
     */
    public static EndpointsForRange getNeighbors(String keyspaceName, Iterable<Range<Token>> keyspaceLocalRanges,
                                                 Range<Token> toRepair, Collection<String> dataCenters,
                                                 Collection<String> hosts)
    {
        StorageService ss = StorageService.instance;
        EndpointsByRange replicaSets = ss.getRangeToAddressMap(keyspaceName);
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
            return EndpointsForRange.empty(toRepair);

        EndpointsForRange neighbors = replicaSets.get(rangeSuperSet).withoutSelf();

        if (dataCenters != null && !dataCenters.isEmpty())
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Multimap<String, InetAddressAndPort> dcEndpointsMap = topology.getDatacenterEndpoints();
            Iterable<InetAddressAndPort> dcEndpoints = concat(transform(dataCenters, dcEndpointsMap::get));
            return neighbors.select(dcEndpoints, true);
        }
        else if (hosts != null && !hosts.isEmpty())
        {
            Set<InetAddressAndPort> specifiedHost = new HashSet<>();
            for (final String host : hosts)
            {
                try
                {
                    final InetAddressAndPort endpoint = InetAddressAndPort.getByName(host.trim());
                    if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()) || neighbors.endpoints().contains(endpoint))
                        specifiedHost.add(endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalArgumentException("Unknown host specified " + host, e);
                }
            }

            if (!specifiedHost.contains(FBUtilities.getBroadcastAddressAndPort()))
                throw new IllegalArgumentException("The current host must be part of the repair");

            if (specifiedHost.size() <= 1)
            {
                String msg = "Specified hosts %s do not share range %s needed for repair. Either restrict repair ranges " +
                             "with -st/-et options, or specify one of the neighbors that share this range with " +
                             "this node: %s.";
                throw new IllegalArgumentException(String.format(msg, hosts, toRepair, neighbors));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddressAndPort());
            return neighbors.keep(specifiedHost);
        }

        return neighbors;
    }

    /**
     * we only want to set repairedAt for incremental repairs including all replicas for a token range. For non-global
     * incremental repairs, forced incremental repairs, and full repairs, the UNREPAIRED_SSTABLE value will prevent
     * sstables from being promoted to repaired or preserve the repairedAt/pendingRepair values, respectively.
     */
    static long getRepairedAt(RepairOption options, boolean force)
    {
        // we only want to set repairedAt for incremental repairs including all replicas for a token range. For non-global incremental repairs, full repairs, the UNREPAIRED_SSTABLE value will prevent
        // sstables from being promoted to repaired or preserve the repairedAt/pendingRepair values, respectively. For forced repairs, repairedAt time is only set to UNREPAIRED_SSTABLE if we actually
        // end up skipping replicas
        if (options.isIncremental() && options.isGlobal() && !force)
        {
            return currentTimeMillis();
        }
        else
        {
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        }
    }

    public static boolean verifyCompactionsPendingThreshold(TimeUUID parentRepairSession, PreviewKind previewKind)
    {
        // Snapshot values so failure message is consistent with decision
        int pendingCompactions = CompactionManager.instance.getPendingTasks();
        int pendingThreshold = ActiveRepairService.instance.getRepairPendingCompactionRejectThreshold();
        if (pendingCompactions > pendingThreshold)
        {
            logger.error("[{}] Rejecting incoming repair, pending compactions ({}) above threshold ({})",
                         previewKind.logPrefix(parentRepairSession), pendingCompactions, pendingThreshold);
            return false;
        }
        return true;
    }

    public TimeUUID prepareForRepair(TimeUUID parentRepairSession, InetAddressAndPort coordinator, Set<InetAddressAndPort> endpoints, RepairOption options, boolean isForcedRepair, List<ColumnFamilyStore> columnFamilyStores)
    {
        if (!verifyCompactionsPendingThreshold(parentRepairSession, options.getPreviewKind()))
            failRepair(parentRepairSession, "Rejecting incoming repair, pending compactions above threshold"); // failRepair throws exception

        long repairedAt = getRepairedAt(options, isForcedRepair);
        registerParentRepairSession(parentRepairSession, coordinator, columnFamilyStores, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal(), options.getPreviewKind());
        final CountDownLatch prepareLatch = newCountDownLatch(endpoints.size());
        final AtomicBoolean status = new AtomicBoolean(true);
        final Set<String> failedNodes = synchronizedSet(new HashSet<String>());
        final AtomicInteger timeouts = new AtomicInteger(0);
        RequestCallback callback = new RequestCallback()
        {
            @Override
            public void onResponse(Message msg)
            {
                prepareLatch.decrement();
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                status.set(false);
                failedNodes.add(from.toString());
                if (failureReason == RequestFailureReason.TIMEOUT)
                    timeouts.incrementAndGet();
                prepareLatch.decrement();
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        };

        List<TableId> tableIds = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores)
            tableIds.add(cfs.metadata.id);

        PrepareMessage message = new PrepareMessage(parentRepairSession, tableIds, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal(), options.getPreviewKind());
        register(new ParticipateState(FBUtilities.getBroadcastAddressAndPort(), message));
        for (InetAddressAndPort neighbour : endpoints)
        {
            if (FailureDetector.instance.isAlive(neighbour))
            {
                Message<RepairMessage> msg = out(PREPARE_MSG, message);
                MessagingService.instance().sendWithCallback(msg, neighbour, callback);
            }
            else
            {
                // we pre-filter the endpoints we want to repair for forced incremental repairs. So if any of the
                // remaining ones go down, we still want to fail so we don't create repair sessions that can't complete
                if (isForcedRepair && !options.isIncremental())
                {
                    prepareLatch.decrement();
                }
                else
                {
                    // bailout early to avoid potentially waiting for a long time.
                    failRepair(parentRepairSession, "Endpoint not alive: " + neighbour);
                }
            }
        }
        try
        {
            if (!prepareLatch.await(getRpcTimeout(MILLISECONDS), MILLISECONDS) || timeouts.get() > 0)
                failRepair(parentRepairSession, "Did not get replies from all endpoints.");
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

    /**
     * Send Verb.CLEANUP_MSG to the given endpoints. This results in removing parent session object from the
     * endpoint's cache.
     * This method does not throw an exception in case of a messaging failure.
     */
    public void cleanUp(TimeUUID parentRepairSession, Set<InetAddressAndPort> endpoints)
    {
        for (InetAddressAndPort endpoint : endpoints)
        {
            try
            {
                if (FailureDetector.instance.isAlive(endpoint))
                {
                    CleanupMessage message = new CleanupMessage(parentRepairSession);
                    Message<CleanupMessage> msg = Message.out(Verb.CLEANUP_MSG, message);

                    RequestCallback loggingCallback = new RequestCallback()
                    {
                        @Override
                        public void onResponse(Message msg)
                        {
                            logger.trace("Successfully cleaned up {} parent repair session on {}.", parentRepairSession, endpoint);
                        }

                        @Override
                        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
                        {
                            logger.debug("Failed to clean up parent repair session {} on {}. The uncleaned sessions will " +
                                         "be removed on a node restart. This should not be a problem unless you see thousands " +
                                         "of messages like this.", parentRepairSession, endpoint);
                        }
                    };

                    MessagingService.instance().sendWithCallback(msg, endpoint, loggingCallback);
                }
            }
            catch (Exception exc)
            {
                logger.warn("Failed to send a clean up message to {}", endpoint, exc);
            }
        }
        ParticipateState state = participate(parentRepairSession);
        if (state != null)
            state.phase.success("Cleanup message recieved");
    }

    private void failRepair(TimeUUID parentRepairSession, String errorMsg)
    {
        ParticipateState state = participate(parentRepairSession);
        if (state != null)
            state.phase.fail(errorMsg);
        removeParentRepairSession(parentRepairSession);
        throw new RuntimeException(errorMsg);
    }

    public synchronized void registerParentRepairSession(TimeUUID parentRepairSession, InetAddressAndPort coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal, PreviewKind previewKind)
    {
        assert isIncremental || repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE;
        if (!registeredForEndpointChanges)
        {
            Gossiper.instance.register(this);
            FailureDetector.instance.registerFailureDetectionEventListener(this);
            registeredForEndpointChanges = true;
        }

        if (!parentRepairSessions.containsKey(parentRepairSession))
        {
            parentRepairSessions.put(parentRepairSession, new ParentRepairSession(coordinator, columnFamilyStores, ranges, isIncremental, repairedAt, isGlobal, previewKind));
        }
    }

    /**
     * We assume when calling this method that a parent session for the provided identifier
     * exists, and that session is still in progress. When it doesn't, that should mean either
     * {@link #abort(Predicate, String)} or {@link #failRepair(TimeUUID, String)} have removed it.
     *
     * @param parentSessionId an identifier for an active parent repair session
     * @return the {@link ParentRepairSession} associated with the provided identifier
     * @throws NoSuchRepairSessionException if the provided identifier does not map to an active parent session
     */
    public ParentRepairSession getParentRepairSession(TimeUUID parentSessionId) throws NoSuchRepairSessionException
    {
        ParentRepairSession session = parentRepairSessions.get(parentSessionId);
        if (session == null)
            throw new NoSuchRepairSessionException(parentSessionId);

        return session;
    }

    /**
     * called when the repair session is done - either failed or anticompaction has completed
     * <p>
     * clears out any snapshots created by this repair
     *
     * @param parentSessionId an identifier for an active parent repair session
     * @return the {@link ParentRepairSession} associated with the provided identifier
     * @see org.apache.cassandra.db.repair.CassandraTableRepairManager#snapshot(String, Collection, boolean)
     */
    public synchronized ParentRepairSession removeParentRepairSession(TimeUUID parentSessionId)
    {
        String snapshotName = parentSessionId.toString();
        ParentRepairSession session = parentRepairSessions.remove(parentSessionId);
        if (session == null)
            return null;

        if (session.hasSnapshots)
        {
            snapshotExecutor.submit(() -> {
                logger.info("[repair #{}] Clearing snapshots for {}", parentSessionId,
                            session.columnFamilyStores.values()
                                                      .stream()
                                                      .map(cfs -> cfs.metadata().toString()).collect(Collectors.joining(", ")));
                long startNanos = nanoTime();
                for (ColumnFamilyStore cfs : session.columnFamilyStores.values())
                {
                    if (cfs.snapshotExists(snapshotName))
                        cfs.clearSnapshot(snapshotName);
                }
                logger.info("[repair #{}] Cleared snapshots in {}ms", parentSessionId, TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNanos));
            });
        }
        return session;
    }

    public void handleMessage(Message<? extends RepairMessage> message)
    {
        RepairMessage payload = message.payload;
        RepairJobDesc desc = payload.desc;
        RepairSession session = sessions.get(desc.sessionId);

        if (session == null)
        {
            if (payload instanceof ValidationResponse)
            {
                // The trees may be off-heap, and will therefore need to be released.
                ValidationResponse validation = (ValidationResponse) payload;
                MerkleTrees trees = validation.trees;

                // The response from a failed validation won't have any trees.
                if (trees != null)
                    trees.release();
            }

            return;
        }

        switch (message.verb())
        {
            case VALIDATION_RSP:
                ValidationResponse validation = (ValidationResponse) payload;
                session.validationComplete(desc, message.from(), validation.trees);
                break;
            case SYNC_RSP:
                // one of replica is synced.
                SyncResponse sync = (SyncResponse) payload;
                session.syncComplete(desc, sync.nodes, sync.success, sync.summaries);
                break;
            default:
                break;
        }
    }

    /**
     * We keep a ParentRepairSession around for the duration of the entire repair, for example, on a 256 token vnode rf=3 cluster
     * we would have 768 RepairSession but only one ParentRepairSession. We use the PRS to avoid anticompacting the sstables
     * 768 times, instead we take all repaired ranges at the end of the repair and anticompact once.
     */
    public static class ParentRepairSession
    {
        private final Keyspace keyspace;
        private final Map<TableId, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        private final Collection<Range<Token>> ranges;
        public final boolean isIncremental;
        public final boolean isGlobal;
        public final long repairedAt;
        public final InetAddressAndPort coordinator;
        public final PreviewKind previewKind;
        public volatile boolean hasSnapshots = false;

        public ParentRepairSession(InetAddressAndPort coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal, PreviewKind previewKind)
        {
            this.coordinator = coordinator;
            Set<Keyspace> keyspaces = new HashSet<>();
            for (ColumnFamilyStore cfs : columnFamilyStores)
            {
                keyspaces.add(cfs.keyspace);
                this.columnFamilyStores.put(cfs.metadata.id, cfs);
            }

            Preconditions.checkArgument(keyspaces.size() == 1, "repair sessions cannot operate on multiple keyspaces");
            this.keyspace = Iterables.getOnlyElement(keyspaces);

            this.ranges = ranges;
            this.repairedAt = repairedAt;
            this.isIncremental = isIncremental;
            this.isGlobal = isGlobal;
            this.previewKind = previewKind;
        }

        public boolean isPreview()
        {
            return previewKind != PreviewKind.NONE;
        }

        public Collection<ColumnFamilyStore> getColumnFamilyStores()
        {
            return ImmutableSet.<ColumnFamilyStore>builder().addAll(columnFamilyStores.values()).build();
        }

        public Keyspace getKeyspace()
        {
            return keyspace;
        }

        public Set<TableId> getTableIds()
        {
            return ImmutableSet.copyOf(transform(getColumnFamilyStores(), cfs -> cfs.metadata.id));
        }

        public Set<Range<Token>> getRanges()
        {
            return ImmutableSet.copyOf(ranges);
        }

        @Override
        public String toString()
        {
            return "ParentRepairSession{" +
                   "columnFamilyStores=" + columnFamilyStores +
                   ", ranges=" + ranges +
                   ", repairedAt=" + repairedAt +
                   '}';
        }

        public void setHasSnapshots()
        {
            hasSnapshots = true;
        }
    }

    /*
    If the coordinator node dies we should remove the parent repair session from the other nodes.
    This uses the same notifications as we get in RepairSession
     */
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
    }

    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
    }

    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
    }

    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
    }

    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
    }

    public void onRemove(InetAddressAndPort endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    /**
     * Something has happened to a remote node - if that node is a coordinator, we mark the parent repair session id as failed.
     * <p>
     * The fail marker is kept in the map for 24h to make sure that if the coordinator does not agree
     * that the repair failed, we need to fail the entire repair session
     *
     * @param ep  endpoint to be convicted
     * @param phi the value of phi with with ep was convicted
     */
    public void convict(InetAddressAndPort ep, double phi)
    {
        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold() || parentRepairSessions.isEmpty())
            return;

        abort((prs) -> prs.coordinator.equals(ep), "Removing {} in parent repair sessions");
    }

    public int getRepairPendingCompactionRejectThreshold()
    {
        return DatabaseDescriptor.getRepairPendingCompactionRejectThreshold();
    }

    public void setRepairPendingCompactionRejectThreshold(int value)
    {
        DatabaseDescriptor.setRepairPendingCompactionRejectThreshold(value);
    }

    /**
     * Remove any parent repair sessions matching predicate
     */
    public void abort(Predicate<ParentRepairSession> predicate, String message)
    {
        Set<TimeUUID> parentSessionsToRemove = new HashSet<>();
        for (Map.Entry<TimeUUID, ParentRepairSession> repairSessionEntry : parentRepairSessions.entrySet())
        {
            if (predicate.test(repairSessionEntry.getValue()))
                parentSessionsToRemove.add(repairSessionEntry.getKey());
        }
        if (!parentSessionsToRemove.isEmpty())
        {
            logger.info(message, parentSessionsToRemove);
            parentSessionsToRemove.forEach(this::removeParentRepairSession);
        }
    }

    @VisibleForTesting
    public int parentRepairSessionCount()
    {
        return parentRepairSessions.size();
    }

    @VisibleForTesting
    public int sessionCount()
    {
        return sessions.size();
    }

    public Future<?> repairPaxosForTopologyChange(String ksName, Collection<Range<Token>> ranges, String reason)
    {
        if (!paxosRepairEnabled())
        {
            logger.warn("Not running paxos repair for topology change because paxos repair has been disabled");
            return ImmediateFuture.success(null);
        }

        if (ranges.isEmpty())
        {
            logger.warn("Not running paxos repair for topology change because there are no ranges to repair");
            return ImmediateFuture.success(null);
        }
        List<TableMetadata> tables = Lists.newArrayList(Schema.instance.getKeyspaceMetadata(ksName).tables);
        List<Future<Void>> futures = new ArrayList<>(ranges.size() * tables.size());
        Keyspace keyspace = Keyspace.open(ksName);
        AbstractReplicationStrategy replication = keyspace.getReplicationStrategy();
        for (Range<Token> range: ranges)
        {
            for (TableMetadata table : tables)
            {
                Set<InetAddressAndPort> endpoints = replication.getNaturalReplicas(range.right).filter(FailureDetector.isReplicaAlive).endpoints();
                if (!PaxosRepair.hasSufficientLiveNodesForTopologyChange(keyspace, range, endpoints))
                {
                    Set<InetAddressAndPort> downEndpoints = replication.getNaturalReplicas(range.right).filter(e -> !endpoints.contains(e)).endpoints();
                    downEndpoints.removeAll(endpoints);

                    throw new RuntimeException(String.format("Insufficient live nodes to repair paxos for %s in %s for %s.\n" +
                                                             "There must be enough live nodes to satisfy EACH_QUORUM, but the following nodes are down: %s\n" +
                                                             "This check can be skipped by setting either the yaml property skip_paxos_repair_on_topology_change or " +
                                                             "the system property cassandra.skip_paxos_repair_on_topology_change to false. The jmx property " +
                                                             "StorageService.SkipPaxosRepairOnTopologyChange can also be set to false to temporarily disable without " +
                                                             "restarting the node\n" +
                                                             "Individual keyspaces can be skipped with the yaml property skip_paxos_repair_on_topology_change_keyspaces, the" +
                                                             "system property cassandra.skip_paxos_repair_on_topology_change_keyspaces, or temporarily with the jmx" +
                                                             "property StorageService.SkipPaxosRepairOnTopologyChangeKeyspaces\n" +
                                                             "Skipping this check can lead to paxos correctness issues",
                                                             range, ksName, reason, downEndpoints));
                }
                EndpointsForToken pending = StorageService.instance.getTokenMetadata().pendingEndpointsForToken(range.right, ksName);
                if (pending.size() > 1 && !Boolean.getBoolean("cassandra.paxos_repair_allow_multiple_pending_unsafe"))
                {
                    throw new RuntimeException(String.format("Cannot begin paxos auto repair for %s in %s.%s, multiple pending endpoints exist for range (%s). " +
                                                             "Set -Dcassandra.paxos_repair_allow_multiple_pending_unsafe=true to skip this check",
                                                             range, table.keyspace, table.name, pending));

                }
                Future<Void> future = PaxosCleanup.cleanup(endpoints, table, Collections.singleton(range), false, repairCommandExecutor());
                futures.add(future);
            }
        }

        return FutureCombiner.allOf(futures);
    }

    public int getPaxosRepairParallelism()
    {
        return DatabaseDescriptor.getPaxosRepairParallelism();
    }

    public void setPaxosRepairParallelism(int v)
    {
        DatabaseDescriptor.setPaxosRepairParallelism(v);
    }

    public void shutdownNowAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, snapshotExecutor);
    }

    public Collection<CoordinatorState> coordinators()
    {
        return repairs.asMap().values();
    }

    public CoordinatorState coordinator(TimeUUID id)
    {
        return repairs.getIfPresent(id);
    }

    public void register(CoordinatorState state)
    {
        repairs.put(state.id, state);
    }

    public boolean register(ParticipateState state)
    {
        synchronized (participates)
        {
            ParticipateState current = participates.getIfPresent(state.id);
            if (current != null)
                return false;
            participates.put(state.id, state);
        }
        return true;
    }

    public Collection<ParticipateState> participates()
    {
        return participates.asMap().values();
    }

    public ParticipateState participate(TimeUUID id)
    {
        return participates.getIfPresent(id);
    }

    public Collection<ValidationState> validations()
    {
        return participates.asMap().values().stream().flatMap(p -> p.validations().stream()).collect(Collectors.toList());
    }

    public ValidationState validation(UUID id)
    {
        for (ValidationState state : validations())
        {
            if (state.id.equals(id))
                return state;
        }
        return null;
    }
}
