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
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
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
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.consistent.CoordinatorSessions;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.config.Config.RepairCommandPoolFullStrategy.queue;

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

    public static CassandraVersion SUPPORTS_GLOBAL_PREPARE_FLAG_VERSION = new CassandraVersion("2.2.1");

    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService(FailureDetector.instance, Gossiper.instance);

    public static final long UNREPAIRED_SSTABLE = 0;
    public static final UUID NO_PENDING_REPAIR = null;

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions = new ConcurrentHashMap<>();

    private final ConcurrentMap<UUID, ParentRepairSession> parentRepairSessions = new ConcurrentHashMap<>();

    public final static ExecutorService repairCommandExecutor;
    static
    {
        Config.RepairCommandPoolFullStrategy strategy = DatabaseDescriptor.getRepairCommandPoolFullStrategy();
        BlockingQueue<Runnable> queue;
        if (strategy == Config.RepairCommandPoolFullStrategy.reject)
            queue = new SynchronousQueue<>();
        else
            queue = new LinkedBlockingQueue<>();

        repairCommandExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                 DatabaseDescriptor.getRepairCommandPoolSize(),
                                                                 1, TimeUnit.HOURS,
                                                                 queue,
                                                                 new NamedThreadFactory("Repair-Task"),
                                                                 "internal",
                                                                 new ThreadPoolExecutor.AbortPolicy());
    }

    private final IFailureDetector failureDetector;
    private final Gossiper gossiper;
    private final Cache<Integer, Pair<ParentRepairStatus, List<String>>> repairStatusByCmd;

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

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start()
    {
        consistent.local.start();
        ScheduledExecutors.optionalTasks.scheduleAtFixedRate(consistent.local::cleanup, 0,
                                                             LocalSessions.CLEANUP_INTERVAL,
                                                             TimeUnit.SECONDS);
    }

    @Override
    public List<Map<String, String>> getSessions(boolean all)
    {
        return consistent.local.sessionInfo(all);
    }

    @Override
    public void failSession(String session, boolean force)
    {
        UUID sessionID = UUID.fromString(session);
        consistent.local.cancelSession(sessionID, force);
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
                                             boolean isIncremental,
                                             boolean pullRepair,
                                             boolean force,
                                             PreviewKind previewKind,
                                             boolean optimiseStreams,
                                             ListeningExecutorService executor,
                                             String... cfnames)
    {
        if (endpoints.isEmpty())
            return null;

        if (cfnames.length == 0)
            return null;


        final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, endpoints, isIncremental, pullRepair, force, previewKind, optimiseStreams, cfnames);

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
        }, MoreExecutors.directExecutor());
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


    Pair<ParentRepairStatus, List<String>> getRepairStatus(Integer cmd)
    {
        return repairStatusByCmd.getIfPresent(cmd);
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

    /**
     * we only want to set repairedAt for incremental repairs including all replicas for a token range. For non-global
     * incremental repairs, forced incremental repairs, and full repairs, the UNREPAIRED_SSTABLE value will prevent
     * sstables from being promoted to repaired or preserve the repairedAt/pendingRepair values, respectively.
     */
    static long getRepairedAt(RepairOption options)
    {
        // we only want to set repairedAt for incremental repairs including all replicas for a token range. For non-global incremental repairs, forced incremental repairs, and
        // full repairs, the UNREPAIRED_SSTABLE value will prevent sstables from being promoted to repaired or preserve the repairedAt/pendingRepair values, respectively.
        if (options.isIncremental() && options.isGlobal() && !options.isForcedRepair())
        {
            return Clock.instance.currentTimeMillis();
        }
        else
        {
            return  ActiveRepairService.UNREPAIRED_SSTABLE;
        }
    }

    public UUID prepareForRepair(UUID parentRepairSession, InetAddress coordinator, Set<InetAddress> endpoints, RepairOption options, List<ColumnFamilyStore> columnFamilyStores)
    {
        long repairedAt = getRepairedAt(options);
        registerParentRepairSession(parentRepairSession, coordinator, columnFamilyStores, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal(), options.getPreviewKind());
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

        List<TableId> tableIds = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores)
            tableIds.add(cfs.metadata.id);

        for (InetAddress neighbour : endpoints)
        {
            if (FailureDetector.instance.isAlive(neighbour))
            {
                PrepareMessage message = new PrepareMessage(parentRepairSession, tableIds, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal(), options.getPreviewKind());
                MessageOut<RepairMessage> msg = message.createMessage();
                MessagingService.instance().sendRR(msg, neighbour, callback, DatabaseDescriptor.getRpcTimeout(), true);
            }
            else
            {
                if (options.isForcedRepair())
                {
                    prepareLatch.countDown();
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

    public synchronized void registerParentRepairSession(UUID parentRepairSession, InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal, PreviewKind previewKind)
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
        private final Map<TableId, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        private final Collection<Range<Token>> ranges;
        public final boolean isIncremental;
        public final boolean isGlobal;
        public final long repairedAt;
        public final InetAddress coordinator;
        public final PreviewKind previewKind;

        public ParentRepairSession(InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal, PreviewKind previewKind)
        {
            this.coordinator = coordinator;
            for (ColumnFamilyStore cfs : columnFamilyStores)
            {
                this.columnFamilyStores.put(cfs.metadata.id, cfs);
            }
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

        public Predicate<SSTableReader> getPreviewPredicate()
        {
            switch (previewKind)
            {
                case ALL:
                    return (s) -> true;
                case REPAIRED:
                    return (s) -> s.isRepaired();
                case UNREPAIRED:
                    return (s) -> !s.isRepaired();
                default:
                    throw new RuntimeException("Can't get preview predicate for preview kind " + previewKind);
            }
        }

        public synchronized void maybeSnapshot(TableId tableId, UUID parentSessionId)
        {
            String snapshotName = parentSessionId.toString();
            if (!columnFamilyStores.get(tableId).snapshotExists(snapshotName))
            {
                Set<SSTableReader> snapshottedSSTables = columnFamilyStores.get(tableId).snapshot(snapshotName, new Predicate<SSTableReader>()
                {
                    public boolean apply(SSTableReader sstable)
                    {
                        return sstable != null &&
                               (!isIncremental || !sstable.isRepaired()) &&
                               !(sstable.metadata().isIndex()) && // exclude SSTables from 2i
                               new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges);
                    }
                }, true, false);
            }
        }

        public Collection<ColumnFamilyStore> getColumnFamilyStores()
        {
            return ImmutableSet.<ColumnFamilyStore>builder().addAll(columnFamilyStores.values()).build();
        }

        public Set<TableId> getTableIds()
        {
            return ImmutableSet.copyOf(Iterables.transform(getColumnFamilyStores(), cfs -> cfs.metadata.id));
        }

        public Collection<Range<Token>> getRanges()
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
