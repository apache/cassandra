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
package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.FutureTask;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.ExpiringMemoizingSupplier;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RecomputingSupplier;
import org.apache.cassandra.utils.concurrent.NotScheduledFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_GOSSIP_ENDPOINT_REMOVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIPER_QUARANTINE_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIPER_SKIP_WAITING_TO_SETTLE;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.SHUTDOWN_ANNOUNCE_DELAY_IN_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.VERY_LONG_TIME_MS;
import static org.apache.cassandra.config.DatabaseDescriptor.getClusterName;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitionerName;
import static org.apache.cassandra.gms.VersionedValue.BOOTSTRAPPING_STATUS;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.apache.cassandra.net.Verb.GOSSIP_DIGEST_SYN;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

/**
 * This module is responsible for Gossiping information for the local endpoint. This abstraction
 * maintains the list of live and dead endpoints. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 * Upon hearing a GossipShutdownMessage, this module will instantly mark the remote node as down in
 * the Failure Detector.
 *
 * This class is not threadsafe and any state changes should happen in the gossip stage.
 */

public class Gossiper implements IFailureDetectionEventListener, GossiperMBean, IGossiper
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=Gossiper";

    private static final ScheduledExecutorPlus executor = executorFactory().scheduled("GossipTasks");

    static final ApplicationState[] STATES = ApplicationState.values();
    static final List<String> DEAD_STATES = Arrays.asList(VersionedValue.REMOVING_TOKEN, VersionedValue.REMOVED_TOKEN,
                                                          VersionedValue.STATUS_LEFT, VersionedValue.HIBERNATE);
    static ArrayList<String> SILENT_SHUTDOWN_STATES = new ArrayList<>();
    static
    {
        SILENT_SHUTDOWN_STATES.addAll(DEAD_STATES);
        SILENT_SHUTDOWN_STATES.add(VersionedValue.STATUS_BOOTSTRAPPING);
        SILENT_SHUTDOWN_STATES.add(VersionedValue.STATUS_BOOTSTRAPPING_REPLACE);
    }
    private static final List<String> ADMINISTRATIVELY_INACTIVE_STATES = Arrays.asList(VersionedValue.HIBERNATE,
                                                                                       VersionedValue.REMOVED_TOKEN,
                                                                                       VersionedValue.STATUS_LEFT);
    private volatile ScheduledFuture<?> scheduledGossipTask;
    private static final ReentrantLock taskLock = new ReentrantLock();
    public final static int intervalInMillis = 1000;
    public final static int QUARANTINE_DELAY = GOSSIPER_QUARANTINE_DELAY.getInt(StorageService.RING_DELAY_MILLIS * 2);
    private static final Logger logger = LoggerFactory.getLogger(Gossiper.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);

    public static final Gossiper instance = new Gossiper(true);

    // Timestamp to prevent processing any in-flight messages for we've not send any SYN yet, see CASSANDRA-12653.
    volatile long firstSynSendAt = 0L;

    public static final long aVeryLongTime = getVeryLongTime();

    // Maximimum difference between generation value and local time we are willing to accept about a peer
    static final int MAX_GENERATION_DIFFERENCE = 86400 * 365;
    private final long fatClientTimeout;
    private final Random random = new Random();

    /* subscribers for interest in EndpointState change */
    private final List<IEndpointStateChangeSubscriber> subscribers = new CopyOnWriteArrayList<>();

    /* live member set */
    @VisibleForTesting
    final Set<InetAddressAndPort> liveEndpoints = new ConcurrentSkipListSet<>();

    /* unreachable member set */
    private final Map<InetAddressAndPort, Long> unreachableEndpoints = new ConcurrentHashMap<>();

    /* initial seeds for joining the cluster */
    @VisibleForTesting
    final Set<InetAddressAndPort> seeds = new ConcurrentSkipListSet<>();

    /* map where key is the endpoint and value is the state associated with the endpoint.
     * This is made public to be consumed by the GossipInfoTable virtual table */
    public final ConcurrentMap<InetAddressAndPort, EndpointState> endpointStateMap = new ConcurrentHashMap<>();

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    private final Map<InetAddressAndPort, Long> justRemovedEndpoints = new ConcurrentHashMap<>();

    private final Map<InetAddressAndPort, Long> expireTimeEndpointMap = new ConcurrentHashMap<>();

    private volatile boolean inShadowRound = false;
    // seeds gathered during shadow round that indicated to be in the shadow round phase as well
    private final Set<InetAddressAndPort> seedsInShadowRound = new ConcurrentSkipListSet<>();
    // endpoint states as gathered during shadow round
    private final Map<InetAddressAndPort, EndpointState> endpointShadowStateMap = new ConcurrentHashMap<>();

    private volatile long lastProcessedMessageAt = currentTimeMillis();

    /**
     * This property is initially set to {@code true} which means that we have no information about the other nodes.
     * Once all nodes are on at least this node version, it becomes {@code false}, which means that we are not
     * upgrading from the previous version (major, minor).
     *
     * This property and anything that checks it should be removed in 5.0
     */
    private volatile boolean upgradeInProgressPossible = true;
    private volatile boolean hasNodeWithUnknownVersion = false;

    public void clearUnsafe()
    {
        unreachableEndpoints.clear();
        liveEndpoints.clear();
        justRemovedEndpoints.clear();
        expireTimeEndpointMap.clear();
        endpointStateMap.clear();
        endpointShadowStateMap.clear();
        seedsInShadowRound.clear();
    }

    // returns true when the node does not know the existence of other nodes.
    private static boolean isLoneNode(Map<InetAddressAndPort, EndpointState> epStates)
    {
        return epStates.isEmpty() || epStates.keySet().equals(Collections.singleton(FBUtilities.getBroadcastAddressAndPort()));
    }

    private static final ExpiringMemoizingSupplier.Memoized<CassandraVersion> NO_UPGRADE_IN_PROGRESS = new ExpiringMemoizingSupplier.Memoized<>(null);
    private static final ExpiringMemoizingSupplier.NotMemoized<CassandraVersion> CURRENT_NODE_VERSION = new ExpiringMemoizingSupplier.NotMemoized<>(SystemKeyspace.CURRENT_VERSION);
    final Supplier<ExpiringMemoizingSupplier.ReturnValue<CassandraVersion>> upgradeFromVersionSupplier = () ->
    {
        // Once there are no prior version nodes we don't need to keep rechecking
        if (!upgradeInProgressPossible)
            return NO_UPGRADE_IN_PROGRESS;

        CassandraVersion minVersion = SystemKeyspace.CURRENT_VERSION;

        // Skip the round if the gossiper has not started yet
        // Otherwise, upgradeInProgressPossible can be set to false wrongly.
        // If we don't know any epstate we don't know anything about the cluster.
        // If we only know about ourselves, we can assume that version is CURRENT_VERSION
        if (!isEnabled() || isLoneNode(endpointStateMap))
            return CURRENT_NODE_VERSION;

        // Check the release version of all the peers it heard of. Not necessary the peer that it has/had contacted with.
        hasNodeWithUnknownVersion = false;
        for (Entry<InetAddressAndPort, EndpointState> entry : endpointStateMap.entrySet())
        {

            if (justRemovedEndpoints.containsKey(entry.getKey()))
                continue;

            CassandraVersion version = getReleaseVersion(entry.getKey());

            // if it is dead state, we skip the version check
            if (isDeadState(entry.getValue()))
                continue;
            //Raced with changes to gossip state, wait until next iteration
            if (version == null)
                hasNodeWithUnknownVersion = true;
            else if (version.compareTo(minVersion) < 0)
                minVersion = version;
        }

        if (minVersion.compareTo(SystemKeyspace.CURRENT_VERSION) < 0)
            return new ExpiringMemoizingSupplier.Memoized<>(minVersion);

        if (hasNodeWithUnknownVersion)
            return new ExpiringMemoizingSupplier.NotMemoized<>(minVersion);

        upgradeInProgressPossible = false;
        return NO_UPGRADE_IN_PROGRESS;
    };

    private final Supplier<CassandraVersion> upgradeFromVersionMemoized = ExpiringMemoizingSupplier.memoizeWithExpiration(upgradeFromVersionSupplier, 1, TimeUnit.MINUTES);

    @VisibleForTesting
    public void expireUpgradeFromVersion()
    {
        upgradeInProgressPossible = true;
        ((ExpiringMemoizingSupplier<CassandraVersion>) upgradeFromVersionMemoized).expire();
    }

    private static final boolean disableThreadValidation = GOSSIP_DISABLE_THREAD_VALIDATION.getBoolean();
    private static volatile boolean disableEndpointRemoval = DISABLE_GOSSIP_ENDPOINT_REMOVAL.getBoolean();

    private static long getVeryLongTime()
    {
        long time = VERY_LONG_TIME_MS.getLong();
        String defaultValue = VERY_LONG_TIME_MS.getDefaultValue();

        if (!String.valueOf(time).equals(defaultValue))
            logger.info("Overriding {} from {} to {}ms", VERY_LONG_TIME_MS.getKey(), defaultValue, time);

        return time;
    }

    private static boolean isInGossipStage()
    {
        return Stage.GOSSIP.executor().inExecutor();
    }

    private static void checkProperThreadForStateMutation()
    {
        if (disableThreadValidation || isInGossipStage())
            return;

        IllegalStateException e = new IllegalStateException("Attempting gossip state mutation from illegal thread: " + Thread.currentThread().getName());
        if (DatabaseDescriptor.strictRuntimeChecks())
        {
            throw e;
        }
        else
        {
            noSpamLogger.getStatement(Throwables.getStackTraceAsString(e)).error(e.getMessage(), e);
        }
    }

    private class GossipTask implements Runnable
    {
        public void run()
        {
            try
            {
                //wait on messaging service to start listening
                MessagingService.instance().waitUntilListening();

                taskLock.lock();

                /* Update the local heartbeat counter. */
                endpointStateMap.get(getBroadcastAddressAndPort()).getHeartBeatState().updateHeartBeat();
                if (logger.isTraceEnabled())
                    logger.trace("My heartbeat is now {}", endpointStateMap.get(FBUtilities.getBroadcastAddressAndPort()).getHeartBeatState().getHeartBeatVersion());
                final List<GossipDigest> gDigests = new ArrayList<>();

                Gossiper.instance.makeGossipDigest(gDigests);

                if (gDigests.size() > 0)
                {
                    GossipDigestSyn digestSynMessage = new GossipDigestSyn(getClusterName(),
                                                                           getPartitionerName(),
                                                                           gDigests);
                    Message<GossipDigestSyn> message = Message.out(GOSSIP_DIGEST_SYN, digestSynMessage);
                    /* Gossip to some random live member */
                    boolean gossipedToSeed = doGossipToLiveMember(message);

                    /* Gossip to some unreachable member with some probability to check if he is back up */
                    maybeGossipToUnreachableMember(message);

                    /* Gossip to a seed if we did not do so above, or we have seen less nodes
                       than there are seeds.  This prevents partitions where each group of nodes
                       is only gossiping to a subset of the seeds.

                       The most straightforward check would be to check that all the seeds have been
                       verified either as live or unreachable.  To avoid that computation each round,
                       we reason that:

                       either all the live nodes are seeds, in which case non-seeds that come online
                       will introduce themselves to a member of the ring by definition,

                       or there is at least one non-seed node in the list, in which case eventually
                       someone will gossip to it, and then do a gossip to a random seed from the
                       gossipedToSeed check.

                       See CASSANDRA-150 for more exposition. */
                    if (!gossipedToSeed || liveEndpoints.size() < seeds.size())
                        maybeGossipToSeed(message);

                    doStatusCheck();
                }
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error("Gossip error", e);
            }
            finally
            {
                taskLock.unlock();
            }
        }
    }

    private final RecomputingSupplier<CassandraVersion> minVersionSupplier = new RecomputingSupplier<>(this::computeMinVersion, executor);

    @VisibleForTesting
    public Gossiper(boolean registerJmx)
    {
        // half of QUARATINE_DELAY, to ensure justRemovedEndpoints has enough leeway to prevent re-gossip
        fatClientTimeout = (QUARANTINE_DELAY / 2);
        /* register with the Failure Detector for receiving Failure detector events */
        FailureDetector.instance.registerFailureDetectionEventListener(this);

        // Register this instance with JMX
        if (registerJmx)
        {
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
        }

        subscribers.add(new IEndpointStateChangeSubscriber()
        {
            public void onJoin(InetAddressAndPort endpoint, EndpointState state)
	    {
                maybeRecompute(state);
            }

            public void onAlive(InetAddressAndPort endpoint, EndpointState state)
	    {
                maybeRecompute(state);
            }

            private void maybeRecompute(EndpointState state)
	    {
                if (state.getApplicationState(ApplicationState.RELEASE_VERSION) != null)
                    minVersionSupplier.recompute();
            }

            public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
            {
                if (state == ApplicationState.RELEASE_VERSION)
                    minVersionSupplier.recompute();
            }
        });
    }

    public void setLastProcessedMessageAt(long timeInMillis)
    {
        this.lastProcessedMessageAt = timeInMillis;
    }

    public boolean seenAnySeed()
    {
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : endpointStateMap.entrySet())
        {
            if (seeds.contains(entry.getKey()))
                return true;
            try
            {
                VersionedValue internalIp = entry.getValue().getApplicationState(ApplicationState.INTERNAL_IP);
                VersionedValue internalIpAndPort = entry.getValue().getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
                InetAddressAndPort endpoint = null;
                if (internalIpAndPort != null)
                {
                    endpoint = InetAddressAndPort.getByName(internalIpAndPort.value);
                }
                else if (internalIp != null)
                {
                    endpoint = InetAddressAndPort.getByName(internalIp.value);
                }
                if (endpoint != null && seeds.contains(endpoint))
                    return true;
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    /**
     * Register for interesting state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    @Override
    public void register(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers.add(subscriber);
    }

    /**
     * Unregister interest for state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    @Override
    public void unregister(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers.remove(subscriber);
    }

    /**
     * @return a list of live gossip participants, including fat clients
     */
    public Set<InetAddressAndPort> getLiveMembers()
    {
        Set<InetAddressAndPort> liveMembers = new HashSet<>(liveEndpoints);
        if (!liveMembers.contains(getBroadcastAddressAndPort()))
            liveMembers.add(getBroadcastAddressAndPort());
        return liveMembers;
    }

    /**
     * @return a list of live ring members.
     */
    public Set<InetAddressAndPort> getLiveTokenOwners()
    {
        return StorageService.instance.getLiveRingMembers(true);
    }

    /**
     * @return a list of unreachable gossip participants, including fat clients
     */
    public Set<InetAddressAndPort> getUnreachableMembers()
    {
        return unreachableEndpoints.keySet();
    }

    /**
     * @return a list of unreachable token owners
     */
    public Set<InetAddressAndPort> getUnreachableTokenOwners()
    {
        Set<InetAddressAndPort> tokenOwners = new HashSet<>();
        for (InetAddressAndPort endpoint : unreachableEndpoints.keySet())
        {
            if (StorageService.instance.getTokenMetadata().isMember(endpoint))
                tokenOwners.add(endpoint);
        }

        return tokenOwners;
    }

    public long getEndpointDowntime(InetAddressAndPort ep)
    {
        Long downtime = unreachableEndpoints.get(ep);
        if (downtime != null)
            return TimeUnit.NANOSECONDS.toMillis(nanoTime() - downtime);
        else
            return 0L;
    }

    private boolean isShutdown(InetAddressAndPort endpoint)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        if (epState == null)
        {
            return false;
        }

        VersionedValue versionedValue = epState.getApplicationState(ApplicationState.STATUS_WITH_PORT);
        if (versionedValue == null)
        {
            versionedValue = epState.getApplicationState(ApplicationState.STATUS);
            if (versionedValue == null)
            {
                return false;
            }
        }

        String value = versionedValue.value;
        String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        String state = pieces[0];
        return state.equals(VersionedValue.SHUTDOWN);
    }

    public static void runInGossipStageBlocking(Runnable runnable)
    {
        // run immediately if we're already in the gossip stage
        if (isInGossipStage())
        {
            runnable.run();
            return;
        }

        FutureTask<?> task = new FutureTask<>(runnable);
        Stage.GOSSIP.execute(task);
        try
        {
            task.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * @param endpoint end point that is convicted.
     */
    public void convict(InetAddressAndPort endpoint, double phi)
    {
        runInGossipStageBlocking(() -> {
            EndpointState epState = endpointStateMap.get(endpoint);
            if (epState == null)
                return;

            if (!epState.isAlive())
                return;

            logger.debug("Convicting {} with status {} - alive {}", endpoint, getGossipStatus(epState), epState.isAlive());

            if (isShutdown(endpoint))
            {
                markAsShutdown(endpoint);
            }
            else
            {
                markDead(endpoint, epState);
            }
            GossiperDiagnostics.convicted(this, endpoint, phi);
        });
    }

    /**
     * This method is used to mark a node as shutdown; that is it gracefully exited on its own and told us about it
     * @param endpoint endpoint that has shut itself down
     * @deprecated see CASSANDRA-18913
     */
    @Deprecated(since = "5.0") // can remove once 4.x is not supported
    protected void markAsShutdown(InetAddressAndPort endpoint)
    {
        checkProperThreadForStateMutation();
        EndpointState epState = endpointStateMap.get(endpoint);
        if (epState == null || epState.isStateEmpty())
            return;
        VersionedValue shutdown = StorageService.instance.valueFactory.shutdown(true);
        epState.addApplicationState(ApplicationState.STATUS_WITH_PORT, shutdown);
        epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.shutdown(true));
        epState.addApplicationState(ApplicationState.RPC_READY, StorageService.instance.valueFactory.rpcReady(false));
        epState.getHeartBeatState().forceHighestPossibleVersionUnsafe();
        markDead(endpoint, epState);
        FailureDetector.instance.forceConviction(endpoint);
        GossiperDiagnostics.markedAsShutdown(this, endpoint);
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onChange(endpoint, ApplicationState.STATUS_WITH_PORT, shutdown);
        logger.debug("Marked {} as shutdown", endpoint);
    }

    /**
     * This method is used to mark a node as shutdown; that is it gracefully exited on its own and told us about it
     * @param endpoint endpoint that has shut itself down
     * @param remoteState from the endpoint shutting down
     */
    protected void markAsShutdown(InetAddressAndPort endpoint, EndpointState remoteState)
    {
        checkProperThreadForStateMutation();
        EndpointState epState = endpointStateMap.get(endpoint);
        if (epState == null || epState.isStateEmpty())
            return;
        if (!VersionedValue.SHUTDOWN.equals(remoteState.getStatus()))
            throw new AssertionError("Remote shutdown sent but was not with a shutdown status?  " + remoteState);
        // added in 5.0 so we know STATUS_WITH_PORT is set
        VersionedValue shutdown = remoteState.getApplicationState(ApplicationState.STATUS_WITH_PORT);
        if (shutdown == null)
            throw new AssertionError("Remote shutdown sent but missing STATUS_WITH_PORT; " + remoteState);
        endpointStateMap.put(endpoint, remoteState);
        markDead(endpoint, remoteState);
        FailureDetector.instance.forceConviction(endpoint);
        GossiperDiagnostics.markedAsShutdown(this, endpoint);
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onChange(endpoint, ApplicationState.STATUS_WITH_PORT, shutdown);
        logger.debug("Marked {} as shutdown", endpoint);
    }

    /**
     * Return either: the greatest heartbeat or application state
     *
     * @param epState
     * @return
     */
    static int getMaxEndpointStateVersion(EndpointState epState)
    {
        int maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
        for (Map.Entry<ApplicationState, VersionedValue> state : epState.states())
            maxVersion = Math.max(maxVersion, state.getValue().version);
        return maxVersion;
    }

    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
     */
    private void evictFromMembership(InetAddressAndPort endpoint)
    {
        checkProperThreadForStateMutation();
        unreachableEndpoints.remove(endpoint);
        endpointStateMap.remove(endpoint);
        expireTimeEndpointMap.remove(endpoint);
        FailureDetector.instance.remove(endpoint);
        quarantineEndpoint(endpoint);
        if (logger.isDebugEnabled())
            logger.debug("evicting {} from gossip", endpoint);
        GossiperDiagnostics.evictedFromMembership(this, endpoint);
    }

    /**
     * Removes the endpoint from Gossip but retains endpoint state
     */
    public void removeEndpoint(InetAddressAndPort endpoint)
    {
        checkProperThreadForStateMutation();
        // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onRemove(endpoint);

        if(seeds.contains(endpoint))
        {
            buildSeedsList();
            seeds.remove(endpoint);
            logger.info("removed {} from seeds, updated seeds list = {}", endpoint, seeds);
            if (seeds.isEmpty())
                logger.warn("Seeds list is now empty!");
        }

        if (disableEndpointRemoval)
            return;

        liveEndpoints.remove(endpoint);
        unreachableEndpoints.remove(endpoint);
        MessagingService.instance().versions.reset(endpoint);
        quarantineEndpoint(endpoint);
        MessagingService.instance().closeOutbound(endpoint);
        MessagingService.instance().removeInbound(endpoint);
        logger.debug("removing endpoint {}", endpoint);
        GossiperDiagnostics.removedEndpoint(this, endpoint);
    }

    @VisibleForTesting
    public void unsafeAnnulEndpoint(InetAddressAndPort endpoint)
    {
        removeEndpoint(endpoint);
        justRemovedEndpoints.remove(endpoint);
        endpointStateMap.remove(endpoint);
        expireTimeEndpointMap.remove(endpoint);
        unreachableEndpoints.remove(endpoint);
    }

    /**
     * Quarantines the endpoint for QUARANTINE_DELAY
     *
     * @param endpoint
     */
    private void quarantineEndpoint(InetAddressAndPort endpoint)
    {
        quarantineEndpoint(endpoint, currentTimeMillis());
    }

    /**
     * Quarantines the endpoint until quarantineExpiration + QUARANTINE_DELAY
     *
     * @param endpoint
     * @param quarantineExpiration
     */
    private void quarantineEndpoint(InetAddressAndPort endpoint, long quarantineExpiration)
    {
        if (disableEndpointRemoval)
            return;
        justRemovedEndpoints.put(endpoint, quarantineExpiration);
        GossiperDiagnostics.quarantinedEndpoint(this, endpoint, quarantineExpiration);
    }

    /**
     * Quarantine endpoint specifically for replacement purposes.
     * @param endpoint
     */
    public void replacementQuarantine(InetAddressAndPort endpoint)
    {
        // remember, quarantineEndpoint will effectively already add QUARANTINE_DELAY, so this is 2x
        quarantineEndpoint(endpoint, currentTimeMillis() + QUARANTINE_DELAY);
        GossiperDiagnostics.replacementQuarantine(this, endpoint);
    }

    /**
     * Remove the Endpoint and evict immediately, to avoid gossiping about this node.
     * This should only be called when a token is taken over by a new IP address.
     *
     * @param endpoint The endpoint that has been replaced
     */
    public void replacedEndpoint(InetAddressAndPort endpoint)
    {
        checkProperThreadForStateMutation();
        removeEndpoint(endpoint);
        evictFromMembership(endpoint);
        replacementQuarantine(endpoint);
        GossiperDiagnostics.replacedEndpoint(this, endpoint);
    }

    /**
     * @param gDigests list of Gossip Digests to be filled
     */
    private void makeGossipDigest(List<GossipDigest> gDigests)
    {
        EndpointState epState;
        int generation;
        int maxVersion;

        // local epstate will be part of endpointStateMap
        for (Entry<InetAddressAndPort, EndpointState> entry : endpointStateMap.entrySet())
        {
            epState = entry.getValue();
            if (epState != null)
            {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = getMaxEndpointStateVersion(epState);
            }
            else
            {
                generation = 0;
                maxVersion = 0;
            }
            gDigests.add(new GossipDigest(entry.getKey(), generation, maxVersion));
        }

        if (logger.isTraceEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest gDigest : gDigests)
            {
                sb.append(gDigest);
                sb.append(' ');
            }
            logger.trace("Gossip Digests are : {}", sb);
        }
    }

    /**
     * This method will begin removing an existing endpoint from the cluster by spoofing its state
     * This should never be called unless this coordinator has had 'removenode' invoked
     *
     * @param endpoint    - the endpoint being removed
     * @param hostId      - the ID of the host being removed
     * @param localHostId - my own host ID for replication coordination
     */
    public void advertiseRemoving(InetAddressAndPort endpoint, UUID hostId, UUID localHostId)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        // remember this node's generation
        int generation = epState.getHeartBeatState().getGeneration();
        logger.info("Removing host: {}", hostId);
        logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY_MILLIS, endpoint);
        Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        // make sure it did not change
        epState = endpointStateMap.get(endpoint);
        if (epState.getHeartBeatState().getGeneration() != generation)
            throw new RuntimeException("Endpoint " + endpoint + " generation changed while trying to remove it");
        // update the other node's generation to mimic it as if it had changed it itself
        logger.info("Advertising removal for {}", endpoint);
        epState.updateTimestamp(); // make sure we don't evict it too soon
        epState.getHeartBeatState().forceNewerGenerationUnsafe();
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.removingNonlocal(hostId));
        states.put(ApplicationState.STATUS, StorageService.instance.valueFactory.removingNonlocal(hostId));
        states.put(ApplicationState.REMOVAL_COORDINATOR, StorageService.instance.valueFactory.removalCoordinator(localHostId));
        epState.addApplicationStates(states);
        endpointStateMap.put(endpoint, epState);
    }

    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     * This should only be called after advertiseRemoving
     *
     * @param endpoint
     * @param hostId
     */
    public void advertiseTokenRemoved(InetAddressAndPort endpoint, UUID hostId)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        epState.updateTimestamp(); // make sure we don't evict it too soon
        epState.getHeartBeatState().forceNewerGenerationUnsafe();
        long expireTime = computeExpireTime();
        epState.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.removedNonlocal(hostId, expireTime));
        epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.removedNonlocal(hostId, expireTime));
        logger.info("Completing removal of {}", endpoint);
        addExpireTimeForEndpoint(endpoint, expireTime);
        endpointStateMap.put(endpoint, epState);
        // ensure at least one gossip round occurs before returning
        Uninterruptibles.sleepUninterruptibly(intervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    public void unsafeAssassinateEndpoint(String address) throws UnknownHostException
    {
        logger.warn("Gossiper.unsafeAssassinateEndpoint is deprecated and will be removed in the next release; use assassinateEndpoint instead");
        assassinateEndpoint(address);
    }

    /**
     * Do not call this method unless you know what you are doing.
     * It will try extremely hard to obliterate any endpoint from the ring,
     * even if it does not know about it.
     *
     * @param address
     * @throws UnknownHostException
     */
    public void assassinateEndpoint(String address) throws UnknownHostException
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
        runInGossipStageBlocking(() -> {
            EndpointState epState = endpointStateMap.get(endpoint);
            logger.warn("Assassinating {} via gossip", endpoint);

            if (epState == null)
            {
                epState = new EndpointState(new HeartBeatState((int) ((currentTimeMillis() + 60000) / 1000), 9999));
            }
            else
            {
                int generation = epState.getHeartBeatState().getGeneration();
                int heartbeat = epState.getHeartBeatState().getHeartBeatVersion();
                logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY_MILLIS, endpoint);
                Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
                // make sure it did not change
                EndpointState newState = endpointStateMap.get(endpoint);
                if (newState == null)
                    logger.warn("Endpoint {} disappeared while trying to assassinate, continuing anyway", endpoint);
                else if (newState.getHeartBeatState().getGeneration() != generation)
                    throw new RuntimeException("Endpoint still alive: " + endpoint + " generation changed while trying to assassinate it");
                else if (newState.getHeartBeatState().getHeartBeatVersion() != heartbeat)
                    throw new RuntimeException("Endpoint still alive: " + endpoint + " heartbeat changed while trying to assassinate it");
                epState.updateTimestamp(); // make sure we don't evict it too soon
                epState.getHeartBeatState().forceNewerGenerationUnsafe();
            }

            Collection<Token> tokens = null;
            try
            {
                tokens = StorageService.instance.getTokenMetadata().getTokens(endpoint);
            }
            catch (Throwable th)
            {
                JVMStabilityInspector.inspectThrowable(th);
            }
            if (tokens == null || tokens.isEmpty())
            {
                logger.warn("Trying to assassinate an endpoint {} that does not have any tokens assigned. This should not have happened, trying to continue with a random token.", address);
                tokens = Collections.singletonList(StorageService.instance.getTokenMetadata().partitioner.getRandomToken());
            }

            long expireTime = computeExpireTime();
            epState.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.left(tokens, expireTime));
            epState.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.left(tokens, computeExpireTime()));
            handleMajorStateChange(endpoint, epState);
            Uninterruptibles.sleepUninterruptibly(intervalInMillis * 4, TimeUnit.MILLISECONDS);
            logger.warn("Finished assassinating {}", endpoint);
        });
    }

    public boolean isKnownEndpoint(InetAddressAndPort endpoint)
    {
        return endpointStateMap.containsKey(endpoint);
    }

    public int getCurrentGenerationNumber(InetAddressAndPort endpoint)
    {
        return endpointStateMap.get(endpoint).getHeartBeatState().getGeneration();
    }

    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     * @param message
     * @param epSet   a set of endpoint from which a random endpoint is chosen.
     * @return true if the chosen endpoint is also a seed.
     */
    private boolean sendGossip(Message<GossipDigestSyn> message, Set<InetAddressAndPort> epSet)
    {
        List<InetAddressAndPort> endpoints = ImmutableList.copyOf(epSet);

        int size = endpoints.size();
        if (size < 1)
            return false;
        /* Generate a random number from 0 -> size */
        int index = (size == 1) ? 0 : random.nextInt(size);
        InetAddressAndPort to = endpoints.get(index);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestSyn to {} ...", to);
        if (firstSynSendAt == 0)
            firstSynSendAt = nanoTime();
        MessagingService.instance().send(message, to);

        boolean isSeed = seeds.contains(to);
        GossiperDiagnostics.sendGossipDigestSyn(this, to);
        return isSeed;
    }

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    private boolean doGossipToLiveMember(Message<GossipDigestSyn> message)
    {
        int size = liveEndpoints.size();
        if (size == 0)
            return false;
        return sendGossip(message, liveEndpoints);
    }

    /* Sends a Gossip message to an unreachable member */
    private void maybeGossipToUnreachableMember(Message<GossipDigestSyn> message)
    {
        double liveEndpointCount = liveEndpoints.size();
        double unreachableEndpointCount = unreachableEndpoints.size();
        if (unreachableEndpointCount > 0)
        {
            /* based on some probability */
            double prob = unreachableEndpointCount / (liveEndpointCount + 1);
            double randDbl = random.nextDouble();
            if (randDbl < prob)
            {
                sendGossip(message, Sets.filter(unreachableEndpoints.keySet(),
                                                ep -> !isDeadState(getEndpointStateMap().get(ep))));
            }
        }
    }

    /* Possibly gossip to a seed for facilitating partition healing */
    private void maybeGossipToSeed(Message<GossipDigestSyn> prod)
    {
        int size = seeds.size();
        if (size > 0)
        {
            if (size == 1 && seeds.contains(getBroadcastAddressAndPort()))
            {
                return;
            }

            if (liveEndpoints.size() == 0)
            {
                sendGossip(prod, seeds);
            }
            else
            {
                /* Gossip with the seed with some probability. */
                double probability = seeds.size() / (double) (liveEndpoints.size() + unreachableEndpoints.size());
                double randDbl = random.nextDouble();
                if (randDbl <= probability)
                    sendGossip(prod, seeds);
            }
        }
    }

    public boolean isGossipOnlyMember(InetAddressAndPort endpoint)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        if (epState == null)
        {
            return false;
        }
        return !isDeadState(epState) && !StorageService.instance.getTokenMetadata().isMember(endpoint);
    }

    /**
     * Check if this node can safely be started and join the ring.
     * If the node is bootstrapping, examines gossip state for any previous status to decide whether
     * it's safe to allow this node to start and bootstrap. If not bootstrapping, compares the host ID
     * that the node itself has (obtained by reading from system.local or generated if not present)
     * with the host ID obtained from gossip for the endpoint address (if any). This latter case
     * prevents a non-bootstrapping, new node from being started with the same address of a
     * previously started, but currently down predecessor.
     *
     * @param endpoint - the endpoint to check
     * @param localHostUUID - the host id to check
     * @param isBootstrapping - whether the node intends to bootstrap when joining
     * @param epStates - endpoint states in the cluster
     * @return true if it is safe to start the node, false otherwise
     */
    public boolean isSafeForStartup(InetAddressAndPort endpoint, UUID localHostUUID, boolean isBootstrapping,
                                    Map<InetAddressAndPort, EndpointState> epStates)
    {
        EndpointState epState = epStates.get(endpoint);
        // if there's no previous state, we're good
        if (epState == null)
            return true;

        String status = getGossipStatus(epState);

        if (status.equals(VersionedValue.HIBERNATE)
            && !SystemKeyspace.bootstrapComplete())
        {
            logger.warn("A node with the same IP in hibernate status was detected. Was a replacement already attempted?");
            return false;
        }

        //the node was previously removed from the cluster
        if (isDeadState(epState))
            return true;

        if (isBootstrapping)
        {
            // these states are not allowed to join the cluster as it would not be safe
            final List<String> unsafeStatuses = new ArrayList<String>()
            {{
                add("");                           // failed bootstrap but we did start gossiping
                add(VersionedValue.STATUS_NORMAL); // node is legit in the cluster or it was stopped with kill -9
                add(VersionedValue.SHUTDOWN);      // node was shutdown
            }};
            return !unsafeStatuses.contains(status);
        }
        else
        {
            // if the previous UUID matches what we currently have (i.e. what was read from
            // system.local at startup), then we're good to start up. Otherwise, something
            // is amiss and we need to replace the previous node
            VersionedValue previous = epState.getApplicationState(ApplicationState.HOST_ID);
            return UUID.fromString(previous.value).equals(localHostUUID);
        }
    }

    @VisibleForTesting
    void doStatusCheck()
    {
        if (logger.isTraceEnabled())
            logger.trace("Performing status check ...");

        long now = currentTimeMillis();
        long nowNano = nanoTime();

        long pending = Stage.GOSSIP.executor().getPendingTaskCount();
        if (pending > 0 && lastProcessedMessageAt < now - 1000)
        {
            // if some new messages just arrived, give the executor some time to work on them
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            // still behind?  something's broke
            if (lastProcessedMessageAt < now - 1000)
            {
                logger.warn("Gossip stage has {} pending tasks; skipping status check (no nodes will be marked down)", pending);
                return;
            }
        }

        Set<InetAddressAndPort> eps = endpointStateMap.keySet();
        for (InetAddressAndPort endpoint : eps)
        {
            if (endpoint.equals(getBroadcastAddressAndPort()))
                continue;

            FailureDetector.instance.interpret(endpoint);
            EndpointState epState = endpointStateMap.get(endpoint);
            if (epState != null)
            {
                // check if this is a fat client. fat clients are removed automatically from
                // gossip after FatClientTimeout.  Do not remove dead states here.
                if (isGossipOnlyMember(endpoint)
                    && !justRemovedEndpoints.containsKey(endpoint)
                    && TimeUnit.NANOSECONDS.toMillis(nowNano - epState.getUpdateTimestamp()) > fatClientTimeout)
                {
                    logger.info("FatClient {} has been silent for {}ms, removing from gossip", endpoint, fatClientTimeout);
                    runInGossipStageBlocking(() -> {
                        if (!isGossipOnlyMember(endpoint))
                        {
                            // updating gossip and token metadata are not atomic, but rely on the single threaded gossip stage
                            // since status checks are done outside the gossip stage, need to confirm the state of the endpoint
                            // to make sure that the previous read data was correct
                            logger.info("Race condition marking {} as a FatClient; ignoring", endpoint);
                            return;
                        }                        
                        removeEndpoint(endpoint); // will put it in justRemovedEndpoints to respect quarantine delay
                        evictFromMembership(endpoint); // can get rid of the state immediately
                    });
                }

                // check for dead state removal
                long expireTime = getExpireTimeForEndpoint(endpoint);
                if (!epState.isAlive() && (now > expireTime)
                    && (!StorageService.instance.getTokenMetadata().isMember(endpoint)))
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("time is expiring for endpoint : {} ({})", endpoint, expireTime);
                    }
                    runInGossipStageBlocking(() -> evictFromMembership(endpoint));
                }
            }
        }

        if (!justRemovedEndpoints.isEmpty())
        {
            for (Entry<InetAddressAndPort, Long> entry : justRemovedEndpoints.entrySet())
            {
                if ((now - entry.getValue()) > QUARANTINE_DELAY)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("{} elapsed, {} gossip quarantine over", QUARANTINE_DELAY, entry.getKey());
                    justRemovedEndpoints.remove(entry.getKey());
                }
            }
        }
    }

    protected long getExpireTimeForEndpoint(InetAddressAndPort endpoint)
    {
        /* default expireTime is aVeryLongTime */
        Long storedTime = expireTimeEndpointMap.get(endpoint);
        return storedTime == null ? computeExpireTime() : storedTime;
    }

    @Override
    public EndpointState getEndpointStateForEndpoint(InetAddressAndPort ep)
    {
        return endpointStateMap.get(ep);
    }

    public EndpointState copyEndpointStateForEndpoint(InetAddressAndPort ep)
    {
        EndpointState epState = endpointStateMap.get(ep);
        if (epState == null)
            return null;
        return new EndpointState(epState);
    }

    public ImmutableSet<InetAddressAndPort> getEndpoints()
    {
        return ImmutableSet.copyOf(endpointStateMap.keySet());
    }

    public String getForEndpoint(InetAddressAndPort ep, ApplicationState state)
    {
        EndpointState epState = endpointStateMap.get(ep);
        if (epState == null)
            return null;
        VersionedValue value = epState.getApplicationState(state);
        return value == null ? null : value.value;
    }

    public int getEndpointCount()
    {
        return endpointStateMap.size();
    }

    Map<InetAddressAndPort, EndpointState> getEndpointStateMap()
    {
        return ImmutableMap.copyOf(endpointStateMap);
    }

    Map<InetAddressAndPort, Long> getJustRemovedEndpoints()
    {
        return ImmutableMap.copyOf(justRemovedEndpoints);
    }

    Map<InetAddressAndPort, Long> getUnreachableEndpoints()
    {
        return ImmutableMap.copyOf(unreachableEndpoints);
    }

    Set<InetAddressAndPort> getSeedsInShadowRound()
    {
        return ImmutableSet.copyOf(seedsInShadowRound);
    }

    long getLastProcessedMessageAt()
    {
        return lastProcessedMessageAt;
    }

    public UUID getHostId(InetAddressAndPort endpoint)
    {
        return getHostId(endpoint, endpointStateMap);
    }

    public UUID getHostId(InetAddressAndPort endpoint, Map<InetAddressAndPort, EndpointState> epStates)
    {
        return UUID.fromString(epStates.get(endpoint).getApplicationState(ApplicationState.HOST_ID).value);
    }

    /**
     * The value for the provided application state for the provided endpoint as currently known by this Gossip instance.
     *
     * @param endpoint the endpoint from which to get the endpoint state.
     * @param state the endpoint state to get.
     * @return the value of the application state {@code state} for {@code endpoint}, or {@code null} if either
     * {@code endpoint} is not known by Gossip or has no value for {@code state}.
     */
    public String getApplicationState(InetAddressAndPort endpoint, ApplicationState state)
    {
        EndpointState epState = endpointStateMap.get(endpoint);
        if (epState == null)
            return null;

        VersionedValue value = epState.getApplicationState(state);
        return value == null ? null : value.value;
    }

    EndpointState getStateForVersionBiggerThan(InetAddressAndPort forEndpoint, int version)
    {
        EndpointState epState = endpointStateMap.get(forEndpoint);
        EndpointState reqdEndpointState = null;

        if (epState != null)
        {
            /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
            HeartBeatState heartBeatState = epState.getHeartBeatState();
            int localHbGeneration = heartBeatState.getGeneration();
            int localHbVersion = heartBeatState.getHeartBeatVersion();
            if (localHbVersion > version)
            {
                reqdEndpointState = new EndpointState(new HeartBeatState(localHbGeneration, localHbVersion));
                if (logger.isTraceEnabled())
                    logger.trace("local heartbeat version {} greater than {} for {}", localHbVersion, version, forEndpoint);
            }
            /* Accumulate all application states whose versions are greater than "version" variable */
            Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
            for (Entry<ApplicationState, VersionedValue> entry : epState.states())
            {
                VersionedValue value = entry.getValue();
                if (value.version > version)
                {
                    if (reqdEndpointState == null)
                    {
                        reqdEndpointState = new EndpointState(new HeartBeatState(localHbGeneration, localHbVersion));
                    }
                    final ApplicationState key = entry.getKey();
                    if (logger.isTraceEnabled())
                        logger.trace("Adding state {}: {}" , key, value.value);

                    states.put(key, value);
                }
            }
            if (reqdEndpointState != null)
                reqdEndpointState.addApplicationStates(states);
        }
        return reqdEndpointState;
    }

    /**
     * determine which endpoint started up earlier
     */
    public int compareEndpointStartup(InetAddressAndPort addr1, InetAddressAndPort addr2)
    {
        EndpointState ep1 = getEndpointStateForEndpoint(addr1);
        EndpointState ep2 = getEndpointStateForEndpoint(addr2);
        assert ep1 != null && ep2 != null;
        return ep1.getHeartBeatState().getGeneration() - ep2.getHeartBeatState().getGeneration();
    }

    public void notifyFailureDetector(Map<InetAddressAndPort, EndpointState> remoteEpStateMap)
    {
        for (Entry<InetAddressAndPort, EndpointState> entry : remoteEpStateMap.entrySet())
        {
            notifyFailureDetector(entry.getKey(), entry.getValue());
        }
    }

    void notifyFailureDetector(InetAddressAndPort endpoint, EndpointState remoteEndpointState)
    {
        if (remoteEndpointState == null)
            return;

        EndpointState localEndpointState = endpointStateMap.get(endpoint);
        /*
         * If the local endpoint state exists then report to the FD only
         * if the versions workout.
        */
        if (localEndpointState != null)
        {
            IFailureDetector fd = FailureDetector.instance;
            int localGeneration = localEndpointState.getHeartBeatState().getGeneration();
            int remoteGeneration = remoteEndpointState.getHeartBeatState().getGeneration();
            if (remoteGeneration > localGeneration)
            {
                localEndpointState.updateTimestamp();
                // this node was dead and the generation changed, this indicates a reboot, or possibly a takeover
                // we will clean the fd intervals for it and relearn them
                if (!localEndpointState.isAlive())
                {
                    logger.debug("Clearing interval times for {} due to generation change", endpoint);
                    fd.remove(endpoint);
                }
                fd.report(endpoint);
                return;
            }

            if (remoteGeneration == localGeneration)
            {
                int localVersion = getMaxEndpointStateVersion(localEndpointState);
                int remoteVersion = remoteEndpointState.getHeartBeatState().getHeartBeatVersion();
                if (remoteVersion > localVersion)
                {
                    localEndpointState.updateTimestamp();
                    // just a version change, report to the fd
                    fd.report(endpoint);
                }
            }
        }

    }

    private void markAlive(final InetAddressAndPort addr, final EndpointState localState)
    {
        localState.markDead();

        Message<NoPayload> echoMessage = Message.out(ECHO_REQ, noPayload);
        logger.trace("Sending ECHO_REQ to {}", addr);
        RequestCallback echoHandler = msg ->
        {
            runInGossipStageBlocking(() -> realMarkAlive(addr, localState));
        };

        MessagingService.instance().sendWithCallback(echoMessage, addr, echoHandler);

        GossiperDiagnostics.markedAlive(this, addr, localState);
    }

    @VisibleForTesting
    public void realMarkAlive(final InetAddressAndPort addr, final EndpointState localState)
    {
        checkProperThreadForStateMutation();
        if (logger.isTraceEnabled())
            logger.trace("marking as alive {}", addr);
        localState.markAlive();
        localState.updateTimestamp(); // prevents doStatusCheck from racing us and evicting if it was down > aVeryLongTime
        liveEndpoints.add(addr);
        unreachableEndpoints.remove(addr);
        expireTimeEndpointMap.remove(addr);
        logger.debug("removing expire time for endpoint : {}", addr);
        logger.info("InetAddress {} is now UP", addr);
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onAlive(addr, localState);
        if (logger.isTraceEnabled())
            logger.trace("Notified {}", subscribers);

        GossiperDiagnostics.realMarkedAlive(this, addr, localState);
    }

    @VisibleForTesting
    public void markDead(InetAddressAndPort addr, EndpointState localState)
    {
        checkProperThreadForStateMutation();
        if (logger.isTraceEnabled())
            logger.trace("marking as down {}", addr);
        silentlyMarkDead(addr, localState);
        logger.info("InetAddress {} is now DOWN", addr);
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onDead(addr, localState);
        if (logger.isTraceEnabled())
            logger.trace("Notified {}", subscribers);

        GossiperDiagnostics.markedDead(this, addr, localState);
    }

    /**
     * Used by {@link #markDead(InetAddressAndPort, EndpointState)} and {@link #addSavedEndpoint(InetAddressAndPort)}
     * to register a endpoint as dead.  This method is "silent" to avoid triggering listeners, diagnostics, or logs
     * on startup via addSavedEndpoint.
     */
    private void silentlyMarkDead(InetAddressAndPort addr, EndpointState localState)
    {
        localState.markDead();
        if (!disableEndpointRemoval)
        {
            liveEndpoints.remove(addr);
            unreachableEndpoints.put(addr, nanoTime());
        }
    }

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      endpoint
     * @param epState EndpointState for the endpoint
     */
    private void handleMajorStateChange(InetAddressAndPort ep, EndpointState epState)
    {
        checkProperThreadForStateMutation();
        EndpointState localEpState = endpointStateMap.get(ep);
        if (!isDeadState(epState))
        {
            if (localEpState != null)
                logger.info("Node {} has restarted, now UP", ep);
            else
                logger.info("Node {} is now part of the cluster", ep);
        }
        if (logger.isTraceEnabled())
            logger.trace("Adding endpoint state for {}", ep);
        endpointStateMap.put(ep, epState);

        if (localEpState != null)
        {   // the node restarted: it is up to the subscriber to take whatever action is necessary
            for (IEndpointStateChangeSubscriber subscriber : subscribers)
                subscriber.onRestart(ep, localEpState);
        }

        if (!isDeadState(epState))
            markAlive(ep, epState);
        else
        {
            logger.debug("Not marking {} alive due to dead state", ep);
            markDead(ep, epState);
        }
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onJoin(ep, epState);
        // check this at the end so nodes will learn about the endpoint
        if (isShutdown(ep))
            markAsShutdown(ep);

        GossiperDiagnostics.majorStateChangeHandled(this, ep, epState);
    }

    public boolean isAlive(InetAddressAndPort endpoint)
    {
        EndpointState epState = getEndpointStateForEndpoint(endpoint);
        if (epState == null)
            return false;
        return epState.isAlive() && !isDeadState(epState);
    }

    public boolean isDeadState(EndpointState epState)
    {
        String status = getGossipStatus(epState);
        if (status.isEmpty())
            return false;

        return DEAD_STATES.contains(status);
    }

    public boolean isSilentShutdownState(EndpointState epState)
    {
        String status = getGossipStatus(epState);
        if (status.isEmpty())
            return false;

        return SILENT_SHUTDOWN_STATES.contains(status);
    }

    public boolean isAdministrativelyInactiveState(EndpointState epState)
    {
        String status = getGossipStatus(epState);
        if (status.isEmpty())
            return false;

        return ADMINISTRATIVELY_INACTIVE_STATES.contains(status);
    }

    public boolean isAdministrativelyInactiveState(InetAddressAndPort endpoint)
    {
        EndpointState epState = getEndpointStateForEndpoint(endpoint);
        if (epState == null)
            return true; // if the end point cannot be found, treat as inactive
        return isAdministrativelyInactiveState(epState);
    }

    public static String getGossipStatus(EndpointState epState)
    {
        if (epState == null)
        {
            return "";
        }

        VersionedValue versionedValue = epState.getApplicationState(ApplicationState.STATUS_WITH_PORT);
        if (versionedValue == null)
        {
            versionedValue = epState.getApplicationState(ApplicationState.STATUS);
            if (versionedValue == null)
            {
                return "";
            }
        }

        String value = versionedValue.value;
        String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        return pieces[0];
    }

    /**
     * Gossip offers no happens-before relationship, but downstream subscribers assume a happens-before relationship
     * before being notified!  To attempt to be nicer to subscribers, this {@link Comparator} attempts to order EndpointState
     * within a map based off a few heuristics:
     * <ol>
     *     <li>STATUS - some STATUS depends on other instance STATUS, so make sure they are last; eg. BOOT, and BOOT_REPLACE</li>
     *     <li>generation - normally defined as system clock millis, this can be skewed and is a best effort</li>
     *     <li>address - tie breaker to make sure order is consistent</li>
     * </ol>
     * <p>
     * Problems:
     * Generation is normally defined as system clock millis, which can be skewed and in-consistent cross nodes
     * (generations do not have a happens-before relationship, so ordering is sketchy at best).
     * <p>
     * Motivations:
     * {@link Map#entrySet()} returns data in effectivlly random order, so can get into a situation such as the following example.
     * {@code
     * 3 node cluster: n1, n2, and n3
     * n2 goes down and n4 does host replacement and fails before completion
     * h5 tries to do a host replacement against n4 (ignore the fact this doesn't make sense)
     * }
     * In that case above, the {@link Map#entrySet()} ordering can be random, causing h4 to apply before h2, which will
     * be rejected by subscripers (only after updating gossip causing zero retries).
     */
    @VisibleForTesting
    static Comparator<Entry<InetAddressAndPort, EndpointState>> stateOrderMap()
    {
        // There apears to be some edge cases where the state we are ordering get added to the global state causing
        // ordering to change... to avoid that rely on a cache
        // see CASSANDRA-17908
        class Cache extends HashMap<InetAddressAndPort, EndpointState>
        {
            EndpointState get(Entry<InetAddressAndPort, EndpointState> e)
            {
                if (containsKey(e.getKey()))
                    return get(e.getKey());
                put(e.getKey(), new EndpointState(e.getValue()));
                return get(e.getKey());
            }
        }
        Cache cache = new Cache();
        return ((Comparator<Entry<InetAddressAndPort, EndpointState>>) (e1, e2) -> {
            String e1status = getGossipStatus(cache.get(e1));
            String e2status = getGossipStatus(cache.get(e2));

            if (Objects.equals(e1status, e2status) || (BOOTSTRAPPING_STATUS.contains(e1status) && BOOTSTRAPPING_STATUS.contains(e2status)))
                return 0;

            // check status first, make sure bootstrap status happens-after all others
            if (BOOTSTRAPPING_STATUS.contains(e1status))
                return 1;
            if (BOOTSTRAPPING_STATUS.contains(e2status))
                return -1;
            return 0;
        })
        .thenComparingInt((Entry<InetAddressAndPort, EndpointState> e) -> cache.get(e).getHeartBeatState().getGeneration())
        .thenComparing(Entry::getKey);
    }

    private static Iterable<Entry<InetAddressAndPort, EndpointState>> order(Map<InetAddressAndPort, EndpointState> epStateMap)
    {
        List<Entry<InetAddressAndPort, EndpointState>> list = new ArrayList<>(epStateMap.entrySet());
        Collections.sort(list, stateOrderMap());
        return list;
    }

    @VisibleForTesting
    public void applyStateLocally(Map<InetAddressAndPort, EndpointState> epStateMap)
    {
        checkProperThreadForStateMutation();
        boolean hasMajorVersion3Nodes = hasMajorVersion3OrUnknownNodes();
        for (Entry<InetAddressAndPort, EndpointState> entry : order(epStateMap))
        {
            InetAddressAndPort ep = entry.getKey();
            if (ep.equals(getBroadcastAddressAndPort()) && !isInShadowRound())
                continue;
            if (justRemovedEndpoints.containsKey(ep))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Ignoring gossip for {} because it is quarantined", ep);
                continue;
            }

            EndpointState localEpStatePtr = endpointStateMap.get(ep);
            EndpointState remoteState = entry.getValue();

            if (!hasMajorVersion3Nodes)
                remoteState.removeMajorVersion3LegacyApplicationStates();

            /*
                If state does not exist just add it. If it does then add it if the remote generation is greater.
                If there is a generation tie, attempt to break it by heartbeat version.
            */
            if (localEpStatePtr != null)
            {
                int localGeneration = localEpStatePtr.getHeartBeatState().getGeneration();
                int remoteGeneration = remoteState.getHeartBeatState().getGeneration();
                long localTime = currentTimeMillis() / 1000;
                if (logger.isTraceEnabled())
                    logger.trace("{} local generation {}, remote generation {}", ep, localGeneration, remoteGeneration);

                // We measure generation drift against local time, based on the fact that generation is initialized by time
                if (remoteGeneration > localTime + MAX_GENERATION_DIFFERENCE)
                {
                    // assume some peer has corrupted memory and is broadcasting an unbelievable generation about another peer (or itself)
                    logger.warn("received an invalid gossip generation for peer {}; local time = {}, received generation = {}", ep, localTime, remoteGeneration);
                }
                else if (remoteGeneration > localGeneration)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Updating heartbeat state generation to {} from {} for {}", remoteGeneration, localGeneration, ep);
                    // major state change will handle the update by inserting the remote state directly
                    handleMajorStateChange(ep, remoteState);
                }
                else if (remoteGeneration == localGeneration) // generation has not changed, apply new states
                {
                    /* find maximum state */
                    int localMaxVersion = getMaxEndpointStateVersion(localEpStatePtr);
                    int remoteMaxVersion = getMaxEndpointStateVersion(remoteState);
                    if (remoteMaxVersion > localMaxVersion)
                    {
                        // apply states, but do not notify since there is no major change
                        applyNewStates(ep, localEpStatePtr, remoteState, hasMajorVersion3Nodes);
                    }
                    else if (logger.isTraceEnabled())
                            logger.trace("Ignoring remote version {} <= {} for {}", remoteMaxVersion, localMaxVersion, ep);

                    if (!localEpStatePtr.isAlive() && !isDeadState(localEpStatePtr)) // unless of course, it was dead
                        markAlive(ep, localEpStatePtr);
                }
                else
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Ignoring remote generation {} < {}", remoteGeneration, localGeneration);
                }
            }
            else
            {
                // this is a new node, report it to the FD in case it is the first time we are seeing it AND it's not alive
                FailureDetector.instance.report(ep);
                handleMajorStateChange(ep, remoteState);
            }
        }
    }

    private void applyNewStates(InetAddressAndPort addr, EndpointState localState, EndpointState remoteState, boolean hasMajorVersion3Nodes)
    {
        // don't assert here, since if the node restarts the version will go back to zero
        int oldVersion = localState.getHeartBeatState().getHeartBeatVersion();

        localState.setHeartBeatState(remoteState.getHeartBeatState());
        if (logger.isTraceEnabled())
            logger.trace("Updating heartbeat state version to {} from {} for {} ...", localState.getHeartBeatState().getHeartBeatVersion(), oldVersion, addr);

        Set<Entry<ApplicationState, VersionedValue>> remoteStates = remoteState.states();
        assert remoteState.getHeartBeatState().getGeneration() == localState.getHeartBeatState().getGeneration();


        Set<Entry<ApplicationState, VersionedValue>> updatedStates = remoteStates.stream().filter(entry -> {
            // filter out the states that are already up to date (has the same or higher version)
            VersionedValue local = localState.getApplicationState(entry.getKey());
            return (local == null || local.version < entry.getValue().version);
        }).collect(Collectors.toSet());

        if (logger.isTraceEnabled() && updatedStates.size() > 0)
        {
            for (Entry<ApplicationState, VersionedValue> entry : updatedStates)
            {
                logger.trace("Updating {} state version to {} for {}", entry.getKey().toString(), entry.getValue().version, addr);
            }
        }
        localState.addApplicationStates(updatedStates);

        // get rid of legacy fields once the cluster is not in mixed mode
        if (!hasMajorVersion3Nodes)
            localState.removeMajorVersion3LegacyApplicationStates();

        // need to run STATUS or STATUS_WITH_PORT first to handle BOOT_REPLACE correctly (else won't be a member, so TOKENS won't be processed)
        for (Entry<ApplicationState, VersionedValue> updatedEntry : updatedStates)
        {
            switch (updatedEntry.getKey())
            {
                default:
                    continue;
                case STATUS:
                    if (localState.containsApplicationState(ApplicationState.STATUS_WITH_PORT))
                        continue;
                case STATUS_WITH_PORT:
            }
            doOnChangeNotifications(addr, updatedEntry.getKey(), updatedEntry.getValue());
        }

        for (Entry<ApplicationState, VersionedValue> updatedEntry : updatedStates)
        {
            switch (updatedEntry.getKey())
            {
                // We should have alredy handled these two states above:
                case STATUS_WITH_PORT:
                case STATUS:
                    continue;
            }
            // filters out legacy change notifications
            // only if local state already indicates that the peer has the new fields
            if ((ApplicationState.INTERNAL_IP == updatedEntry.getKey() && localState.containsApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT))
                || (ApplicationState.RPC_ADDRESS == updatedEntry.getKey() && localState.containsApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT)))
                continue;
            doOnChangeNotifications(addr, updatedEntry.getKey(), updatedEntry.getValue());
        }
    }

    // notify that a local application state is going to change (doesn't get triggered for remote changes)
    private void doBeforeChangeNotifications(InetAddressAndPort addr, EndpointState epState, ApplicationState apState, VersionedValue newValue)
    {
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
        {
            subscriber.beforeChange(addr, epState, apState, newValue);
        }
    }

    // notify that an application state has changed
    public void doOnChangeNotifications(InetAddressAndPort addr, ApplicationState state, VersionedValue value)
    {
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
        {
            subscriber.onChange(addr, state, value);
        }
    }

    /* Request all the state for the endpoint in the gDigest */
    private void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration)
    {
        /* We are here since we have no data for this endpoint locally so request everthing. */
        deltaGossipDigestList.add(new GossipDigest(gDigest.getEndpoint(), remoteGeneration, 0));
        if (logger.isTraceEnabled())
            logger.trace("requestAll for {}", gDigest.getEndpoint());
    }

    /* Send all the data with version greater than maxRemoteVersion */
    private void sendAll(GossipDigest gDigest, Map<InetAddressAndPort, EndpointState> deltaEpStateMap, int maxRemoteVersion)
    {
        EndpointState localEpStatePtr = getStateForVersionBiggerThan(gDigest.getEndpoint(), maxRemoteVersion);
        if (localEpStatePtr != null)
            deltaEpStateMap.put(gDigest.getEndpoint(), localEpStatePtr);
    }

    /**
     * Used during a shadow round to collect the current state; this method clones the current state, no filtering
     * is done.
     *
     * During the shadow round its desirable to return gossip state for remote instances that were created by this
     * process also known as "empty", this is done for host replacement to be able to replace downed hosts that are
     * in the ring but have no state in gossip (see CASSANDRA-16213).
     *
     * This method is different than {@link #examineGossiper(List, List, Map)} with respect to how "empty" states are
     * dealt with; they are kept.
     */
    Map<InetAddressAndPort, EndpointState> examineShadowState()
    {
        logger.debug("Shadow request received, adding all states");
        Map<InetAddressAndPort, EndpointState> map = new HashMap<>();
        for (Entry<InetAddressAndPort, EndpointState> e : endpointStateMap.entrySet())
        {
            InetAddressAndPort endpoint = e.getKey();
            EndpointState state = new EndpointState(e.getValue());
            if (state.isEmptyWithoutStatus())
            {
                // We have no app states loaded for this endpoint, but we may well have
                // some state persisted in the system keyspace. This can happen in the case
                // of a full cluster bounce where one or more nodes fail to come up. As
                // gossip state is transient, the peers which do successfully start will be
                // aware of the failed nodes thanks to StorageService::initServer calling
                // Gossiper.instance::addSavedEndpoint with every endpoint in TokenMetadata,
                // which itself is populated from the system tables at startup.
                // Here we know that a peer which is starting up and attempting to perform
                // a shadow round of gossip. This peer is in one of two states:
                // * it is replacing a down node, in which case it needs to learn the tokens
                //   of the down node and optionally its host id.
                // * it needs to check that no other instance is already associated with its
                //   endpoint address and port.
                // To support both of these cases, we can add the tokens and host id from
                // the system table, if they exist. These are only ever persisted to the system
                // table when the actual node to which they apply enters the UP/NORMAL state.
                // This invariant will be preserved as nodes never persist or propagate the
                // results of a shadow round, so this communication will be strictly limited
                // to this node and the node performing the shadow round.
                UUID hostId = SystemKeyspace.loadHostIds().get(endpoint);
                if (null != hostId)
                {
                    state.addApplicationState(ApplicationState.HOST_ID,
                                                 StorageService.instance.valueFactory.hostId(hostId));
                }
                Set<Token> tokens = SystemKeyspace.loadTokens().get(endpoint);
                if (null != tokens && !tokens.isEmpty())
                {
                    state.addApplicationState(ApplicationState.TOKENS,
                                                 StorageService.instance.valueFactory.tokens(tokens));
                }
            }
            map.put(endpoint, state);
        }
        return map;
    }

    /**
     * This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
     * and the delta state are built up.
     *
     * When a {@link EndpointState} is "empty" then it is filtered out and not added to the delta state (see CASSANDRA-16213).
     */
    void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList, Map<InetAddressAndPort, EndpointState> deltaEpStateMap)
    {
        assert !gDigestList.isEmpty() : "examineGossiper called with empty digest list";
        for ( GossipDigest gDigest : gDigestList )
        {
            int remoteGeneration = gDigest.getGeneration();
            int maxRemoteVersion = gDigest.getMaxVersion();
            /* Get state associated with the end point in digest */
            EndpointState epStatePtr = endpointStateMap.get(gDigest.getEndpoint());
            /*
                Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
                then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
                request all the data for this endpoint.
            */
            if (epStatePtr != null)
            {
                int localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                /* get the max version of all keys in the state associated with this endpoint */
                int maxLocalVersion = getMaxEndpointStateVersion(epStatePtr);
                if (remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion)
                    continue;

                if (remoteGeneration > localGeneration)
                {
                    /* we request everything from the gossiper */
                    requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
                }
                else if (remoteGeneration < localGeneration)
                {
                    /* send all data with generation = localgeneration and version > -1 */
                    sendAll(gDigest, deltaEpStateMap, HeartBeatState.EMPTY_VERSION);
                }
                else if (remoteGeneration == localGeneration)
                {
                    /*
                        If the max remote version is greater then we request the remote endpoint send us all the data
                        for this endpoint with version greater than the max version number we have locally for this
                        endpoint.
                        If the max remote version is lesser, then we send all the data we have locally for this endpoint
                        with version greater than the max remote version.
                    */
                    if (maxRemoteVersion > maxLocalVersion)
                    {
                        deltaGossipDigestList.add(new GossipDigest(gDigest.getEndpoint(), remoteGeneration, maxLocalVersion));
                    }
                    else if (maxRemoteVersion < maxLocalVersion)
                    {
                        /* send all data with generation = localgeneration and version > maxRemoteVersion */
                        sendAll(gDigest, deltaEpStateMap, maxRemoteVersion);
                    }
                }
            }
            else
            {
                /* We are here since we have no data for this endpoint locally so request everything. */
                requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
            }
        }
    }

    public void start(int generationNumber)
    {
        start(generationNumber, new EnumMap<>(ApplicationState.class));
    }

    /**
     * Start the gossiper with the generation number, preloading the map of application states before starting
     */
    public void start(int generationNbr, Map<ApplicationState, VersionedValue> preloadLocalStates)
    {
        buildSeedsList();
        /* initialize the heartbeat state for this localEndpoint */
        maybeInitializeLocalState(generationNbr);
        EndpointState localState = endpointStateMap.get(getBroadcastAddressAndPort());
        localState.addApplicationStates(preloadLocalStates);
        minVersionSupplier.recompute();

        //notify snitches that Gossiper is about to start
        DatabaseDescriptor.getEndpointSnitch().gossiperStarting();
        if (logger.isTraceEnabled())
            logger.trace("gossip started with generation {}", localState.getHeartBeatState().getGeneration());

        scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask(),
                                                              Gossiper.intervalInMillis,
                                                              Gossiper.intervalInMillis,
                                                              TimeUnit.MILLISECONDS);
    }

    public synchronized Map<InetAddressAndPort, EndpointState> doShadowRound()
    {
        return doShadowRound(Collections.EMPTY_SET);
    }

    /**
     * Do a single 'shadow' round of gossip by retrieving endpoint states that will be stored exclusively in the
     * map return value, instead of endpointStateMap.
     *
     * Used when preparing to join the ring:
     * <ul>
     *     <li>when replacing a node, to get and assume its tokens</li>
     *     <li>when joining, to check that the local host id matches any previous id for the endpoint address</li>
     * </ul>
     *
     * Method is synchronized, as we use an in-progress flag to indicate that shadow round must be cleared
     * again by calling {@link Gossiper#maybeFinishShadowRound(InetAddressAndPort, boolean, Map)}. This will update
     * {@link Gossiper#endpointShadowStateMap} with received values, in order to return an immutable copy to the
     * caller of {@link Gossiper#doShadowRound()}. Therefor only a single shadow round execution is permitted at
     * the same time.
     *
     * @param peers Additional peers to try gossiping with.
     * @return endpoint states gathered during shadow round or empty map
     */
    public synchronized Map<InetAddressAndPort, EndpointState> doShadowRound(Set<InetAddressAndPort> peers)
    {
        buildSeedsList();
        // it may be that the local address is the only entry in the seed + peers
        // list in which case, attempting a shadow round is pointless
        if (seeds.isEmpty() && peers.isEmpty())
            return endpointShadowStateMap;

        boolean isSeed = DatabaseDescriptor.getSeeds().contains(getBroadcastAddressAndPort());
        // We double RING_DELAY if we're not a seed to increase chance of successful startup during a full cluster bounce,
        // giving the seeds a chance to startup before we fail the shadow round
        int shadowRoundDelay = isSeed ? StorageService.RING_DELAY_MILLIS : StorageService.RING_DELAY_MILLIS * 2;
        seedsInShadowRound.clear();
        endpointShadowStateMap.clear();
        // send a completely empty syn
        List<GossipDigest> gDigests = new ArrayList<>();
        GossipDigestSyn digestSynMessage = new GossipDigestSyn(getClusterName(), getPartitionerName(), gDigests);
        Message<GossipDigestSyn> message = Message.out(GOSSIP_DIGEST_SYN, digestSynMessage);

        inShadowRound = true;
        boolean includePeers = false;
        int slept = 0;
        try
        {
            while (true)
            {
                if (slept % 5000 == 0)
                { // CASSANDRA-8072, retry at the beginning and every 5 seconds
                    logger.trace("Sending shadow round GOSSIP DIGEST SYN to seeds {}", seeds);

                    for (InetAddressAndPort seed : seeds)
                        MessagingService.instance().send(message, seed);

                    // Send to any peers we already know about, but only if a seed didn't respond.
                    if (includePeers)
                    {
                        logger.trace("Sending shadow round GOSSIP DIGEST SYN to known peers {}", peers);
                        for (InetAddressAndPort peer : peers)
                            MessagingService.instance().send(message, peer);
                    }
                    includePeers = true;
                }

                Thread.sleep(1000);
                if (!inShadowRound)
                    break;

                slept += 1000;
                if (slept > shadowRoundDelay)
                {
                    // if we got here no peers could be gossiped to. If we're a seed that's OK, but otherwise we stop. See CASSANDRA-13851
                    if (!isSeed)
                        throw new RuntimeException("Unable to gossip with any peers");

                    inShadowRound = false;
                    break;
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        return ImmutableMap.copyOf(endpointShadowStateMap);
    }

    @VisibleForTesting
    void buildSeedsList()
    {
        for (InetAddressAndPort seed : DatabaseDescriptor.getSeeds())
        {
            if (seed.equals(getBroadcastAddressAndPort()))
                continue;
            seeds.add(seed);
        }
    }

    /**
     * JMX interface for triggering an update of the seed node list.
     */
    public List<String> reloadSeeds()
    {
        logger.trace("Triggering reload of seed node list");

        // Get the new set in the same that buildSeedsList does
        Set<InetAddressAndPort> tmp = new HashSet<>();
        try
        {
            for (InetAddressAndPort seed : DatabaseDescriptor.getSeeds())
            {
                if (seed.equals(getBroadcastAddressAndPort()))
                    continue;
                tmp.add(seed);
            }
        }
        // If using the SimpleSeedProvider invalid yaml added to the config since startup could
        // cause this to throw. Additionally, third party seed providers may throw exceptions.
        // Handle the error and return a null to indicate that there was a problem.
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Error while getting seed node list: {}", e.getLocalizedMessage());
            return null;
        }

        if (tmp.size() == 0)
        {
            logger.trace("New seed node list is empty. Not updating seed list.");
            return getSeeds();
        }

        if (tmp.equals(seeds))
        {
            logger.trace("New seed node list matches the existing list.");
            return getSeeds();
        }

        // Add the new entries
        seeds.addAll(tmp);
        // Remove the old entries
        seeds.retainAll(tmp);
        logger.trace("New seed node list after reload {}", seeds);
        return getSeeds();
    }

    /**
     * JMX endpoint for getting the list of seeds from the node
     */
    public List<String> getSeeds()
    {
        List<String> seedList = new ArrayList<>();
        for (InetAddressAndPort seed : seeds)
        {
            seedList.add(seed.toString());
        }
        return seedList;
    }

    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    public void maybeInitializeLocalState(int generationNbr)
    {
        HeartBeatState hbState = new HeartBeatState(generationNbr);
        EndpointState localState = new EndpointState(hbState);
        localState.markAlive();
        endpointStateMap.putIfAbsent(getBroadcastAddressAndPort(), localState);
    }

    public void forceNewerGeneration()
    {
        EndpointState epstate = endpointStateMap.get(getBroadcastAddressAndPort());
        epstate.getHeartBeatState().forceNewerGenerationUnsafe();
    }


    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    public void addSavedEndpoint(InetAddressAndPort ep)
    {
        checkProperThreadForStateMutation();
        if (ep.equals(getBroadcastAddressAndPort()))
        {
            logger.debug("Attempt to add self as saved endpoint");
            return;
        }

        //preserve any previously known, in-memory data about the endpoint (such as DC, RACK, and so on)
        EndpointState epState = endpointStateMap.get(ep);
        if (epState != null)
        {
            logger.debug("not replacing a previous epState for {}, but reusing it: {}", ep, epState);
            epState.setHeartBeatState(HeartBeatState.empty());
        }
        else
        {
            epState = new EndpointState(HeartBeatState.empty());
            logger.info("Adding {} as there was no previous epState; new state is {}", ep, epState);
        }

        epState.markDead();
        endpointStateMap.put(ep, epState);
        silentlyMarkDead(ep, epState);
        if (logger.isTraceEnabled())
            logger.trace("Adding saved endpoint {} {}", ep, epState.getHeartBeatState().getGeneration());
    }

    private void addLocalApplicationStateInternal(ApplicationState state, VersionedValue value)
    {
        assert taskLock.isHeldByCurrentThread();
        InetAddressAndPort epAddr = getBroadcastAddressAndPort();
        EndpointState epState = endpointStateMap.get(epAddr);
        assert epState != null : "Can't find endpoint state for " + epAddr;
        // Fire "before change" notifications:
        doBeforeChangeNotifications(epAddr, epState, state, value);
        // Notifications may have taken some time, so preventively raise the version
        // of the new value, otherwise it could be ignored by the remote node
        // if another value with a newer version was received in the meantime:
        value = StorageService.instance.valueFactory.cloneWithHigherVersion(value);
        // Add to local application state and fire "on change" notifications:
        epState.addApplicationState(state, value);
        doOnChangeNotifications(epAddr, state, value);
    }

    public void addLocalApplicationState(ApplicationState applicationState, VersionedValue value)
    {
        addLocalApplicationStates(Arrays.asList(Pair.create(applicationState, value)));
    }

    public void addLocalApplicationStates(List<Pair<ApplicationState, VersionedValue>> states)
    {
        taskLock.lock();
        try
        {
            for (Pair<ApplicationState, VersionedValue> pair : states)
            {
               addLocalApplicationStateInternal(pair.left, pair.right);
            }
        }
        finally
        {
            taskLock.unlock();
        }

    }

    public void stop()
    {
        EndpointState mystate = endpointStateMap.get(getBroadcastAddressAndPort());
        if (mystate != null && !isSilentShutdownState(mystate) && StorageService.instance.isJoined())
        {
            logger.info("Announcing shutdown");
            addLocalApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.shutdown(true));
            addLocalApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.shutdown(true));
            // clone endpointstate to avoid it changing between serializedSize and serialize calls
            EndpointState clone = new EndpointState(mystate);
            Message<GossipShutdown> message = Message.out(Verb.GOSSIP_SHUTDOWN, new GossipShutdown(clone));
            for (InetAddressAndPort ep : liveEndpoints)
                MessagingService.instance().send(message, ep);
            Uninterruptibles.sleepUninterruptibly(SHUTDOWN_ANNOUNCE_DELAY_IN_MS.getInt(), TimeUnit.MILLISECONDS);
        }
        else
            logger.warn("No local state, state is in silent shutdown, or node hasn't joined, not announcing shutdown");
        if (scheduledGossipTask != null)
            scheduledGossipTask.cancel(false);
    }

    public boolean isEnabled()
    {
        ScheduledFuture<?> scheduledGossipTask = this.scheduledGossipTask;
        return (scheduledGossipTask != null) && (!scheduledGossipTask.isCancelled());
    }

    public boolean sufficientForStartupSafetyCheck(Map<InetAddressAndPort, EndpointState> epStateMap)
    {
        // it is possible for a previously queued ack to be sent to us when we come back up in shadow
        EndpointState localState = epStateMap.get(FBUtilities.getBroadcastAddressAndPort());
        // return false if response doesn't contain state necessary for safety check
        return localState == null || isDeadState(localState) || localState.containsApplicationState(ApplicationState.HOST_ID);
    }

    protected void maybeFinishShadowRound(InetAddressAndPort respondent, boolean isInShadowRound, Map<InetAddressAndPort, EndpointState> epStateMap)
    {
        if (inShadowRound)
        {
            if (!isInShadowRound)
            {
                if (!sufficientForStartupSafetyCheck(epStateMap))
                {
                    logger.debug("Not exiting shadow round because received ACK with insufficient states {} -> {}",
                                 FBUtilities.getBroadcastAddressAndPort(), epStateMap.get(FBUtilities.getBroadcastAddressAndPort()));
                    return;
                }

                if (!seeds.contains(respondent))
                    logger.warn("Received an ack from {}, who isn't a seed. Ensure your seed list includes a live node. Exiting shadow round",
                                respondent);
                logger.debug("Received a regular ack from {}, can now exit shadow round", respondent);
                // respondent sent back a full ack, so we can exit our shadow round
                endpointShadowStateMap.putAll(epStateMap);
                inShadowRound = false;
                seedsInShadowRound.clear();
            }
            else
            {
                // respondent indicates it too is in a shadow round, if all seeds
                // are in this state then we can exit our shadow round. Otherwise,
                // we keep retrying the SR until one responds with a full ACK or
                // we learn that all seeds are in SR.
                logger.debug("Received an ack from {} indicating it is also in shadow round", respondent);
                seedsInShadowRound.add(respondent);
                if (seedsInShadowRound.containsAll(seeds))
                {
                    logger.debug("All seeds are in a shadow round, clearing this node to exit its own");
                    inShadowRound = false;
                    seedsInShadowRound.clear();
                }
            }
        }
    }

    public boolean isInShadowRound()
    {
        return inShadowRound;
    }

    /**
     * Creates a new dead {@link EndpointState} that is {@link EndpointState#isEmptyWithoutStatus() empty}.  This is used during
     * host replacement for edge cases where the seed notified that the endpoint was empty, so need to add such state
     * into gossip explicitly (as empty endpoints are not gossiped outside of the shadow round).
     *
     * see CASSANDRA-16213
     */
    public void initializeUnreachableNodeUnsafe(InetAddressAndPort addr)
    {
        EndpointState state = new EndpointState(HeartBeatState.empty());
        state.markDead();
        EndpointState oldState = endpointStateMap.putIfAbsent(addr, state);
        if (null != oldState)
        {
            throw new RuntimeException("Attempted to initialize endpoint state for unreachable node, " +
                                       "but found existing endpoint state for it.");
        }
    }

    @VisibleForTesting
    public void initializeNodeUnsafe(InetAddressAndPort addr, UUID uuid, int generationNbr)
    {
        initializeNodeUnsafe(addr, uuid, MessagingService.current_version, generationNbr);
    }

    @VisibleForTesting
    public void initializeNodeUnsafe(InetAddressAndPort addr, UUID uuid, int netVersion, int generationNbr)
    {
        HeartBeatState hbState = new HeartBeatState(generationNbr);
        EndpointState newState = new EndpointState(hbState);
        newState.markAlive();
        EndpointState oldState = endpointStateMap.putIfAbsent(addr, newState);
        EndpointState localState = oldState == null ? newState : oldState;

        // always add the version state
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, StorageService.instance.valueFactory.networkVersion(netVersion));
        states.put(ApplicationState.HOST_ID, StorageService.instance.valueFactory.hostId(uuid));
        states.put(ApplicationState.RPC_ADDRESS, StorageService.instance.valueFactory.rpcaddress(addr.getAddress()));
        states.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, StorageService.instance.valueFactory.internalAddressAndPort(addr));
        states.put(ApplicationState.RELEASE_VERSION, StorageService.instance.valueFactory.releaseVersion());
        localState.addApplicationStates(states);
    }

    @VisibleForTesting
    public void injectApplicationState(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        EndpointState localState = endpointStateMap.get(endpoint);
        localState.addApplicationState(state, value);
    }

    public long getEndpointDowntime(String address) throws UnknownHostException
    {
        return getEndpointDowntime(InetAddressAndPort.getByName(address));
    }

    public int getCurrentGenerationNumber(String address) throws UnknownHostException
    {
        return getCurrentGenerationNumber(InetAddressAndPort.getByName(address));
    }

    public void addExpireTimeForEndpoint(InetAddressAndPort endpoint, long expireTime)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("adding expire time for endpoint : {} ({})", endpoint, expireTime);
        }
        expireTimeEndpointMap.put(endpoint, expireTime);
    }

    public static long computeExpireTime()
    {
        return currentTimeMillis() + aVeryLongTime;
    }

    public Map<String, List<String>> getReleaseVersionsWithPort()
    {
        Map<String, List<String>> results = new HashMap<>();
        Iterable<InetAddressAndPort> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());

        for (InetAddressAndPort host : allHosts)
        {
            CassandraVersion version = getReleaseVersion(host);
            String stringVersion = version == null ? "" : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddressAndPort());
        }

        return results;
    }

    @Nullable
    public UUID getSchemaVersion(InetAddressAndPort ep)
    {
        EndpointState state = getEndpointStateForEndpoint(ep);
        return state != null ? state.getSchemaVersion() : null;
    }

    public static void waitToSettle()
    {
        int forceAfter = GOSSIPER_SKIP_WAITING_TO_SETTLE.getInt();
        if (forceAfter == 0)
        {
            return;
        }
        final int GOSSIP_SETTLE_MIN_WAIT_MS = CassandraRelevantProperties.GOSSIP_SETTLE_MIN_WAIT_MS.getInt();
        final int GOSSIP_SETTLE_POLL_INTERVAL_MS = CassandraRelevantProperties.GOSSIP_SETTLE_POLL_INTERVAL_MS.getInt();
        final int GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED = CassandraRelevantProperties.GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED.getInt();

        logger.info("Waiting for gossip to settle...");
        Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_MIN_WAIT_MS, TimeUnit.MILLISECONDS);
        int totalPolls = 0;
        int numOkay = 0;
        int epSize = Gossiper.instance.getEndpointCount();
        while (numOkay < GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
        {
            Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            int currentSize = Gossiper.instance.getEndpointCount();
            totalPolls++;
            if (currentSize == epSize)
            {
                logger.debug("Gossip looks settled.");
                numOkay++;
            }
            else
            {
                logger.info("Gossip not settled after {} polls.", totalPolls);
                numOkay = 0;
            }
            epSize = currentSize;
            if (forceAfter > 0 && totalPolls > forceAfter)
            {
                logger.warn("Gossip not settled but startup forced by cassandra.skip_wait_for_gossip_to_settle. Gossip total polls: {}",
                            totalPolls);
                break;
            }
        }
        if (totalPolls > GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
            logger.info("Gossip settled after {} extra polls; proceeding", totalPolls - GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED);
        else
            logger.info("No gossip backlog; proceeding");
    }

    /**
     * Blockingly wait for all live nodes to agree on the current schema version.
     *
     * @param maxWait maximum time to wait for schema agreement
     * @param unit TimeUnit of maxWait
     * @return true if agreement was reached, false if not
     */
    public boolean waitForSchemaAgreement(long maxWait, TimeUnit unit, BooleanSupplier abortCondition)
    {
        int waited = 0;
        int toWait = 50;

        Set<InetAddressAndPort> members = getLiveTokenOwners();

        while (true)
        {
            if (nodesAgreeOnSchema(members))
                return true;

            if (waited >= unit.toMillis(maxWait) || abortCondition.getAsBoolean())
                return false;

            Uninterruptibles.sleepUninterruptibly(toWait, TimeUnit.MILLISECONDS);
            waited += toWait;
            toWait = Math.min(1000, toWait * 2);
        }
    }

    /**
     * Returns {@code false} only if the information about the version of each node in the cluster is available and
     * ALL the nodes are on 4.0+ (regardless of the patch version).
     */
    public boolean hasMajorVersion3OrUnknownNodes()
    {
        return isUpgradingFromVersionLowerThan(CassandraVersion.CASSANDRA_4_0) || // this is quite obvious
               // however if we discovered only nodes at current version so far (in particular only this node),
               // but still there are nodes with unknown version, we also want to report that the cluster may have nodes at 3.x
               hasNodeWithUnknownVersion;
    }

    /**
     * Returns {@code true} if there are nodes on version lower than the provided version
     */
    public boolean isUpgradingFromVersionLowerThan(CassandraVersion referenceVersion)
    {
        CassandraVersion v = upgradeFromVersionMemoized.get();
        if (CassandraVersion.NULL_VERSION.equals(v) && scheduledGossipTask == null)
            return false;

        return v != null && v.compareTo(referenceVersion) < 0;
    }

    private boolean nodesAgreeOnSchema(Collection<InetAddressAndPort> nodes)
    {
        UUID expectedVersion = null;

        for (InetAddressAndPort node : nodes)
        {
            EndpointState state = getEndpointStateForEndpoint(node);
            UUID remoteVersion = state.getSchemaVersion();

            if (null == expectedVersion)
                expectedVersion = remoteVersion;

            if (null == expectedVersion || !expectedVersion.equals(remoteVersion))
                return false;
        }

        return true;
    }

    @VisibleForTesting
    public void stopShutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        stop();
        ExecutorUtils.shutdownAndWait(timeout, unit, executor);
    }

    @Nullable
    public CassandraVersion getMinVersion(long delay, TimeUnit timeUnit)
    {
        try
        {
            return minVersionSupplier.get(delay, timeUnit);
        }
        catch (TimeoutException e)
        {
            // Timeouts here are harmless: they won't cause reprepares and may only
            // cause the old version of the hash to be kept for longer
            return null;
        }
        catch (Throwable e)
        {
            logger.error("Caught an exception while waiting for min version", e);
            return null;
        }
    }

    @Nullable
    private String getReleaseVersionString(InetAddressAndPort ep)
    {
        EndpointState state = getEndpointStateForEndpoint(ep);
        if (state == null)
            return null;

        VersionedValue value = state.getApplicationState(ApplicationState.RELEASE_VERSION);
        return value == null ? null : value.value;
    }

    private CassandraVersion computeMinVersion()
    {
        CassandraVersion minVersion = null;

        for (InetAddressAndPort addr : Iterables.concat(Gossiper.instance.getLiveMembers(),
                                                        Gossiper.instance.getUnreachableMembers()))
        {
            String versionString = getReleaseVersionString(addr);
            // Raced with changes to gossip state, wait until next iteration
            if (versionString == null)
                return null;

            CassandraVersion version;

            try
            {
                version = new CassandraVersion(versionString);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                String message = String.format("Can't parse version string %s", versionString);
                logger.warn(message);
                if (logger.isDebugEnabled())
                    logger.debug(message, t);
                return null;
            }

            if (minVersion == null || version.compareTo(minVersion) < 0)
                minVersion = version;
        }

        return minVersion;
    }

    @Override
    public boolean getLooseEmptyEnabled()
    {
        return EndpointState.LOOSE_DEF_OF_EMPTY_ENABLED;
    }

    @Override
    public void setLooseEmptyEnabled(boolean enabled)
    {
        logger.info("Setting loose definition of empty to {}", enabled);
        EndpointState.LOOSE_DEF_OF_EMPTY_ENABLED = enabled;
    }

    public void unsafeSetEnabled()
    {
        scheduledGossipTask = new NotScheduledFuture<>();
        firstSynSendAt = 1;
    }

    public Collection<InetAddressAndPort> unsafeClearRemoteState()
    {
        List<InetAddressAndPort> removed = new ArrayList<>();
        for (InetAddressAndPort ep : endpointStateMap.keySet())
        {
            if (ep.equals(getBroadcastAddressAndPort()))
                continue;

            for (IEndpointStateChangeSubscriber subscriber : subscribers)
                subscriber.onRemove(ep);

            removed.add(ep);
        }
        this.endpointStateMap.keySet().retainAll(Collections.singleton(getBroadcastAddressAndPort()));
        this.endpointShadowStateMap.keySet().retainAll(Collections.singleton(getBroadcastAddressAndPort()));
        this.expireTimeEndpointMap.keySet().retainAll(Collections.singleton(getBroadcastAddressAndPort()));
        this.justRemovedEndpoints.keySet().retainAll(Collections.singleton(getBroadcastAddressAndPort()));
        this.unreachableEndpoints.keySet().retainAll(Collections.singleton(getBroadcastAddressAndPort()));
        return removed;
    }

    public void unsafeGossipWith(InetAddressAndPort ep)
    {
        /* Update the local heartbeat counter. */
        EndpointState epState = endpointStateMap.get(getBroadcastAddressAndPort());
        if (epState != null)
        {
            epState.getHeartBeatState().updateHeartBeat();
            if (logger.isTraceEnabled())
                logger.trace("My heartbeat is now {}", epState.getHeartBeatState().getHeartBeatVersion());
        }

        final List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
        Gossiper.instance.makeGossipDigest(gDigests);

        GossipDigestSyn digestSynMessage = new GossipDigestSyn(getClusterName(),
                getPartitionerName(),
                gDigests);
        Message<GossipDigestSyn> message = Message.out(GOSSIP_DIGEST_SYN, digestSynMessage);

        MessagingService.instance().send(message, ep);
    }

    public void unsafeSendShutdown(InetAddressAndPort to)
    {
        Message<?> message = Message.out(Verb.GOSSIP_SHUTDOWN, noPayload);
        MessagingService.instance().send(message, to);
    }

    public void unsafeSendLocalEndpointStateTo(InetAddressAndPort ep)
    {
        /* Update the local heartbeat counter. */
        EndpointState epState = endpointStateMap.get(getBroadcastAddressAndPort());
        if (epState == null)
            throw new IllegalStateException();

        GossipDigestAck2 digestAck2Message = new GossipDigestAck2(Collections.singletonMap(getBroadcastAddressAndPort(), epState));
        Message<GossipDigestAck2> message = Message.out(Verb.GOSSIP_DIGEST_ACK2, digestAck2Message);
        MessagingService.instance().send(message, ep);
    }
}
