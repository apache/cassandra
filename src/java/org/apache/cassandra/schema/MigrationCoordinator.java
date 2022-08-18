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

package org.apache.cassandra.schema;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.FutureTask;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_ENDPOINTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_VERSIONS;
import static org.apache.cassandra.net.Verb.SCHEMA_PUSH_REQ;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

/**
 * Migration coordinator is responsible for tracking schema versions on various nodes and, if needed, synchronize the
 * schema. It performs periodic checks and if there is a schema version mismatch between the current node and the other
 * node, it pulls the schema and applies the changes locally through the callback.
 * <p>
 * It works in close cooperation with {@link DefaultSchemaUpdateHandler} which is responsible for maintaining local
 * schema metadata stored in {@link SchemaKeyspace}.
 */
@Simulate(with = MONITORS)
public class MigrationCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationCoordinator.class);
    private static final Future<Void> FINISHED_FUTURE = ImmediateFuture.success(null);

    private static LongSupplier getUptimeFn = () -> ManagementFactory.getRuntimeMXBean().getUptime();

    @VisibleForTesting
    public static void setUptimeFn(LongSupplier supplier)
    {
        getUptimeFn = supplier;
    }

    private static final int MIGRATION_DELAY_IN_MS = CassandraRelevantProperties.MIGRATION_DELAY.getInt();
    public static final int MAX_OUTSTANDING_VERSION_REQUESTS = 3;

    private static ImmutableSet<UUID> getIgnoredVersions()
    {
        String s = IGNORED_SCHEMA_CHECK_VERSIONS.getString();
        if (s == null || s.isEmpty())
            return ImmutableSet.of();

        ImmutableSet.Builder<UUID> versions = ImmutableSet.builder();
        for (String version : s.split(","))
        {
            versions.add(UUID.fromString(version));
        }

        return versions.build();
    }

    private static final Set<UUID> IGNORED_VERSIONS = getIgnoredVersions();

    private static Set<InetAddressAndPort> getIgnoredEndpoints()
    {
        Set<InetAddressAndPort> endpoints = new HashSet<>();

        String s = IGNORED_SCHEMA_CHECK_ENDPOINTS.getString();
        if (s == null || s.isEmpty())
            return endpoints;

        for (String endpoint : s.split(","))
        {
            try
            {
                endpoints.add(InetAddressAndPort.getByName(endpoint));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }

        return endpoints;
    }

    static class VersionInfo
    {
        final UUID version;

        // the requests which have been scheduled for pull this version
        final Set<InetAddressAndPort> outstandingRequests = Sets.newConcurrentHashSet();

        // pending requests - an endpoint is added to the queue whenever we discover it offers this version;
        // when for some reason we cannot fetch the schema from it, it is moved to the end of the queue so that it
        // is tried later unless we get the schema of that version before
        final Deque<InetAddressAndPort> requestQueue = new ArrayDeque<>();

        private final WaitQueue waitQueue = newWaitQueue();

        private volatile boolean receivedSchema;

        VersionInfo(UUID version)
        {
            this.version = version;
        }

        WaitQueue.Signal register()
        {
            return waitQueue.register();
        }

        void markReceived()
        {
            if (receivedSchema)
                return;

            receivedSchema = true;
            waitQueue.signalAll();
        }

        boolean wasReceived()
        {
            return receivedSchema;
        }

        @Override
        public String toString()
        {
            return "VersionInfo{" +
                   "version=" + version +
                   ", outstandingRequests=" + outstandingRequests +
                   ", requestQueue=" + requestQueue +
                   ", waitQueue=" + waitQueue.getWaiting() +
                   ", receivedSchema=" + receivedSchema +
                   '}';
        }
    }

    private final Map<UUID, VersionInfo> versionInfo = new HashMap<>();
    private final Map<InetAddressAndPort, UUID> endpointVersions = new HashMap<>();
    private final Set<InetAddressAndPort> ignoredEndpoints = getIgnoredEndpoints();
    private final ScheduledExecutorService periodicCheckExecutor;
    private final MessagingService messagingService;
    private final AtomicReference<ScheduledFuture<?>> periodicPullTask = new AtomicReference<>();
    private final int maxOutstandingVersionRequests;
    private final Gossiper gossiper;
    private final Supplier<UUID> schemaVersion;
    private final BiConsumer<InetAddressAndPort, Collection<Mutation>> schemaUpdateCallback;

    final ExecutorPlus executor;

    /**
     * Creates but does not start migration coordinator instance.
     *
     * @param messagingService      messaging service instance used to communicate with other nodes for pulling schema
     *                              and pushing changes
     * @param periodicCheckExecutor executor on which the periodic checks are scheduled
     */
    MigrationCoordinator(MessagingService messagingService,
                         ExecutorPlus executor,
                         ScheduledExecutorService periodicCheckExecutor,
                         int maxOutstandingVersionRequests,
                         Gossiper gossiper,
                         Supplier<UUID> schemaVersionSupplier,
                         BiConsumer<InetAddressAndPort, Collection<Mutation>> schemaUpdateCallback)
    {
        this.messagingService = messagingService;
        this.executor = executor;
        this.periodicCheckExecutor = periodicCheckExecutor;
        this.maxOutstandingVersionRequests = maxOutstandingVersionRequests;
        this.gossiper = gossiper;
        this.schemaVersion = schemaVersionSupplier;
        this.schemaUpdateCallback = schemaUpdateCallback;
    }

    void start()
    {
        logger.info("Starting migration coordinator and scheduling pulling schema versions");
        announce(schemaVersion.get());
        periodicPullTask.updateAndGet(curTask -> curTask == null
                                                 ? periodicCheckExecutor.scheduleWithFixedDelay(this::pullUnreceivedSchemaVersions, 60, 60, TimeUnit.SECONDS)
                                                 : curTask);
    }

    private synchronized void pullUnreceivedSchemaVersions()
    {
        logger.debug("Pulling unreceived schema versions: {}", versionInfo.values().stream().map(v -> v.version + " from " + getAvailableEndpointsForVersion(v.version)).collect(Collectors.joining(", ")));
        cleanUpVersions();
        for (VersionInfo info : versionInfo.values())
        {
            if (info.outstandingRequests.size() > 0)
            {
                logger.debug("Skipping pull of schema at version {} because there are in-flight requests for that version ({})", info.version, info.outstandingRequests);
                continue;
            }

            maybePullSchema(info);
        }
    }

    private void invalidate(VersionInfo info)
    {
        if (!info.wasReceived())
            info.waitQueue.signalAll();
        versionInfo.remove(info.version);
        logger.debug("Invalidated schema version {}", info.version);
    }

    private synchronized Future<Void> maybePullSchema(VersionInfo info)
    {
        UUID localSchemaVersion = schemaVersion.get();

        if (localSchemaVersion == null)
        {
            // In this case we are just postponing the pull of this schema version until the local schema is initialized
            logger.debug("Not pulling schema version {} because the local schema is not known yet", info.version);
            return FINISHED_FUTURE;
        }

        Set<InetAddressAndPort> endpoints;
        if (info.wasReceived() || info.version.equals(localSchemaVersion) || (endpoints = getAvailableEndpointsForVersion(info.version)).isEmpty())
        {
            // in any of the above cases there is no reason to care about this version any longer - it is either already fetched or not available at any node
            logger.debug("Cancelled pulling schema version {} because the request is stale", info.version);
            invalidate(info);
            return FINISHED_FUTURE;
        }

        if (info.outstandingRequests.size() >= maxOutstandingVersionRequests)
        {
            logger.debug("Not pulling schema version {} because it is already being pulled", info.version);
            return FINISHED_FUTURE;
        }

        for (int i = 0, isize = info.requestQueue.size(); i < isize; i++)
        {
            InetAddressAndPort endpoint = info.requestQueue.remove();
            if (!endpoints.contains(endpoint))
            {
                logger.debug("Skipping request to get schema version {} from {} because that enpoint does not contain that schema version any longer", info.version, endpoint);
                continue;
            }

            if (shouldPullFromEndpoint(endpoint).orElse(false))
            {
                if (info.outstandingRequests.add(endpoint))
                {
                    logger.debug("Scheduling pull of {}", info);
                    return scheduleSchemaPull(endpoint, info);
                }
            }
            else
            {
                // return to queue only if the node was determined as valid so that it makes sense to try again later
                logger.debug("Returning request of {} to the queue", info);
                info.requestQueue.offer(endpoint);
            }
        }

        // no suitable endpoints were found, check again in a minute, the periodic task will pick it up
        return FINISHED_FUTURE;
    }

    synchronized Map<UUID, Set<InetAddressAndPort>> outstandingVersions()
    {
        HashMap<UUID, Set<InetAddressAndPort>> map = new HashMap<>();
        for (VersionInfo info : versionInfo.values())
            if (!info.wasReceived())
                map.put(info.version, ImmutableSet.copyOf(getAvailableEndpointsForVersion(info.version)));
        return map;
    }

    @VisibleForTesting
    VersionInfo getVersionInfoUnsafe(UUID version)
    {
        return versionInfo.get(version);
    }

    /**
     * It verifies whether we should pull schema form the provided endpoint. The return value is either true, which
     * obviously means that we can pull schema from it, false - which means that the endpoint is permanently invalid
     * for pulling schema form it (until it gets restarted), none - which means that we should not pull from this
     * endpoint at the moment, but we may try again later.
     */
    private Optional<Boolean> shouldPullFromEndpoint(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return Optional.of(false);

        EndpointState state = gossiper.getEndpointStateForEndpoint(endpoint);
        if (state == null)
            return Optional.empty();

        final String releaseVersion = state.getApplicationState(ApplicationState.RELEASE_VERSION).value;
        final String ourMajorVersion = FBUtilities.getReleaseVersionMajor();

        if (!releaseVersion.startsWith(ourMajorVersion))
        {
            logger.debug("Not pulling schema from {} because release version in Gossip is not major version {}, it is {}",
                         endpoint, ourMajorVersion, releaseVersion);
            return Optional.of(false);
        }

        if (!messagingService.versions.knows(endpoint))
        {
            logger.debug("Not pulling schema from {} because their messaging version is unknown", endpoint);
            return Optional.empty();
        }

        if (messagingService.versions.getRaw(endpoint) != MessagingService.current_version)
        {
            logger.debug("Not pulling schema from {} because their schema format is incompatible", endpoint);
            return Optional.of(false);
        }

        if (gossiper.isGossipOnlyMember(endpoint))
        {
            logger.debug("Not pulling schema from {} because it's a gossip only member", endpoint);
            return Optional.empty();
        }

        return Optional.of(true);
    }

    private boolean shouldPullImmediately(InetAddressAndPort endpoint, UUID version)
    {
        UUID localSchemaVersion = schemaVersion.get();
        if (SchemaConstants.emptyVersion.equals(localSchemaVersion) || getUptimeFn.getAsLong() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Immediately submitting migration task for {}, " +
                         "schema versions: local={}, remote={}",
                         endpoint,
                         DistributedSchema.schemaVersionToString(localSchemaVersion),
                         DistributedSchema.schemaVersionToString(version));
            return true;
        }

        if (!SchemaConstants.emptyVersion.equals(localSchemaVersion))
        {
            logger.debug("Not submitting migration immediately because the current schema is not empty: {} != {}", localSchemaVersion, SchemaConstants.emptyVersion);
        }
        else
        {
            logger.debug("Not submitting migration immediately because the instance is running longer than the specified migration delay of {}ms", MIGRATION_DELAY_IN_MS);
        }

        return false;
    }

    /**
     * If a previous schema update brought our version the same as the incoming schema, don't apply it
     */
    private synchronized boolean shouldApplySchemaFor(VersionInfo info)
    {
        if (info.wasReceived())
            return false;
        return !Objects.equals(schemaVersion.get(), info.version);
    }

    private void removeEndpoint(InetAddressAndPort endpoint)
    {
        endpointVersions.remove(endpoint);
        versionInfo.forEach((v, info) -> info.requestQueue.removeIf(e -> e.equals(endpoint)));
    }

    synchronized Future<Void> reportEndpointVersion(InetAddressAndPort endpoint, UUID version)
    {
        logger.debug("Reported endpoint/version: {}/{}", endpoint, version);
        if (ignoredEndpoints.contains(endpoint) || !shouldPullFromEndpoint(endpoint).orElse(true))
        {
            removeEndpoint(endpoint);
            logger.debug("Skipping endpoint/version {}/{} because the endpoint is either ignored or invalid", endpoint, version);
            return FINISHED_FUTURE;
        }

        if (IGNORED_VERSIONS.contains(version))
        {
            getAvailableEndpointsForVersion(version).forEach(endpointVersions::remove);
            cleanUpVersions();
            logger.debug("Skipping endpoint/version {}/{} because the version is ignored", endpoint, version);
            return FINISHED_FUTURE;
        }

        UUID current = endpointVersions.put(endpoint, version);
        cleanUpVersions();

        if (Objects.equals(current, version))
        {
            logger.debug("Skipping endpoint/version {}/{} because this is the version already reported by this endpoint", endpoint, version);
            return FINISHED_FUTURE;
        }

        VersionInfo info = versionInfo.computeIfAbsent(version, VersionInfo::new);
        if (Objects.equals(schemaVersion.get(), version))
        {
            // note that this is very likely to be true for the local node schema updates - after the schema is updated
            // we update the schema version in Gossiper, which in turn notify listeners about the onChange event, and
            // we eventually get there
            logger.debug("Marking version {} as received as it is equal to the current version", version);
            info.markReceived();
            invalidate(info);
            return FINISHED_FUTURE;
        }
        else
        {
            // if the version we got notified about is something else than our current version, we add a request to pull
            // that version from that node
            info.requestQueue.addFirst(endpoint);
            return maybePullSchema(info);
        }
    }

    /**
     * This invalidates all the versions which not available at any endpoint and there is no in-flight requests for them.
     */
    private synchronized void cleanUpVersions()
    {
        Set<VersionInfo> selected = versionInfo.values().stream()
                                               .filter(info -> getAvailableEndpointsForVersion(info.version).isEmpty())
                                               .collect(Collectors.toSet());
        selected.forEach(this::invalidate);
    }

    synchronized void reset()
    {
        // first, remove all entries
        Iterator<Map.Entry<UUID, VersionInfo>> it = versionInfo.entrySet().iterator();
        while (it.hasNext())
        {
            Map.Entry<UUID, VersionInfo> entry = it.next();
            entry.getValue().waitQueue.signal();
            it.remove();
        }

        this.endpointVersions.clear();

        // now report again the versions we are aware of
        gossiper.getLiveMembers().forEach(endpoint -> {
            EndpointState state = gossiper.getEndpointStateForEndpoint(endpoint);
            if (state != null)
            {
                UUID v = state.getSchemaVersion();
                if (v != null)
                {
                    reportEndpointVersion(endpoint, v);
                }
            }
        });
    }

    synchronized void removeAndIgnoreEndpoint(InetAddressAndPort endpoint)
    {
        logger.debug("Removing and ignoring endpoint {}", endpoint);
        Preconditions.checkArgument(endpoint != null);
        // TODO The endpoint address is now ignored but when a node with the same address is added again later,
        //  there will be no way to include it in schema synchronization other than restarting each other node
        ignoredEndpoints.add(endpoint);
        removeEndpoint(endpoint);
        cleanUpVersions();
    }

    private Future<Void> scheduleSchemaPull(InetAddressAndPort endpoint, VersionInfo info)
    {
        FutureTask<Void> task = new FutureTask<>(() -> pullSchema(endpoint, new Callback(endpoint, info)));

        if (shouldPullImmediately(endpoint, info.version))
        {
            logger.debug("Pulling {} immediatelly from {}", info, endpoint);
            submitToMigrationIfNotShutdown(task);
        }
        else
        {
            logger.debug("Postponing pull of {} from {} for {}ms", info, endpoint, MIGRATION_DELAY_IN_MS);
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> submitToMigrationIfNotShutdown(task), MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }

        return task;
    }

    private Future<Collection<Mutation>> pullSchemaFrom(InetAddressAndPort endpoint)
    {
        AsyncPromise<Collection<Mutation>> result = new AsyncPromise<>();
        return submitToMigrationIfNotShutdown(() -> pullSchema(endpoint, new RequestCallback<Collection<Mutation>>()
        {
            @Override
            public void onResponse(Message<Collection<Mutation>> msg)
            {
                result.setSuccess(msg.payload);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                result.setFailure(new RuntimeException("Failed to get schema from " + from + ". The failure reason was: " + failureReason));
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        })).flatMap(ignored -> result);
    }

    void announce(UUID schemaVersion)
    {
        if (gossiper.isEnabled())
            gossiper.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(schemaVersion));
        SchemaDiagnostics.versionAnnounced(Schema.instance);
    }

    private Future<?> submitToMigrationIfNotShutdown(Runnable task)
    {
        boolean skipped = false;
        try
        {
            if (executor.isShutdown() || executor.isTerminated())
            {
                skipped = true;
                return ImmediateFuture.success(null);
            }
            return executor.submit(task);
        }
        catch (RejectedExecutionException ex)
        {
            skipped = true;
            return ImmediateFuture.success(null);
        }
        finally
        {
            if (skipped)
            {
                logger.info("Skipped scheduled pulling schema from other nodes: the MIGRATION executor service has been shutdown.");
            }
        }
    }

    private class Callback implements RequestCallback<Collection<Mutation>>
    {
        final InetAddressAndPort endpoint;
        final VersionInfo info;

        public Callback(InetAddressAndPort endpoint, VersionInfo info)
        {
            this.endpoint = endpoint;
            this.info = info;
        }

        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            fail();
        }

        Future<Void> fail()
        {
            return pullComplete(endpoint, info, false);
        }

        public void onResponse(Message<Collection<Mutation>> message)
        {
            response(message.payload);
        }

        Future<Void> response(Collection<Mutation> mutations)
        {
            synchronized (info)
            {
                if (shouldApplySchemaFor(info))
                {
                    try
                    {
                        schemaUpdateCallback.accept(endpoint, mutations);
                    }
                    catch (Exception e)
                    {
                        logger.error(String.format("Unable to merge schema from %s", endpoint), e);
                        return fail();
                    }
                }
                return pullComplete(endpoint, info, true);
            }
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }

    private void pullSchema(InetAddressAndPort endpoint, RequestCallback<Collection<Mutation>> callback)
    {
        if (!gossiper.isAlive(endpoint))
        {
            logger.warn("Can't send schema pull request: node {} is down.", endpoint);
            callback.onFailure(endpoint, RequestFailureReason.UNKNOWN);
            return;
        }

        // There is a chance that quite some time could have passed between now and the MM#maybeScheduleSchemaPull(),
        // potentially enough for the endpoint node to restart - which is an issue if it does restart upgraded, with
        // a higher major.
        if (!shouldPullFromEndpoint(endpoint).orElse(false))
        {
            logger.info("Skipped sending a migration request: node {} has a higher major version now.", endpoint);
            callback.onFailure(endpoint, RequestFailureReason.UNKNOWN);
            return;
        }

        logger.debug("Requesting schema from {}", endpoint);
        sendMigrationMessage(endpoint, callback);
    }

    private void sendMigrationMessage(InetAddressAndPort endpoint, RequestCallback<Collection<Mutation>> callback)
    {
        Message<NoPayload> message = Message.out(Verb.SCHEMA_PULL_REQ, NoPayload.noPayload);
        logger.info("Sending schema pull request to {}", endpoint);
        messagingService.sendWithCallback(message, endpoint, callback);
    }

    private synchronized Future<Void> pullComplete(InetAddressAndPort endpoint, VersionInfo info, boolean wasSuccessful)
    {
        info.outstandingRequests.remove(endpoint);

        if (wasSuccessful)
        {
            info.markReceived();
            invalidate(info);
            return FINISHED_FUTURE;
        }
        else
        {
            // the request will be added to the end of the queue,
            // so that endpoint will be retried after all other endpoints for that version
            info.requestQueue.add(endpoint);
            return maybePullSchema(info);
        }
    }

    /**
     * Wait until we've received schema responses for all versions we're aware of
     *
     * @param waitMillis
     * @return true if response for all schemas were received, false if we timed out waiting
     */
    boolean awaitSchemaRequests(long waitMillis)
    {
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        if (versionInfo.isEmpty())
            logger.debug("Nothing in versionInfo - so no schemas to wait for");

        List<WaitQueue.Signal> signalList = null;
        try
        {
            synchronized (this)
            {
                signalList = new ArrayList<>(versionInfo.size());
                for (VersionInfo version : versionInfo.values())
                {
                    if (version.wasReceived())
                        continue;

                    signalList.add(version.register());
                }

                if (signalList.isEmpty())
                    return true;
            }

            long deadline = nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
            return signalList.stream().allMatch(signal -> signal.awaitUntilUninterruptibly(deadline));
        }
        finally
        {
            if (signalList != null)
                signalList.forEach(WaitQueue.Signal::cancel);
        }
    }

    Pair<Set<InetAddressAndPort>, Set<InetAddressAndPort>> pushSchemaMutations(Collection<Mutation> schemaMutations)
    {
        logger.debug("Pushing schema mutations: {}", schemaMutations);
        Set<InetAddressAndPort> schemaDestinationEndpoints = new HashSet<>();
        Set<InetAddressAndPort> schemaEndpointsIgnored = new HashSet<>();
        Message<Collection<Mutation>> message = Message.out(SCHEMA_PUSH_REQ, schemaMutations);
        for (InetAddressAndPort endpoint : gossiper.getLiveMembers())
        {
            if (shouldPushSchemaTo(endpoint))
            {
                logger.debug("Pushing schema mutations to {}: {}", endpoint, schemaMutations);
                messagingService.send(message, endpoint);
                schemaDestinationEndpoints.add(endpoint);
            }
            else
            {
                schemaEndpointsIgnored.add(endpoint);
            }
        }

        return Pair.create(schemaDestinationEndpoints, schemaEndpointsIgnored);
    }

    private boolean shouldPushSchemaTo(InetAddressAndPort endpoint)
    {
        // only push schema to nodes with known and equal versions
        return !endpoint.equals(FBUtilities.getBroadcastAddressAndPort())
               && messagingService.versions.knows(endpoint)
               && messagingService.versions.getRaw(endpoint) == MessagingService.current_version;
    }

    private Set<InetAddressAndPort> getAvailableEndpointsForVersion(UUID version)
    {
        return endpointVersions.entrySet().stream()
                               .filter(e -> e.getValue().equals(version))
                               .map(Map.Entry::getKey)
                               .collect(Collectors.toSet());
    }
}