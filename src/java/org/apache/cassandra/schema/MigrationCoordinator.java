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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_ENDPOINTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_VERSIONS;

public class MigrationCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationCoordinator.class);
    private static final Future<Void> FINISHED_FUTURE = Futures.immediateFuture(null);

    private static LongSupplier getUptimeFn = () -> ManagementFactory.getRuntimeMXBean().getUptime();

    @VisibleForTesting
    public static void setUptimeFn(LongSupplier supplier)
    {
        getUptimeFn = supplier;
    }


    private static final int MIGRATION_DELAY_IN_MS = 60000;
    private static final int MAX_OUTSTANDING_VERSION_REQUESTS = 3;

    public static final MigrationCoordinator instance = new MigrationCoordinator();

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

        final Set<InetAddressAndPort> endpoints           = Sets.newConcurrentHashSet();
        final Set<InetAddressAndPort> outstandingRequests = Sets.newConcurrentHashSet();
        final Deque<InetAddressAndPort> requestQueue      = new ArrayDeque<>();

        private final WaitQueue waitQueue = new WaitQueue();

        volatile boolean receivedSchema;

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
    }

    private final Map<UUID, VersionInfo> versionInfo = new HashMap<>();
    private final Map<InetAddressAndPort, UUID> endpointVersions = new HashMap<>();
    private final AtomicInteger inflightTasks = new AtomicInteger();
    private final Set<InetAddressAndPort> ignoredEndpoints = getIgnoredEndpoints();

    public void start()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::pullUnreceivedSchemaVersions, 1, 1, TimeUnit.MINUTES);
    }

    public synchronized void reset()
    {
        versionInfo.clear();
    }

    synchronized List<Future<Void>> pullUnreceivedSchemaVersions()
    {
        List<Future<Void>> futures = new ArrayList<>();
        for (VersionInfo info : versionInfo.values())
        {
            if (info.wasReceived() || info.outstandingRequests.size() > 0)
                continue;

            Future<Void> future = maybePullSchema(info);
            if (future != null && future != FINISHED_FUTURE)
                futures.add(future);
        }

        return futures;
    }

    synchronized Future<Void> maybePullSchema(VersionInfo info)
    {
        if (info.endpoints.isEmpty() || info.wasReceived() || !shouldPullSchema(info.version))
            return FINISHED_FUTURE;

        if (info.outstandingRequests.size() >= getMaxOutstandingVersionRequests())
            return FINISHED_FUTURE;

        for (int i=0, isize=info.requestQueue.size(); i<isize; i++)
        {
            InetAddressAndPort endpoint = info.requestQueue.remove();
            if (!info.endpoints.contains(endpoint))
                continue;

            if (shouldPullFromEndpoint(endpoint) && info.outstandingRequests.add(endpoint))
            {
                return scheduleSchemaPull(endpoint, info);
            }
            else
            {
                // return to queue
                info.requestQueue.offer(endpoint);
            }
        }

        // no suitable endpoints were found, check again in a minute, the periodic task will pick it up
        return null;
    }

    public synchronized Map<UUID, Set<InetAddressAndPort>> outstandingVersions()
    {
        HashMap<UUID, Set<InetAddressAndPort>> map = new HashMap<>();
        for (VersionInfo info : versionInfo.values())
            if (!info.wasReceived())
                map.put(info.version, ImmutableSet.copyOf(info.endpoints));
        return map;
    }

    @VisibleForTesting
    protected VersionInfo getVersionInfoUnsafe(UUID version)
    {
        return versionInfo.get(version);
    }

    @VisibleForTesting
    protected int getMaxOutstandingVersionRequests()
    {
        return MAX_OUTSTANDING_VERSION_REQUESTS;
    }

    @VisibleForTesting
    protected boolean isAlive(InetAddressAndPort endpoint)
    {
        return FailureDetector.instance.isAlive(endpoint);
    }

    @VisibleForTesting
    protected boolean shouldPullSchema(UUID version)
    {
        if (Schema.instance.getVersion() == null)
        {
            logger.debug("Not pulling schema for version {}, because local schama version is not known yet", version);
            return false;
        }

        if (Schema.instance.isSameVersion(version))
        {
            logger.debug("Not pulling schema for version {}, because schema versions match: " +
                         "local={}, remote={}",
                         version,
                         Schema.schemaVersionToString(Schema.instance.getVersion()),
                         Schema.schemaVersionToString(version));
            return false;
        }
        return true;
    }

    // Since 3.0.14 protocol contains only a CASSANDRA-13004 bugfix, it is safe to accept schema changes
    // from both 3.0 and 3.0.14.
    private static boolean is30Compatible(int version)
    {
        return version == MessagingService.current_version || version == MessagingService.VERSION_3014;
    }

    @VisibleForTesting
    protected boolean shouldPullFromEndpoint(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return false;

        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null)
            return false;

        VersionedValue releaseVersionValue = state.getApplicationState(ApplicationState.RELEASE_VERSION);
        if (releaseVersionValue == null)
            return false;
        final String releaseVersion = releaseVersionValue.value;
        final String ourMajorVersion = FBUtilities.getReleaseVersionMajor();

        if (!releaseVersion.startsWith(ourMajorVersion))
        {
            logger.debug("Not pulling schema from {} because release version in Gossip is not major version {}, it is {}",
                         endpoint, ourMajorVersion, releaseVersion);
            return false;
        }

        if (!MessagingService.instance().versions.knows(endpoint))
        {
            logger.debug("Not pulling schema from {} because their messaging version is unknown", endpoint);
            return false;
        }

        if (MessagingService.instance().versions.getRaw(endpoint) != MessagingService.current_version)
        {
            logger.debug("Not pulling schema from {} because their schema format is incompatible", endpoint);
            return false;
        }

        if (Gossiper.instance.isGossipOnlyMember(endpoint))
        {
            logger.debug("Not pulling schema from {} because it's a gossip only member", endpoint);
            return false;
        }
        return true;
    }

    @VisibleForTesting
    protected boolean shouldPullImmediately(InetAddressAndPort endpoint, UUID version)
    {
        if (Schema.instance.isEmpty() || getUptimeFn.getAsLong() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Immediately submitting migration task for {}, " +
                         "schema versions: local={}, remote={}",
                         endpoint,
                         Schema.schemaVersionToString(Schema.instance.getVersion()),
                         Schema.schemaVersionToString(version));
            return true;
        }
        return false;
    }

    @VisibleForTesting
    protected boolean isLocalVersion(UUID version)
    {
        return Schema.instance.isSameVersion(version);
    }

    /**
     * If a previous schema update brought our version the same as the incoming schema, don't apply it
     */
    synchronized boolean shouldApplySchemaFor(VersionInfo info)
    {
        if (info.wasReceived())
            return false;
        return !isLocalVersion(info.version);
    }

    public synchronized Future<Void> reportEndpointVersion(InetAddressAndPort endpoint, UUID version)
    {
        if (ignoredEndpoints.contains(endpoint) || IGNORED_VERSIONS.contains(version))
        {
            endpointVersions.remove(endpoint);
            removeEndpointFromVersion(endpoint, null);
            return FINISHED_FUTURE;
        }

        UUID current = endpointVersions.put(endpoint, version);
        if (current != null && current.equals(version))
            return FINISHED_FUTURE;

        VersionInfo info = versionInfo.computeIfAbsent(version, VersionInfo::new);
        if (isLocalVersion(version))
            info.markReceived();
        info.endpoints.add(endpoint);
        info.requestQueue.addFirst(endpoint);

        // disassociate this endpoint from its (now) previous schema version
        removeEndpointFromVersion(endpoint, current);

        return maybePullSchema(info);
    }

    public Future<Void> reportEndpointVersion(InetAddressAndPort endpoint, EndpointState state)
    {
        if (state == null)
            return FINISHED_FUTURE;

        UUID version = state.getSchemaVersion();

        if (version == null)
            return FINISHED_FUTURE;

        return reportEndpointVersion(endpoint, version);
    }

    private synchronized void removeEndpointFromVersion(InetAddressAndPort endpoint, UUID version)
    {
        if (version == null)
            return;

        VersionInfo info = versionInfo.get(version);

        if (info == null)
            return;

        info.endpoints.remove(endpoint);
        if (info.endpoints.isEmpty())
        {
            info.waitQueue.signalAll();
            versionInfo.remove(version);
        }
    }

    public synchronized void removeAndIgnoreEndpoint(InetAddressAndPort endpoint)
    {
        Preconditions.checkArgument(endpoint != null);
        ignoredEndpoints.add(endpoint);
        Set<UUID> versions = ImmutableSet.copyOf(versionInfo.keySet());
        for (UUID version : versions)
        {
            removeEndpointFromVersion(endpoint, version);
        }
    }

    Future<Void> scheduleSchemaPull(InetAddressAndPort endpoint, VersionInfo info)
    {
        FutureTask<Void> task = new FutureTask<>(() -> pullSchema(new Callback(endpoint, info)), null);
        if (shouldPullImmediately(endpoint, info.version))
        {
            submitToMigrationIfNotShutdown(task);
        }
        else
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(()->submitToMigrationIfNotShutdown(task), MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
        return task;
    }

    private static Future<?> submitToMigrationIfNotShutdown(Runnable task)
    {
        boolean skipped = false;
        try
        {
            if (Stage.MIGRATION.executor().isShutdown() || Stage.MIGRATION.executor().isTerminated())
            {
                skipped = true;
                return null;
            }
            return Stage.MIGRATION.submit(task);
        }
        catch (RejectedExecutionException ex)
        {
            skipped = true;
            return null;
        }
        finally
        {
            if (skipped)
            {
                logger.info("Skipped scheduled pulling schema from other nodes: the MIGRATION executor service has been shutdown.");
            }
        }
    }

    @VisibleForTesting
    protected void mergeSchemaFrom(InetAddressAndPort endpoint, Collection<Mutation> mutations)
    {
        Schema.instance.mergeAndAnnounceVersion(mutations);
    }

    class Callback implements RequestCallback<Collection<Mutation>>
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
                        mergeSchemaFrom(endpoint, mutations);
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

    private void pullSchema(Callback callback)
    {
        if (!isAlive(callback.endpoint))
        {
            logger.warn("Can't send schema pull request: node {} is down.", callback.endpoint);
            callback.fail();
            return;
        }

        // There is a chance that quite some time could have passed between now and the MM#maybeScheduleSchemaPull(),
        // potentially enough for the endpoint node to restart - which is an issue if it does restart upgraded, with
        // a higher major.
        if (!shouldPullFromEndpoint(callback.endpoint))
        {
            logger.info("Skipped sending a migration request: node {} has a higher major version now.", callback.endpoint);
            callback.fail();
            return;
        }

        logger.debug("Requesting schema from {}", callback.endpoint);
        sendMigrationMessage(callback);
    }

    protected void sendMigrationMessage(Callback callback)
    {
        inflightTasks.getAndIncrement();
        Message message = Message.out(Verb.SCHEMA_PULL_REQ, NoPayload.noPayload);
        logger.info("Sending schema pull request to {}", callback.endpoint);
        MessagingService.instance().sendWithCallback(message, callback.endpoint, callback);
    }

    private synchronized Future<Void> pullComplete(InetAddressAndPort endpoint, VersionInfo info, boolean wasSuccessful)
    {
        inflightTasks.decrementAndGet();
        if (wasSuccessful)
            info.markReceived();

        info.outstandingRequests.remove(endpoint);
        info.requestQueue.add(endpoint);
        return maybePullSchema(info);
    }

    public int getInflightTasks()
    {
        return inflightTasks.get();
    }

    /**
     * Wait until we've received schema responses for all versions we're aware of
     * @param waitMillis
     * @return true if response for all schemas were received, false if we timed out waiting
     */
    public boolean awaitSchemaRequests(long waitMillis)
    {
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        if (versionInfo.isEmpty())
        {
            logger.debug("Nothing in versionInfo - so no schemas to wait for");
        }

        WaitQueue.Signal signal = null;
        try
        {
            synchronized (this)
            {
                List<WaitQueue.Signal> signalList = new ArrayList<>(versionInfo.size());
                for (VersionInfo version : versionInfo.values())
                {
                    if (version.wasReceived())
                        continue;

                    signalList.add(version.register());
                }

                if (signalList.isEmpty())
                    return true;

                WaitQueue.Signal[] signals = new WaitQueue.Signal[signalList.size()];
                signalList.toArray(signals);
                signal = WaitQueue.all(signals);
            }

            return signal.awaitUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis));
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (signal != null)
                signal.cancel();
        }
    }
}