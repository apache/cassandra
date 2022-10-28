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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
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
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.WaitQueue;

/**
 * Migration coordinator is responsible for tracking schema versions on various nodes and, if needed, synchronize the
 * schema. It performs periodic checks and if there is a schema version mismatch between the current node and the other
 * node, it pulls the schema and applies the changes locally through the callback.
 *
 * In particular the Migration Coordinator keeps track of all schema versions reported from each node in the cluster.
 * As long as a certain version is advertised by some node, it is being tracked. As long as a version is tracked,
 * the migration coordinator tries to fetch it by its periodic job.
 */
public class MigrationCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationCoordinator.class);
    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    private static final Future<Void> FINISHED_FUTURE = Futures.immediateFuture(null);

    private static final int MIGRATION_DELAY_IN_MS = Integer.parseInt(System.getProperty("cassandra.migration_delay_ms", "60000"));
    private static final int MAX_OUTSTANDING_VERSION_REQUESTS = 3;

    public static final MigrationCoordinator instance = new MigrationCoordinator();

    public static final String IGNORED_VERSIONS_PROP = "cassandra.skip_schema_check_for_versions";
    public static final String IGNORED_ENDPOINTS_PROP = "cassandra.skip_schema_check_for_endpoints";

    /**
     * Minimum delay after a failed pull request before it is reattempted. It prevents reattempting failed requests
     * immediately as it is high chance they will fail anyway. It is better to wait a bit instead of flooding logs
     * and wasting resources.
     */
    private static final long BACKOFF_DELAY_MS = Long.parseLong(System.getProperty("cassandra.schema_pull_backoff_delay_ms", "3600"));

    /** Defines how often schema definitions are pulled from the other nodes */
    private static final long SCHEMA_PULL_INTERVAL_MS = Long.parseLong(System.getProperty("cassandra.schema_pull_interval_ms", "60000"));

    /**
     * Holds the timestamps in ms for last pull request attempts.
     */
    private final WeakHashMap<InetAddress, Long> lastPullAttemptTimestamps = new WeakHashMap<>();

    private static ImmutableSet<UUID> getIgnoredVersions()
    {
        String s = System.getProperty(IGNORED_VERSIONS_PROP);
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

    private static Set<InetAddress> getIgnoredEndpoints()
    {
        Set<InetAddress> endpoints = new HashSet<>();

        String s = System.getProperty(IGNORED_ENDPOINTS_PROP);
        if (s == null || s.isEmpty())
            return endpoints;

        for (String endpoint : s.split(","))
        {
            try
            {
                endpoints.add(InetAddress.getByName(endpoint));
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

        /**
         * The set of endpoints containing this schema version
         */
        final Set<InetAddress> endpoints           = Sets.newConcurrentHashSet();

        /**
         * The set of endpoints from which we are already fetching the schema
         */
        final Set<InetAddress> outstandingRequests = Sets.newConcurrentHashSet();

        /**
         * The queue of endpoints from which we are going to fetch the schema
         */
        final Deque<InetAddress> requestQueue      = new ArrayDeque<>();

        /**
         * Threads waiting for schema synchronization are waiting until this object is signalled
         */
        private final WaitQueue waitQueue = new WaitQueue();

        /**
         * Whether this schema version have been received
         */
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

        @Override
        public String toString()
        {
            return "VersionInfo{" +
                   "version=" + version +
                   ", outstandingRequests=" + outstandingRequests +
                   ", requestQueue=" + requestQueue +
                   ", waitQueue.waiting=" + waitQueue.getWaiting() +
                   ", receivedSchema=" + receivedSchema +
                   '}';
        }
    }

    private final Map<UUID, VersionInfo> versionInfo = new HashMap<>();
    private final Map<InetAddress, UUID> endpointVersions = new HashMap<>();

    private final Set<InetAddress> ignoredEndpoints = getIgnoredEndpoints();

    public void start()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::pullUnreceivedSchemaVersions, SCHEMA_PULL_INTERVAL_MS, SCHEMA_PULL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Resets the migration coordinator by notifying all waiting threads and removing all the existing version info.
     */
    public synchronized void reset()
    {
        logger.info("Resetting migration coordinator...");

        // clear all the managed information
        this.endpointVersions.clear();
        clearVersionsInfo();
    }

    synchronized List<Future<Void>> pullUnreceivedSchemaVersions()
    {
        List<Future<Void>> futures = new ArrayList<>();
        for (VersionInfo info : versionInfo.values())
        {
            if (info.wasReceived() || info.outstandingRequests.size() > 0)
            {
                logger.trace("Skipping pull of schema {} because it has been already recevied, or it is being received ({})", info.version, info);
                continue;
            }

            Future<Void> future = maybePullSchema(info);
            if (future != null && future != FINISHED_FUTURE)
                futures.add(future);
        }

        return futures;
    }

    synchronized Future<Void> maybePullSchema(VersionInfo info)
    {
        if (info.endpoints.isEmpty() || info.wasReceived() || !shouldPullSchema(info.version))
        {
            logger.trace("Not pulling schema {} because it was received, there is no endpoint to provide it, or we should not pull it ({})", info.version, info);
            return FINISHED_FUTURE;
        }

        if (info.outstandingRequests.size() >= getMaxOutstandingVersionRequests()) {
            logger.trace("Not pulling schema {} because the number of outstanding requests has been exceeded ({} >= {})", info.version, info.outstandingRequests.size(), getMaxOutstandingVersionRequests());
            return FINISHED_FUTURE;
        }

        for (int i=0, isize=info.requestQueue.size(); i<isize; i++)
        {
            InetAddress endpoint = info.requestQueue.remove();
            if (!info.endpoints.contains(endpoint))
            {
                logger.trace("Skipping request of schema {} from {} because the endpoint does not have that schema any longer", info.version, endpoint);
                continue;
            }

            if (shouldPullFromEndpoint(endpoint) && info.outstandingRequests.add(endpoint))
            {
                return scheduleSchemaPull(endpoint, info);
            }
            else
            {
                // return to queue
                logger.trace("Could not pull schema {} from {} - the request will be added back to the queue", info.version, endpoint);
                info.requestQueue.offer(endpoint);
            }
        }

        // no suitable endpoints were found, check again in a minute, the periodic task will pick it up
        return null;
    }

    public synchronized Map<UUID, Set<InetAddress>> outstandingVersions()
    {
        HashMap<UUID, Set<InetAddress>> map = new HashMap<>();
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
    protected boolean isAlive(InetAddress endpoint)
    {
        return FailureDetector.instance.isAlive(endpoint);
    }

    @VisibleForTesting
    protected boolean shouldPullSchema(UUID version)
    {
        if (Schema.instance.getVersion() == null)
        {
            logger.debug("Not pulling schema {} because the local schama version is not known yet", version);
            return false;
        }

        if (Schema.instance.getVersion().equals(version))
        {
            logger.debug("Not pulling schema {} because it is the same as the local schema", version);
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
    protected boolean shouldPullFromEndpoint(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            logger.trace("Not pulling schema from local endpoint");
            return false;
        }

        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null)
        {
            logger.trace("Not pulling schema from endpoint {} because its state is unknown", endpoint);
            return false;
        }

        final String releaseVersion = state.getApplicationState(ApplicationState.RELEASE_VERSION).value;
        final String ourMajorVersion = FBUtilities.getReleaseVersionMajor();

        if (!releaseVersion.startsWith(ourMajorVersion))
        {
            logger.debug("Not pulling schema from {} because release version in Gossip is not major version {}, it is {}",
                         endpoint, ourMajorVersion, releaseVersion);
            return false;
        }

        if (!MessagingService.instance().knowsVersion(endpoint))
        {
            logger.debug("Not pulling schema from {} because their messaging version is unknown", endpoint);
            return false;
        }

        if (!is30Compatible(MessagingService.instance().getRawVersion(endpoint)))
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
    protected boolean shouldPullImmediately(InetAddress endpoint, UUID version)
    {
        if (Schema.instance.isEmpty() || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Immediately submitting migration task for {}, " +
                         "schema versions: local={}, remote={}",
                         endpoint,
                         Schema.instance.getVersion(),
                         version);
            return true;
        }
        return false;
    }

    @VisibleForTesting
    protected boolean isLocalVersion(UUID version)
    {
        return Schema.instance.getVersion().equals(version);
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

    synchronized Future<Void> reportEndpointVersion(InetAddress endpoint, UUID version)
    {
        logger.debug("Reported schema {} at endpoint {}", version, endpoint);
        if (ignoredEndpoints.contains(endpoint) || IGNORED_VERSIONS.contains(version))
        {
            endpointVersions.remove(endpoint);
            removeEndpointFromVersion(endpoint, null);
            logger.debug("Discarding endpoint {} or schema {} because either endpoint or schema version were marked as ignored", endpoint, version);
            return FINISHED_FUTURE;
        }

        UUID current = endpointVersions.put(endpoint, version);
        if (current != null && current.equals(version))
        {
            logger.trace("Skipping report of schema {} from {} because we already know that", version, endpoint);
            return FINISHED_FUTURE;
        }

        VersionInfo info = versionInfo.computeIfAbsent(version, VersionInfo::new);
        if (isLocalVersion(version))
        {
            info.markReceived();
            logger.trace("Schema {} from {} has been marked as recevied because it is equal the local schema", version, endpoint);
        }
        else
        {
            info.requestQueue.addFirst(endpoint);
        }
        info.endpoints.add(endpoint);
        logger.trace("Added endpoint {} to schema {}: {}", endpoint, info.version, info);

        // disassociate this endpoint from its (now) previous schema version
        removeEndpointFromVersion(endpoint, current);

        return maybePullSchema(info);
    }

    Future<Void> reportEndpointVersion(InetAddress endpoint, EndpointState state)
    {
        if (state == null)
            return FINISHED_FUTURE;

        VersionedValue version = state.getApplicationState(ApplicationState.SCHEMA);

        if (version == null)
            return FINISHED_FUTURE;

        return reportEndpointVersion(endpoint, UUID.fromString(version.value));
    }

    private synchronized void removeEndpointFromVersion(InetAddress endpoint, UUID version)
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
            logger.trace("Removed schema info: {}", info);
        }
    }

    /**
     * Remove all version info and signal all the waiting entities.
     */
    private synchronized void clearVersionsInfo()
    {
        Iterator<Map.Entry<UUID, VersionInfo>> it = versionInfo.entrySet().iterator();
        while (it.hasNext())
        {
            Map.Entry<UUID, VersionInfo> entry = it.next();
            it.remove();
            entry.getValue().waitQueue.signalAll();
        }
    }

    public synchronized void removeAndIgnoreEndpoint(InetAddress endpoint)
    {
        logger.debug("Removing and ignoring endpoint {}", endpoint);
        Preconditions.checkArgument(endpoint != null);
        // TODO The endpoint address is now ignored but when a node with the same address is added again later,
        //  there will be no way to include it in schema synchronization other than restarting each other node
        //  see https://issues.apache.org/jira/browse/CASSANDRA-17883 for details
        ignoredEndpoints.add(endpoint);
        Set<UUID> versions = ImmutableSet.copyOf(versionInfo.keySet());
        for (UUID version : versions)
        {
            removeEndpointFromVersion(endpoint, version);
        }
    } 

    Future<Void> scheduleSchemaPull(InetAddress endpoint, VersionInfo info)
    {
        FutureTask<Void> task = new FutureTask<>(() -> pullSchema(new Callback(endpoint, info)), null);
        if (shouldPullImmediately(endpoint, info.version))
        {
            long nextAttempt = lastPullAttemptTimestamps.getOrDefault(endpoint, 0L) + BACKOFF_DELAY_MS;
            long now = System.currentTimeMillis();
            if (nextAttempt <= now)
            {
                logger.debug("Pulling {} immediately from {}", info, endpoint);
                submitToMigrationIfNotShutdown(task);
            }
            else
            {
                long delay = nextAttempt - now;
                logger.debug("Previous pull of {} from {} failed. Postponing next attempt for {}ms", info, endpoint, delay);
                ScheduledExecutors.nonPeriodicTasks.schedule(() -> submitToMigrationIfNotShutdown(task), delay, TimeUnit.MILLISECONDS);
            }
        }
        else
        {
            logger.debug("Postponing pull of {} from {} for {}ms", info, endpoint, MIGRATION_DELAY_IN_MS);
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> submitToMigrationIfNotShutdown(task), MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
        return task;
    }

    private static Future<?> submitToMigrationIfNotShutdown(Runnable task)
    {
        LocalAwareExecutorService stage = StageManager.getStage(Stage.MIGRATION);

        if (stage.isShutdown() || stage.isTerminated())
        {
            logger.info("Skipped scheduled pulling schema from other nodes: the MIGRATION executor service has been shutdown.");
            return null;
        }
        else
            return stage.submit(task);
    }

    @VisibleForTesting
    protected void mergeSchemaFrom(InetAddress endpoint, Collection<Mutation> mutations)
    {
        SchemaKeyspace.mergeSchemaAndAnnounceVersion(mutations);
    }

    class Callback implements IAsyncCallbackWithFailure<Collection<Mutation>>
    {
        final InetAddress endpoint;
        final VersionInfo info;

        public Callback(InetAddress endpoint, VersionInfo info)
        {
            this.endpoint = endpoint;
            this.info = info;
        }

        public void onFailure(InetAddress from)
        {
            fail();
        }

        Future<Void> fail()
        {
            return pullComplete(endpoint, info, false);
        }

        public void response(MessageIn<Collection<Mutation>> message)
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
        lastPullAttemptTimestamps.put(callback.endpoint, System.currentTimeMillis());

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
        MessageOut<?> message = new MessageOut<>(MessagingService.Verb.MIGRATION_REQUEST, null, MigrationManager.MigrationsSerializer.instance);
        logger.info("Sending schema pull request to {} at {} with timeout {}", callback.endpoint, System.currentTimeMillis(), message.getTimeout());
        MessagingService.instance().sendRR(message, callback.endpoint, callback, message.getTimeout(), true);
    }

    private synchronized Future<Void> pullComplete(InetAddress endpoint, VersionInfo info, boolean wasSuccessful)
    {
        if (wasSuccessful)
        {
            info.markReceived();
            lastPullAttemptTimestamps.remove(endpoint);
        }

        info.outstandingRequests.remove(endpoint);
        info.requestQueue.add(endpoint);
        return maybePullSchema(info);
    }

    /**
     * Wait until we've received schema responses for all versions we're aware of
     * @param waitMillis
     * @return true if response for all schemas were received, false if we timed out waiting
     */
    public boolean awaitSchemaRequests(long waitMillis)
    {
        if (!FBUtilities.getBroadcastAddress().equals(InetAddress.getLoopbackAddress()))
            CassandraDaemon.waitForGossipToSettle();

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
