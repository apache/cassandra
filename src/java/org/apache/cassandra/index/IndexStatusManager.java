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

package org.apache.cassandra.index;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Handles the status of an index across the ring, updating the status per index and endpoint
 * in a per-endpoint map.
 * <p>
 * Peer status changes are recieved via the {@link StorageService} {@link org.apache.cassandra.gms.IEndpointStateChangeSubscriber}.
 * <p>
 * Local status changes are propagated to the {@link Gossiper} using an async executor.
 */
public class IndexStatusManager
{
    private static final Logger logger = LoggerFactory.getLogger(IndexStatusManager.class);

    public static final IndexStatusManager instance = new IndexStatusManager();

    // executes index status propagation task asynchronously to avoid potential deadlock on SIM
    private final ExecutorPlus statusPropagationExecutor = executorFactory().withJmxInternal()
                                                                            .sequential("StatusPropagationExecutor");

    /**
     * A map of per-endpoint index statuses: the key of inner map is the identifier "keyspace.index"
     */
    public final Map<InetAddressAndPort, Map<String, Index.Status>> peerIndexStatus = new HashMap<>();

    private IndexStatusManager() {}

    /**
     * Remove endpoints whose indexes are not queryable for the specified {@link Index.QueryPlan}.
     *
     * @param liveEndpoints current live endpoints where non-queryable endpoints will be removed
     * @param keyspace to be queried
     * @param indexQueryPlan index query plan used in the read command
     * @param level consistency level of read command
     */
    public <E extends Endpoints<E>> E filterForQuery(E liveEndpoints, Keyspace keyspace, Index.QueryPlan indexQueryPlan, ConsistencyLevel level)
    {
        // UNKNOWN states are transient/rare; only a few replicas should have this state at any time. See CASSANDRA-19400
        Set<Replica> queryableNonSucceeded = new HashSet<>(4);

        E queryableEndpoints = liveEndpoints.filter(replica -> {

            boolean allBuilt = true;
            for (Index index : indexQueryPlan.getIndexes())
            {
                Index.Status status = getIndexStatus(replica.endpoint(), keyspace.getName(), index.getIndexMetadata().name);
                if (!index.isQueryable(status))
                    return false;

                if (status != Index.Status.BUILD_SUCCEEDED)
                    allBuilt = false;
            }

            if (!allBuilt)
                queryableNonSucceeded.add(replica);

            return true;
        });

        // deprioritize replicas with queryable but non-succeeded indexes
        if (!queryableNonSucceeded.isEmpty() && queryableNonSucceeded.size() != queryableEndpoints.size())
            queryableEndpoints = queryableEndpoints.sorted(Comparator.comparingInt(e -> queryableNonSucceeded.contains(e) ? 1 : -1));

        int initial = liveEndpoints.size();
        int filtered = queryableEndpoints.size();

        // Throw ReadFailureException if read request cannot satisfy Consistency Level due to non-queryable indexes.
        // It is to provide a better UX, compared to throwing UnavailableException when the nodes are actually alive.
        if (initial != filtered)
        {
            int required = level.blockFor(keyspace.getReplicationStrategy());
            if (required <= initial && required > filtered)
            {
                Map<InetAddressAndPort, RequestFailureReason> failureReasons = new HashMap<>();
                liveEndpoints.without(queryableEndpoints.endpoints())
                             .forEach(replica -> failureReasons.put(replica.endpoint(), RequestFailureReason.INDEX_NOT_AVAILABLE));

                throw new ReadFailureException(level, filtered, required, false, failureReasons);
            }
        }

        return queryableEndpoints;
    }

    /**
     * Recieve a new index status map from a peer. This will include the status for all the indexes on the peer.
     *
     * @param endpoint the {@link InetAddressAndPort} the index status map is coming from
     * @param versionedValue the {@link VersionedValue} containing the index status map
     */
    public synchronized void receivePeerIndexStatus(InetAddressAndPort endpoint, VersionedValue versionedValue)
    {
        try
        {
            if (versionedValue == null)
                return;
            if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
                return;

            Map<String, Index.Status> indexStatusMap = statusMapFromString(versionedValue);

            Map<String, Index.Status> oldStatus = peerIndexStatus.put(endpoint, indexStatusMap);
            Map<String, Index.Status> updated = updatedIndexStatuses(oldStatus, indexStatusMap);
            Set<String> removed = removedIndexStatuses(oldStatus, indexStatusMap);
            if (!updated.isEmpty() || !removed.isEmpty())
                logger.debug("Received index status for peer {}:\n    Updated: {}\n    Removed: {}",
                             endpoint, updated, removed);
        }
        catch (Exception e)
        {
            logger.error("Unable to parse index status: {}", e.getMessage());
        }
    }

    private Map<String, Index.Status> statusMapFromString(VersionedValue versionedValue)
    {
        Map<String, Object> peerStatus = JsonUtils.fromJsonMap(versionedValue.value);
        Map<String, Index.Status> indexStatusMap = new HashMap<>();

        for (Map.Entry<String, Object> endpointStatus : peerStatus.entrySet())
        {
            String keyspaceOrIndex = endpointStatus.getKey();
            Object keyspaceOrIndexStatus = endpointStatus.getValue();

            if (keyspaceOrIndexStatus instanceof String)
            {
                // This is the legacy format: (fully qualified index name -> enum string)
                Index.Status status = Index.Status.valueOf(keyspaceOrIndexStatus.toString());
                indexStatusMap.put(keyspaceOrIndex, status);
            }
            else if (keyspaceOrIndexStatus instanceof Map)
            {
                // This is the new format. (keyspace -> (index -> numeric enum code)) 
                @SuppressWarnings("unchecked") 
                Map<String, Integer> keyspaceIndexStatusMap = (Map<String, Integer>) keyspaceOrIndexStatus;

                for (Map.Entry<String, Integer> indexStatus : keyspaceIndexStatusMap.entrySet())
                {
                    Index.Status status = Index.Status.fromCode(indexStatus.getValue());
                    indexStatusMap.put(identifier(keyspaceOrIndex, indexStatus.getKey()), status);
                }
            }
            else
            {
                throw new MarshalException("Invalid index status format: " + endpointStatus);
            }
        }
        return indexStatusMap;
    }

    /**
     * Propagate a new index status to the ring. The new index status is added to the current index status map
     * and the whole map is sent to the ring as a {@link VersionedValue}.
     *
     * @param keyspace the keyspace name for the index
     * @param index the index name
     * @param status the new {@link Index.Status}
     */
    public synchronized void propagateLocalIndexStatus(String keyspace, String index, Index.Status status)
    {
        try
        {
            Map<String, Index.Status> statusMap = peerIndexStatus.computeIfAbsent(FBUtilities.getBroadcastAddressAndPort(),
                                                                               k -> new HashMap<>());
            String keyspaceIndex = identifier(keyspace, index);

            if (status == Index.Status.DROPPED)
                statusMap.remove(keyspaceIndex);
            else
                statusMap.put(keyspaceIndex, status);

            // Don't try and propagate if the gossiper isn't enabled. This is primarily for tests where the
            // Gossiper has not been started. If we attempt to propagate when not started an exception is
            // logged and this causes a number of dtests to fail.
            if (Gossiper.instance.isEnabled())
            {
                // Versions 5.0.0 through 5.0.2 use a much more bloated format that duplicates keyspace names
                // and writes full status names instead of their numeric codes. If the minimum cluster version is
                // unknown or one of those 3 versions, continue to propagate the old format.
                CassandraVersion minVersion = ClusterMetadata.current().directory.clusterMinVersion.cassandraVersion;

                String newSerializedStatusMap = shouldWriteLegacyStatusFormat(minVersion) ? JsonUtils.writeAsJsonString(statusMap) 
                                                                                          : toSerializedFormat(statusMap);

                statusPropagationExecutor.submit(() -> {
                    // schedule gossiper update asynchronously to avoid potential deadlock when another thread is holding
                    // gossiper taskLock.
                    VersionedValue value = StorageService.instance.valueFactory.indexStatus(newSerializedStatusMap);
                    Gossiper.instance.addLocalApplicationState(ApplicationState.INDEX_STATUS, value);
                });
            }
        }
        catch (Exception e)
        {
            logger.warn("Unable to propagate index status: {}", e.getMessage());
        }
    }

    private static boolean shouldWriteLegacyStatusFormat(CassandraVersion minVersion)
    {
        return minVersion == null || (minVersion.major == 5 && minVersion.minor == 0 && minVersion.patch < 3);
    }

    /**
     * Serializes as a JSON string the status of the indexes in the provided map.
     * <p> 
     * For example, the map...
     * <pre>
     * {
     *     ks1.cf1_idx1=FULL_REBUILD_STARTED,
     *     ks1.cf1_idx2=FULL_REBUILD_STARTED,
     *     system.PaxosUncommittedIndex=BUILD_SUCCEEDED
     * }
     * </pre>
     * ...will be converted to the string...
     * <pre>
     * {
     *     "system": {"PaxosUncommittedIndex": 3},
     *     "ks1": {"cf1_idx1": 1, "cf1_idx2": 1}
     * }
     * </pre>
     */
    public static String toSerializedFormat(Map<String, Index.Status> indexStatusMap)
    {
        Map<String, Map<String, Integer>> serialized = new HashMap<>();

        for (Map.Entry<String, Index.Status> e : indexStatusMap.entrySet())
        {
            String[] keyspaceAndIndex = e.getKey().split("\\.");
            serialized.computeIfAbsent(keyspaceAndIndex[0], ignore -> new HashMap<>())
                      .put(keyspaceAndIndex[1], e.getValue().code);
        }

        return JsonUtils.writeAsJsonString(serialized);
    }

    @VisibleForTesting
    public synchronized Index.Status getIndexStatus(InetAddressAndPort peer, String keyspace, String index)
    {
        return peerIndexStatus.getOrDefault(peer, Collections.emptyMap())
                              .getOrDefault(identifier(keyspace, index), Index.Status.UNKNOWN);
    }

    /**
     * Returns the names of indexes that are present in oldStatus but absent in newStatus.
     */
    private @Nonnull Set<String> removedIndexStatuses(@Nullable Map<String, Index.Status> oldStatus,
                                                      @Nonnull Map<String, Index.Status> newStatus)
    {
        if (oldStatus == null)
            return Collections.emptySet();
        Set<String> result = new HashSet<>(oldStatus.keySet());
        result.removeAll(newStatus.keySet());
        return result;
    }

    /**
     * Returns a new map containing only the entries from newStatus that differ from corresponding entries in oldStatus.
     */
    private @Nonnull Map<String, Index.Status> updatedIndexStatuses(@Nullable Map<String, Index.Status> oldStatus,
                                                                    @Nonnull Map<String, Index.Status> newStatus)
    {
        Map<String, Index.Status> delta = new HashMap<>();
        for (Map.Entry<String, Index.Status> e : newStatus.entrySet())
        {
            if (oldStatus == null || e.getValue() != oldStatus.get(e.getKey()))
                delta.put(e.getKey(), e.getValue());
        }
        return delta;
    }

    private String identifier(String keyspace, String index)
    {
        return keyspace + '.' + index;
    }

    public void shutdownAndWait(long interval, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(interval, unit, statusPropagationExecutor);
    }
}
