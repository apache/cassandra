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
package org.apache.cassandra.locator;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SortedBiMultiValMap;

import static org.apache.cassandra.config.CassandraRelevantProperties.LINE_SEPARATOR;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class TokenMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(TokenMetadata.class);

    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    private final BiMultiValMap<Token, InetAddressAndPort> tokenToEndpointMap;

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final BiMap<InetAddressAndPort, UUID> endpointToHostIdMap;

    // Prior to CASSANDRA-603, we just had <tt>Map<Range, InetAddressAndPort> pendingRanges<tt>,
    // which was added to when a node began bootstrap and removed from when it finished.
    //
    // This is inadequate when multiple changes are allowed simultaneously.  For example,
    // suppose that there is a ring of nodes A, C and E, with replication factor 3.
    // Node D bootstraps between C and E, so its pending ranges will be E-A, A-C and C-D.
    // Now suppose node B bootstraps between A and C at the same time. Its pending ranges
    // would be C-E, E-A and A-B. Now both nodes need to be assigned pending range E-A,
    // which we would be unable to represent with the old Map.  The same thing happens
    // even more obviously for any nodes that boot simultaneously between same two nodes.
    //
    // So, we made two changes:
    //
    // First, we changed pendingRanges to a <tt>Multimap<Range, InetAddressAndPort></tt> (now
    // <tt>Map<String, Multimap<Range, InetAddressAndPort>></tt>, because replication strategy
    // and options are per-KeySpace).
    //
    // Second, we added the bootstrapTokens and leavingEndpoints collections, so we can
    // rebuild pendingRanges from the complete information of what is going on, when
    // additional changes are made mid-operation.
    //
    // Finally, note that recording the tokens of joining nodes in bootstrapTokens also
    // means we can detect and reject the addition of multiple nodes at the same token
    // before one becomes part of the ring.
    private final BiMultiValMap<Token, InetAddressAndPort> bootstrapTokens = new BiMultiValMap<>();

    private final BiMap<InetAddressAndPort, InetAddressAndPort> replacementToOriginal = HashBiMap.create();

    // (don't need to record Token here since it's still part of tokenToEndpointMap until it's done leaving)
    private final Set<InetAddressAndPort> leavingEndpoints = new HashSet<>();
    // this is a cache of the calculation from {tokenToEndpointMap, bootstrapTokens, leavingEndpoints}
    // NOTE: this may contain ranges that conflict with the those implied by sortedTokens when a range is changing its transient status
    private final ConcurrentMap<String, PendingRangeMaps> pendingRanges = new ConcurrentHashMap<String, PendingRangeMaps>();

    // nodes which are migrating to the new tokens in the ring
    private final Set<Pair<Token, InetAddressAndPort>> movingEndpoints = new HashSet<>();

    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private volatile ArrayList<Token> sortedTokens; // safe to be read without a lock, as it's never mutated

    private volatile Topology topology;

    public final IPartitioner partitioner;

    // signals replication strategies that nodes have joined or left the ring and they need to recompute ownership
    @GuardedBy("lock")
    private long ringVersion = 0;

    public TokenMetadata()
    {
        this(SortedBiMultiValMap.create(),
             HashBiMap.create(),
             Topology.empty(),
             DatabaseDescriptor.getPartitioner());
    }

    public TokenMetadata(IEndpointSnitch snitch)
    {
        this(SortedBiMultiValMap.create(),
             HashBiMap.create(),
             Topology.builder(() -> snitch).build(),
             DatabaseDescriptor.getPartitioner());
    }

    private TokenMetadata(BiMultiValMap<Token, InetAddressAndPort> tokenToEndpointMap, BiMap<InetAddressAndPort, UUID> endpointsMap, Topology topology, IPartitioner partitioner)
    {
        this(tokenToEndpointMap, endpointsMap, topology, partitioner, 0);
    }

    private TokenMetadata(BiMultiValMap<Token, InetAddressAndPort> tokenToEndpointMap, BiMap<InetAddressAndPort, UUID> endpointsMap, Topology topology, IPartitioner partitioner, long ringVersion)
    {
        this.tokenToEndpointMap = tokenToEndpointMap;
        this.topology = topology;
        this.partitioner = partitioner;
        endpointToHostIdMap = endpointsMap;
        sortedTokens = sortTokens();
        this.ringVersion = ringVersion;
    }

    /**
     * To be used by tests only (via {@link org.apache.cassandra.service.StorageService#setPartitionerUnsafe}).
     */
    @VisibleForTesting
    public TokenMetadata cloneWithNewPartitioner(IPartitioner newPartitioner)
    {
        return new TokenMetadata(tokenToEndpointMap, endpointToHostIdMap, topology, newPartitioner);
    }

    private ArrayList<Token> sortTokens()
    {
        return new ArrayList<>(tokenToEndpointMap.keySet());
    }

    /** @return the number of nodes bootstrapping into source's primary range */
    public int pendingRangeChanges(InetAddressAndPort source)
    {
        int n = 0;
        Collection<Range<Token>> sourceRanges = getPrimaryRangesFor(getTokens(source));
        lock.readLock().lock();
        try
        {
            for (Token token : bootstrapTokens.keySet())
                for (Range<Token> range : sourceRanges)
                    if (range.contains(token))
                        n++;
        }
        finally
        {
            lock.readLock().unlock();
        }
        return n;
    }

    /**
     * Update token map with a single token/endpoint pair in normal state.
     */
    public void updateNormalToken(Token token, InetAddressAndPort endpoint)
    {
        updateNormalTokens(Collections.singleton(token), endpoint);
    }

    public void updateNormalTokens(Collection<Token> tokens, InetAddressAndPort endpoint)
    {
        Multimap<InetAddressAndPort, Token> endpointTokens = HashMultimap.create();
        endpointTokens.putAll(endpoint, tokens);
        updateNormalTokens(endpointTokens);
    }

    /**
     * Update token map with a set of token/endpoint pairs in normal state.
     *
     * Prefer this whenever there are multiple pairs to update, as each update (whether a single or multiple)
     * is expensive (CASSANDRA-3831).
     */
    public void updateNormalTokens(Multimap<InetAddressAndPort, Token> endpointTokens)
    {
        if (endpointTokens.isEmpty())
            return;

        lock.writeLock().lock();
        try
        {
            boolean shouldSortTokens = false;
            Topology.Builder topologyBuilder = topology.unbuild();
            for (InetAddressAndPort endpoint : endpointTokens.keySet())
            {
                Collection<Token> tokens = endpointTokens.get(endpoint);

                assert tokens != null && !tokens.isEmpty();

                bootstrapTokens.removeValue(endpoint);
                tokenToEndpointMap.removeValue(endpoint);
                topologyBuilder.addEndpoint(endpoint);
                leavingEndpoints.remove(endpoint);
                replacementToOriginal.remove(endpoint);
                removeFromMoving(endpoint); // also removing this endpoint from moving

                for (Token token : tokens)
                {
                    InetAddressAndPort prev = tokenToEndpointMap.put(token, endpoint);
                    if (!endpoint.equals(prev))
                    {
                        if (prev != null)
                            logger.warn("Token {} changing ownership from {} to {}", token, prev, endpoint);
                        shouldSortTokens = true;
                    }
                }
            }
            topology = topologyBuilder.build();

            if (shouldSortTokens)
                sortedTokens = sortTokens();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     */
    public void updateHostId(UUID hostId, InetAddressAndPort endpoint)
    {
        assert hostId != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            updateEndpointToHostIdMap(hostId, endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }

    }

    public void updateHostIds(Map<UUID, InetAddressAndPort> hostIdToEndpointMap)
    {
        lock.writeLock().lock();
        try
        {
            for (Map.Entry<UUID, InetAddressAndPort> entry : hostIdToEndpointMap.entrySet())
            {
                updateEndpointToHostIdMap(entry.getKey(), entry.getValue());
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }

    }
    
    private void updateEndpointToHostIdMap(UUID hostId, InetAddressAndPort endpoint)
    {
        InetAddressAndPort storedEp = endpointToHostIdMap.inverse().get(hostId);
        if (storedEp != null)
        {
            if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp)))
            {
                throw new RuntimeException(String.format("Host ID collision between active endpoint %s and %s (id=%s)",
                                                         storedEp,
                                                         endpoint,
                                                         hostId));
            }
        }

        UUID storedId = endpointToHostIdMap.get(endpoint);
        if ((storedId != null) && (!storedId.equals(hostId)))
            logger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, hostId);

        endpointToHostIdMap.forcePut(endpoint, hostId);
    }

    /** Return the unique host ID for an end-point. */
    public UUID getHostId(InetAddressAndPort endpoint)
    {
        lock.readLock().lock();
        try
        {
            return endpointToHostIdMap.get(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** Return the end-point for a unique host ID */
    public InetAddressAndPort getEndpointForHostId(UUID hostId)
    {
        lock.readLock().lock();
        try
        {
            return endpointToHostIdMap.inverse().get(hostId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** @return a copy of the endpoint-to-id map for read-only operations */
    public Map<InetAddressAndPort, UUID> getEndpointToHostIdMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Map<InetAddressAndPort, UUID> readMap = new HashMap<>();
            readMap.putAll(endpointToHostIdMap);
            return readMap;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** @deprecated See CASSANDRA-4121 */
    @Deprecated(since = "1.2.0")
    public void addBootstrapToken(Token token, InetAddressAndPort endpoint)
    {
        addBootstrapTokens(Collections.singleton(token), endpoint);
    }

    public void addBootstrapTokens(Collection<Token> tokens, InetAddressAndPort endpoint)
    {
        addBootstrapTokens(tokens, endpoint, null);
    }

    private void addBootstrapTokens(Collection<Token> tokens, InetAddressAndPort endpoint, InetAddressAndPort original)
    {
        assert tokens != null && !tokens.isEmpty();
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {

            InetAddressAndPort oldEndpoint;

            for (Token token : tokens)
            {
                oldEndpoint = bootstrapTokens.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);

                oldEndpoint = tokenToEndpointMap.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint) && !oldEndpoint.equals(original))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);
            }

            bootstrapTokens.removeValue(endpoint);

            for (Token token : tokens)
                bootstrapTokens.put(token, endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addReplaceTokens(Collection<Token> replacingTokens, InetAddressAndPort newNode, InetAddressAndPort oldNode)
    {
        assert replacingTokens != null && !replacingTokens.isEmpty();
        assert newNode != null && oldNode != null;

        lock.writeLock().lock();
        try
        {
            Collection<Token> oldNodeTokens = tokenToEndpointMap.inverse().get(oldNode);
            if (!replacingTokens.containsAll(oldNodeTokens) || !oldNodeTokens.containsAll(replacingTokens))
            {
                throw new RuntimeException(String.format("Node %s is trying to replace node %s with tokens %s with a " +
                                                         "different set of tokens %s.", newNode, oldNode, oldNodeTokens,
                                                         replacingTokens));
            }

            logger.debug("Replacing {} with {}", newNode, oldNode);
            replacementToOriginal.put(newNode, oldNode);

            addBootstrapTokens(replacingTokens, newNode, oldNode);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Optional<InetAddressAndPort> getReplacementNode(InetAddressAndPort endpoint)
    {
        lock.readLock().lock();
        try
        {
            return Optional.ofNullable(replacementToOriginal.inverse().get(endpoint));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Optional<InetAddressAndPort> getReplacingNode(InetAddressAndPort endpoint)
    {
        lock.readLock().lock();
        try
        {
            return Optional.ofNullable((replacementToOriginal.get(endpoint)));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public void removeBootstrapTokens(Collection<Token> tokens)
    {
        assert tokens != null && !tokens.isEmpty();

        lock.writeLock().lock();
        try
        {
            for (Token token : tokens)
                bootstrapTokens.remove(token);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addLeavingEndpoint(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            leavingEndpoints.add(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a new moving endpoint
     * @param token token which is node moving to
     * @param endpoint address of the moving node
     */
    public void addMovingEndpoint(Token token, InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            movingEndpoints.add(Pair.create(token, endpoint));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeEndpoint(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            bootstrapTokens.removeValue(endpoint);

            topology = topology.unbuild().removeEndpoint(endpoint).build();
            leavingEndpoints.remove(endpoint);
            if (replacementToOriginal.remove(endpoint) != null)
            {
                logger.debug("Node {} failed during replace.", endpoint);
            }
            endpointToHostIdMap.remove(endpoint);
            Collection<Token> removedTokens = tokenToEndpointMap.removeValue(endpoint);
            if (removedTokens != null && !removedTokens.isEmpty())
            {
                sortedTokens = sortTokens();
                invalidateCachedRingsUnsafe();
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * This is called when the snitch properties for this endpoint are updated, see CASSANDRA-10238.
     */
    public Topology updateTopology(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            logger.info("Updating topology for {}", endpoint);
            topology = topology.unbuild().updateEndpoint(endpoint).build();
            invalidateCachedRingsUnsafe();
            return topology;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * This is called when the snitch properties for many endpoints are updated, it will update
     * the topology mappings of any endpoints whose snitch has changed, see CASSANDRA-10238.
     */
    public Topology updateTopology()
    {
        lock.writeLock().lock();
        try
        {
            logger.info("Updating topology for all endpoints that have changed");
            topology = topology.unbuild().updateEndpoints().build();
            invalidateCachedRingsUnsafe();
            return topology;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove pair of token/address from moving endpoints
     * @param endpoint address of the moving node
     */
    public void removeFromMoving(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            for (Pair<Token, InetAddressAndPort> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                {
                    movingEndpoints.remove(pair);
                    break;
                }
            }

            invalidateCachedRingsUnsafe();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Collection<Token> getTokens(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            assert isMember(endpoint): String.format("Unable to get tokens for %s; it is not a member", endpoint); // don't want to return nulls
            return new ArrayList<>(tokenToEndpointMap.inverse().get(endpoint));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** @deprecated See CASSANDRA-4121 */
    @Deprecated(since = "1.2.0")
    public Token getToken(InetAddressAndPort endpoint)
    {
        return getTokens(endpoint).iterator().next();
    }

    public boolean isMember(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.inverse().containsKey(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isLeaving(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return leavingEndpoints.contains(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isMoving(InetAddressAndPort endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            for (Pair<Token, InetAddressAndPort> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                    return true;
            }

            return false;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private final AtomicReference<TokenMetadata> cachedTokenMap = new AtomicReference<>();

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    public TokenMetadata cloneOnlyTokenMap()
    {
        lock.readLock().lock();
        try
        {
            return new TokenMetadata(SortedBiMultiValMap.create(tokenToEndpointMap),
                                     HashBiMap.create(endpointToHostIdMap),
                                     topology,
                                     partitioner,
                                     ringVersion);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a cached TokenMetadata with only tokenToEndpointMap, i.e., the same as cloneOnlyTokenMap but
     * uses a cached copy that is invalided when the ring changes, so in the common case
     * no extra locking is required.
     *
     * Callers must *NOT* mutate the returned metadata object.
     */
    public TokenMetadata cachedOnlyTokenMap()
    {
        TokenMetadata tm = cachedTokenMap.get();
        if (tm != null)
            return tm;

        // synchronize to prevent thundering herd (CASSANDRA-6345)
        synchronized (this)
        {
            if ((tm = cachedTokenMap.get()) != null)
                return tm;

            tm = cloneOnlyTokenMap();
            cachedTokenMap.set(tm);
            return tm;
        }
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllLeft()
    {
        lock.readLock().lock();
        try
        {
            return removeEndpoints(cloneOnlyTokenMap(), leavingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private static TokenMetadata removeEndpoints(TokenMetadata allLeftMetadata, Set<InetAddressAndPort> leavingEndpoints)
    {
        for (InetAddressAndPort endpoint : leavingEndpoints)
            allLeftMetadata.removeEndpoint(endpoint);

        return allLeftMetadata;
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave, and move operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllSettled()
    {
        lock.readLock().lock();
        try
        {
            TokenMetadata metadata = cloneOnlyTokenMap();

            for (InetAddressAndPort endpoint : leavingEndpoints)
                metadata.removeEndpoint(endpoint);


            for (Pair<Token, InetAddressAndPort> pair : movingEndpoints)
                metadata.updateNormalToken(pair.left, pair.right);

            return metadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public InetAddressAndPort getEndpoint(Token token)
    {
        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.get(token);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens)
    {
        Collection<Range<Token>> ranges = new ArrayList<>(tokens.size());
        for (Token right : tokens)
            ranges.add(new Range<>(getPredecessor(right), right));
        return ranges;
    }

    /** @deprecated See CASSANDRA-4121 */
    @Deprecated(since = "1.2.0")
    public Range<Token> getPrimaryRangeFor(Token right)
    {
        return getPrimaryRangesFor(Arrays.asList(right)).iterator().next();
    }

    public ArrayList<Token> sortedTokens()
    {
        return sortedTokens;
    }

    public EndpointsByRange getPendingRangesMM(String keyspaceName)
    {
        EndpointsByRange.Builder byRange = new EndpointsByRange.Builder();
        PendingRangeMaps pendingRangeMaps = this.pendingRanges.get(keyspaceName);

        if (pendingRangeMaps != null)
        {
            for (Map.Entry<Range<Token>, EndpointsForRange.Builder> entry : pendingRangeMaps)
            {
                byRange.putAll(entry.getKey(), entry.getValue(), Conflict.ALL);
            }
        }

        return byRange.build();
    }

    /** a mutable map may be returned but caller should not modify it */
    public PendingRangeMaps getPendingRanges(String keyspaceName)
    {
        return this.pendingRanges.get(keyspaceName);
    }

    public RangesAtEndpoint getPendingRanges(String keyspaceName, InetAddressAndPort endpoint)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(endpoint);
        for (Map.Entry<Range<Token>, Replica> entry : getPendingRangesMM(keyspaceName).flattenEntries())
        {
            Replica replica = entry.getValue();
            if (replica.endpoint().equals(endpoint))
            {
                builder.add(replica, Conflict.DUPLICATE);
            }
        }
        return builder.build();
    }

     /**
     * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */
    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        // avoid race between both branches - do not use a lock here as this will block any other unrelated operations!
        long startedAt = currentTimeMillis();
        synchronized (pendingRanges)
        {
            TokenMetadataDiagnostics.pendingRangeCalculationStarted(this, keyspaceName);

            unsafeCalculatePendingRanges(strategy, keyspaceName);

            if (logger.isDebugEnabled())
                logger.debug("Starting pending range calculation for {}", keyspaceName);

            long took = currentTimeMillis() - startedAt;

            if (logger.isDebugEnabled())
                logger.debug("Pending range calculation for {} completed (took: {}ms)", keyspaceName, took);
            if (logger.isTraceEnabled())
                logger.trace("Calculated pending ranges for {}:\n{}", keyspaceName, (pendingRanges.isEmpty() ? "<empty>" : printPendingRanges()));
        }
    }

    public void unsafeCalculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        // create clone of current state
        BiMultiValMap<Token, InetAddressAndPort> bootstrapTokensClone;
        Set<InetAddressAndPort> leavingEndpointsClone;
        Set<Pair<Token, InetAddressAndPort>> movingEndpointsClone;
        TokenMetadata metadata;

        lock.readLock().lock();
        try
        {

            if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && movingEndpoints.isEmpty())
            {
                if (logger.isTraceEnabled())
                    logger.trace("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", keyspaceName);
                if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && movingEndpoints.isEmpty())
                {
                    if (logger.isTraceEnabled())
                        logger.trace("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", keyspaceName);
                    pendingRanges.put(keyspaceName, new PendingRangeMaps());

                    return;
                }
            }

            bootstrapTokensClone  = new BiMultiValMap<>(this.bootstrapTokens);
            leavingEndpointsClone = new HashSet<>(this.leavingEndpoints);
            movingEndpointsClone = new HashSet<>(this.movingEndpoints);
            metadata = this.cloneOnlyTokenMap();
        }
        finally
        {
            lock.readLock().unlock();
        }

        pendingRanges.put(keyspaceName, calculatePendingRanges(strategy, metadata, bootstrapTokensClone,
                                                               leavingEndpointsClone, movingEndpointsClone));
    }

    /**
     * @see TokenMetadata#calculatePendingRanges(AbstractReplicationStrategy, String)
     */
    private static PendingRangeMaps calculatePendingRanges(AbstractReplicationStrategy strategy,
                                                           TokenMetadata metadata,
                                                           BiMultiValMap<Token, InetAddressAndPort> bootstrapTokens,
                                                           Set<InetAddressAndPort> leavingEndpoints,
                                                           Set<Pair<Token, InetAddressAndPort>> movingEndpoints)
    {
        PendingRangeMaps newPendingRanges = new PendingRangeMaps();

        RangesByEndpoint addressRanges = strategy.getAddressReplicas(metadata);

        // Copy of metadata reflecting the situation after all leave operations are finished.
        TokenMetadata allLeftMetadata = removeEndpoints(metadata.cloneOnlyTokenMap(), leavingEndpoints);

        // get all ranges that will be affected by leaving nodes
        Set<Range<Token>> removeAffectedRanges = new HashSet<>();
        for (InetAddressAndPort endpoint : leavingEndpoints)
            removeAffectedRanges.addAll(addressRanges.get(endpoint).ranges());

        // for each of those ranges, find what new nodes will be responsible for the range when
        // all leaving nodes are gone.
        for (Range<Token> range : removeAffectedRanges)
        {
            EndpointsForRange currentReplicas = strategy.calculateNaturalReplicas(range.right, metadata);
            EndpointsForRange newReplicas = strategy.calculateNaturalReplicas(range.right, allLeftMetadata);
            for (Replica newReplica : newReplicas)
            {
                if (currentReplicas.endpoints().contains(newReplica.endpoint()))
                    continue;

                // we calculate pending replicas for leave- and move- affected ranges in the same way to avoid
                // a possible conflict when 2 pending replicas have the same endpoint and different ranges.
                for (Replica pendingReplica : newReplica.subtractSameReplication(addressRanges.get(newReplica.endpoint())))
                    newPendingRanges.addPendingRange(range, pendingReplica);
            }
        }

        // At this stage newPendingRanges has been updated according to leave operations. We can
        // now continue the calculation by checking bootstrapping nodes.

        // For each of the bootstrapping nodes, simply add to the allLeftMetadata and check what their
        // ranges would be. We actually need to clone allLeftMetadata each time as resetting its state
        // after getting the new pending ranges is not as simple as just removing the bootstrapping
        // endpoint. If the bootstrapping endpoint constitutes a replacement, removing it after checking
        // the newly pending ranges means there are now fewer endpoints that there were originally and
        // causes its next neighbour to take over its primary range which affects the next RF endpoints
        // in the ring.
        Multimap<InetAddressAndPort, Token> bootstrapAddresses = bootstrapTokens.inverse();
        for (InetAddressAndPort endpoint : bootstrapAddresses.keySet())
        {
            Collection<Token> tokens = bootstrapAddresses.get(endpoint);
            TokenMetadata cloned = allLeftMetadata.cloneOnlyTokenMap();
            cloned.updateNormalTokens(tokens, endpoint);
            for (Replica replica : strategy.getAddressReplicas(cloned, endpoint))
            {
                newPendingRanges.addPendingRange(replica.range(), replica);
            }
        }

        // At this stage newPendingRanges has been updated according to leaving and bootstrapping nodes.
        // We can now finish the calculation by checking moving nodes.

        // For each of the moving nodes, we do the same thing we did for bootstrapping:
        // simply add and remove them one by one to allLeftMetadata and check in between what their ranges would be.
        for (Pair<Token, InetAddressAndPort> moving : movingEndpoints)
        {
            //Calculate all the ranges which will could be affected. This will include the ranges before and after the move.
            Set<Replica> moveAffectedReplicas = new HashSet<>();
            InetAddressAndPort endpoint = moving.right; // address of the moving node
            //Add ranges before the move
            for (Replica replica : strategy.getAddressReplicas(allLeftMetadata, endpoint))
            {
                moveAffectedReplicas.add(replica);
            }

            allLeftMetadata.updateNormalToken(moving.left, endpoint);
            //Add ranges after the move
            for (Replica replica : strategy.getAddressReplicas(allLeftMetadata, endpoint))
            {
                moveAffectedReplicas.add(replica);
            }

            for (Replica replica : moveAffectedReplicas)
            {
                Set<InetAddressAndPort> currentEndpoints = strategy.calculateNaturalReplicas(replica.range().right, metadata).endpoints();
                Set<InetAddressAndPort> newEndpoints = strategy.calculateNaturalReplicas(replica.range().right, allLeftMetadata).endpoints();
                Set<InetAddressAndPort> difference = Sets.difference(newEndpoints, currentEndpoints);
                for (final InetAddressAndPort address : difference)
                {
                    RangesAtEndpoint newReplicas = strategy.getAddressReplicas(allLeftMetadata, address);
                    RangesAtEndpoint oldReplicas = strategy.getAddressReplicas(metadata, address);

                    // Filter out the things that are already replicated
                    newReplicas = newReplicas.filter(r -> !oldReplicas.contains(r));
                    for (Replica newReplica : newReplicas)
                    {
                        // for correctness on write, we need to treat ranges that are becoming full differently
                        // to those that are presently transient; however reads must continue to use the current view
                        // for ranges that are becoming transient. We could choose to ignore them here, but it's probably
                        // cleaner to ensure this is dealt with at point of use, where we can make a conscious decision
                        // about which to use
                        for (Replica pendingReplica : newReplica.subtractSameReplication(oldReplicas))
                        {
                            newPendingRanges.addPendingRange(pendingReplica.range(), pendingReplica);
                        }
                    }
                }
            }

            allLeftMetadata.removeEndpoint(endpoint);
        }

        return newPendingRanges;
    }

    public Token getPredecessor(Token token)
    {
        List<Token> tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + tokenToEndpointMapKeysAsStrings();
        return index == 0 ? tokens.get(tokens.size() - 1) : tokens.get(index - 1);
    }

    public Token getSuccessor(Token token)
    {
        List<Token> tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + tokenToEndpointMapKeysAsStrings();
        return (index == (tokens.size() - 1)) ? tokens.get(0) : tokens.get(index + 1);
    }

    private String tokenToEndpointMapKeysAsStrings()
    {
        lock.readLock().lock();
        try
        {
            return StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** @return a copy of the bootstrapping tokens map */
    public BiMultiValMap<Token, InetAddressAndPort> getBootstrapTokens()
    {
        lock.readLock().lock();
        try
        {
            return new BiMultiValMap<>(bootstrapTokens);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Set<InetAddressAndPort> getAllEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(endpointToHostIdMap.keySet());
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public int getSizeOfAllEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return endpointToHostIdMap.size();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Set<InetAddressAndPort> getAllMembers()
    {
        return getAllEndpoints().stream()
                                .filter(this::isMember)
                                .collect(Collectors.toSet());
    }

    /** caller should not modify leavingEndpoints */
    public Set<InetAddressAndPort> getLeavingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(leavingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public int getSizeOfLeavingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return leavingEndpoints.size();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Endpoints which are migrating to the new tokens
     * @return set of addresses of moving endpoints
     */
    public Set<Pair<Token, InetAddressAndPort>> getMovingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(movingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public int getSizeOfMovingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return movingEndpoints.size();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public static int firstTokenIndex(final ArrayList<Token> ring, Token start, boolean insertMin)
    {
        assert ring.size() > 0;
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = insertMin ? -1 : 0;
        }
        return i;
    }

    public static Token firstToken(final ArrayList<Token> ring, Token start)
    {
        return ring.get(firstTokenIndex(ring, start, false));
    }

    /**
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
     */
    public static Iterator<Token> ringIterator(final ArrayList<Token> ring, Token start, boolean includeMin)
    {
        if (ring.isEmpty())
            return includeMin ? Iterators.singletonIterator(start.getPartitioner().getMinimumToken())
                              : Collections.emptyIterator();

        final boolean insertMin = includeMin && !ring.get(0).isMinimum();
        final int startIndex = firstTokenIndex(ring, start, insertMin);
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < -1)
                    return endOfData();
                try
                {
                    // return minimum for index == -1
                    if (j == -1)
                        return start.getPartitioner().getMinimumToken();
                    // return ring token for other indexes
                    return ring.get(j);
                }
                finally
                {
                    j++;
                    if (j == ring.size())
                        j = insertMin ? -1 : 0;
                    if (j == startIndex)
                        // end iteration
                        j = -2;
                }
            }
        };
    }

    /** used by tests */
    public void clearUnsafe()
    {
        lock.writeLock().lock();
        try
        {
            tokenToEndpointMap.clear();
            endpointToHostIdMap.clear();
            bootstrapTokens.clear();
            leavingEndpoints.clear();
            pendingRanges.clear();
            movingEndpoints.clear();
            sortedTokens.clear();
            topology = Topology.empty();
            invalidateCachedRingsUnsafe();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try
        {
            Multimap<InetAddressAndPort, Token> endpointToTokenMap = tokenToEndpointMap.inverse();
            Set<InetAddressAndPort> eps = endpointToTokenMap.keySet();

            if (!eps.isEmpty())
            {
                sb.append("Normal Tokens:");
                sb.append(LINE_SEPARATOR.getString());
                for (InetAddressAndPort ep : eps)
                {
                    sb.append(ep);
                    sb.append(':');
                    sb.append(endpointToTokenMap.get(ep));
                    sb.append(LINE_SEPARATOR.getString());
                }
            }

            if (!bootstrapTokens.isEmpty())
            {
                sb.append("Bootstrapping Tokens:" );
                sb.append(LINE_SEPARATOR.getString());
                for (Map.Entry<Token, InetAddressAndPort> entry : bootstrapTokens.entrySet())
                {
                    sb.append(entry.getValue()).append(':').append(entry.getKey());
                    sb.append(LINE_SEPARATOR.getString());
                }
            }

            if (!leavingEndpoints.isEmpty())
            {
                sb.append("Leaving Endpoints:");
                sb.append(LINE_SEPARATOR.getString());
                for (InetAddressAndPort ep : leavingEndpoints)
                {
                    sb.append(ep);
                    sb.append(LINE_SEPARATOR.getString());
                }
            }

            if (!pendingRanges.isEmpty())
            {
                sb.append("Pending Ranges:");
                sb.append(LINE_SEPARATOR.getString());
                sb.append(printPendingRanges());
            }
        }
        finally
        {
            lock.readLock().unlock();
        }

        return sb.toString();
    }

    private String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (PendingRangeMaps pendingRangeMaps : pendingRanges.values())
        {
            sb.append(pendingRangeMaps.printPendingRanges());
        }

        return sb.toString();
    }

    public EndpointsForToken pendingEndpointsForToken(Token token, String keyspaceName)
    {
        PendingRangeMaps pendingRangeMaps = this.pendingRanges.get(keyspaceName);
        if (pendingRangeMaps == null)
            return EndpointsForToken.empty(token);

        return pendingRangeMaps.pendingEndpointsFor(token);
    }

    /**
     * @deprecated retained for benefit of old tests
     */
    @Deprecated(since = "4.0")
    public EndpointsForToken getWriteEndpoints(Token token, String keyspaceName, EndpointsForToken natural)
    {
        EndpointsForToken pending = pendingEndpointsForToken(token, keyspaceName);
        return ReplicaLayout.forTokenWrite(Keyspace.open(keyspaceName).getReplicationStrategy(), natural, pending).all();
    }

    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    public Multimap<InetAddressAndPort, Token> getEndpointToTokenMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Multimap<InetAddressAndPort, Token> cloned = HashMultimap.create();
            for (Map.Entry<Token, InetAddressAndPort> entry : tokenToEndpointMap.entrySet())
                cloned.put(entry.getValue(), entry.getKey());
            return cloned;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    public Map<Token, InetAddressAndPort> getNormalAndBootstrappingTokenToEndpointMap()
    {
        lock.readLock().lock();
        try
        {
            Map<Token, InetAddressAndPort> map = new HashMap<>(tokenToEndpointMap.size() + bootstrapTokens.size());
            map.putAll(tokenToEndpointMap);
            map.putAll(bootstrapTokens);
            return map;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return a (stable copy, won't be modified) datacenter to Endpoint map for all the nodes in the cluster.
     */
    public ImmutableMultimap<String, InetAddressAndPort> getDC2AllEndpoints(IEndpointSnitch snitch)
    {
        return Multimaps.index(getAllEndpoints(), snitch::getDatacenter);
    }

    /**
     * @return the Topology map of nodes to DCs + Racks
     *
     * This is only allowed when a copy has been made of TokenMetadata, to avoid concurrent modifications
     * when Topology methods are subsequently used by the caller.
     */
    public Topology getTopology()
    {
        assert !DatabaseDescriptor.isDaemonInitialized() || this != StorageService.instance.getTokenMetadata();
        return topology;
    }

    public long getRingVersion()
    {
        lock.readLock().lock();

        try
        {
            return ringVersion;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public void invalidateCachedRings()
    {   
        lock.writeLock().lock();

        try
        {   
            invalidateCachedRingsUnsafe();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    private void invalidateCachedRingsUnsafe()
    {
        ringVersion++;
        cachedTokenMap.set(null);
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return partitioner.decorateKey(key);
    }

    /**
     * Tracks the assignment of racks and endpoints in each datacenter for all the "normal" endpoints
     * in this TokenMetadata. This allows faster calculation of endpoints in NetworkTopologyStrategy.
     */
    public static class Topology
    {
        /** multi-map of DC to endpoints in that DC */
        private final ImmutableMultimap<String, InetAddressAndPort> dcEndpoints;
        /** map of DC to multi-map of rack to endpoints in that rack */
        private final ImmutableMap<String, ImmutableMultimap<String, InetAddressAndPort>> dcRacks;
        /** reverse-lookup map for endpoint to current known dc/rack assignment */
        private final ImmutableMap<InetAddressAndPort, Pair<String, String>> currentLocations;
        private final Supplier<IEndpointSnitch> snitchSupplier;

        private Topology(Builder builder)
        {
            this.dcEndpoints = ImmutableMultimap.copyOf(builder.dcEndpoints);

            ImmutableMap.Builder<String, ImmutableMultimap<String, InetAddressAndPort>> dcRackBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Multimap<String, InetAddressAndPort>> entry : builder.dcRacks.entrySet())
                dcRackBuilder.put(entry.getKey(), ImmutableMultimap.copyOf(entry.getValue()));
            this.dcRacks = dcRackBuilder.build();

            this.currentLocations = ImmutableMap.copyOf(builder.currentLocations);
            this.snitchSupplier = builder.snitchSupplier;
        }

        /**
         * @return multi-map of DC to endpoints in that DC
         */
        public Multimap<String, InetAddressAndPort> getDatacenterEndpoints()
        {
            return dcEndpoints;
        }

        /**
         * @return map of DC to multi-map of rack to endpoints in that rack
         */
        public ImmutableMap<String, ImmutableMultimap<String, InetAddressAndPort>> getDatacenterRacks()
        {
            return dcRacks;
        }

        /**
         * @return The DC and rack of the given endpoint.
         */
        public Pair<String, String> getLocation(InetAddressAndPort addr)
        {
            return currentLocations.get(addr);
        }

        Builder unbuild()
        {
            return new Builder(this);
        }

        static Builder builder(Supplier<IEndpointSnitch> snitchSupplier)
        {
            return new Builder(snitchSupplier);
        }

        static Topology empty()
        {
            return builder(() -> DatabaseDescriptor.getEndpointSnitch()).build();
        }

        private static class Builder
        {
            /** multi-map of DC to endpoints in that DC */
            private final Multimap<String, InetAddressAndPort> dcEndpoints;
            /** map of DC to multi-map of rack to endpoints in that rack */
            private final Map<String, Multimap<String, InetAddressAndPort>> dcRacks;
            /** reverse-lookup map for endpoint to current known dc/rack assignment */
            private final Map<InetAddressAndPort, Pair<String, String>> currentLocations;
            private final Supplier<IEndpointSnitch> snitchSupplier;

            Builder(Supplier<IEndpointSnitch> snitchSupplier)
            {
                this.dcEndpoints = HashMultimap.create();
                this.dcRacks = new HashMap<>();
                this.currentLocations = new HashMap<>();
                this.snitchSupplier = snitchSupplier;
            }

            Builder(Topology from)
            {
                this.dcEndpoints = HashMultimap.create(from.dcEndpoints);

                this.dcRacks = Maps.newHashMapWithExpectedSize(from.dcRacks.size());
                for (Map.Entry<String, ImmutableMultimap<String, InetAddressAndPort>> entry : from.dcRacks.entrySet())
                    dcRacks.put(entry.getKey(), HashMultimap.create(entry.getValue()));

                this.currentLocations = new HashMap<>(from.currentLocations);
                this.snitchSupplier = from.snitchSupplier;
            }

            /**
             * Stores current DC/rack assignment for ep
             */
            Builder addEndpoint(InetAddressAndPort ep)
            {
                String dc = snitchSupplier.get().getDatacenter(ep);
                String rack = snitchSupplier.get().getRack(ep);
                Pair<String, String> current = currentLocations.get(ep);
                if (current != null)
                {
                    if (current.left.equals(dc) && current.right.equals(rack))
                        return this;
                    doRemoveEndpoint(ep, current);
                }

                doAddEndpoint(ep, dc, rack);
                return this;
            }

            private void doAddEndpoint(InetAddressAndPort ep, String dc, String rack)
            {
                dcEndpoints.put(dc, ep);

                if (!dcRacks.containsKey(dc))
                    dcRacks.put(dc, HashMultimap.<String, InetAddressAndPort>create());
                dcRacks.get(dc).put(rack, ep);

                currentLocations.put(ep, Pair.create(dc, rack));
            }

            /**
             * Removes current DC/rack assignment for ep
             */
            Builder removeEndpoint(InetAddressAndPort ep)
            {
                if (!currentLocations.containsKey(ep))
                    return this;

                doRemoveEndpoint(ep, currentLocations.remove(ep));
                return this;
            }

            private void doRemoveEndpoint(InetAddressAndPort ep, Pair<String, String> current)
            {
                dcRacks.get(current.left).remove(current.right, ep);
                dcEndpoints.remove(current.left, ep);
            }

            Builder updateEndpoint(InetAddressAndPort ep)
            {
                IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
                if (snitch == null || !currentLocations.containsKey(ep))
                    return this;

                updateEndpoint(ep, snitch);
                return this;
            }

            Builder updateEndpoints()
            {
                IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
                if (snitch == null)
                    return this;

                for (InetAddressAndPort ep : currentLocations.keySet())
                    updateEndpoint(ep, snitch);

                return this;
            }

            private void updateEndpoint(InetAddressAndPort ep, IEndpointSnitch snitch)
            {
                Pair<String, String> current = currentLocations.get(ep);
                String dc = snitch.getDatacenter(ep);
                String rack = snitch.getRack(ep);
                if (dc.equals(current.left) && rack.equals(current.right))
                    return;

                doRemoveEndpoint(ep, current);
                doAddEndpoint(ep, dc, rack);
            }

            Topology build()
            {
                return new Topology(this);
            }
        }
    }
}
