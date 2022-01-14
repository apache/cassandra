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

package org.apache.cassandra.nodes;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.LoadingMap;

/**
 * Provides access and updates the locally stored information about this and other nodes. The information is cached in
 * memory in a thread-safe way and additionally stored using the provided implementation of {@link INodesPersistence},
 * which is {@link NodesPersistence} by default and stores everything in system keyspace.
 */
public class Nodes
{
    private static final Logger logger = LoggerFactory.getLogger(Nodes.class);

    @VisibleForTesting
    private final ExecutorService updateExecutor;

    private final INodesPersistence nodesPersistence;

    private final Peers peers;
    private final Local local;

    private static class InstanceHolder
    {
        // put into subclass for lazy initialization
        private static final Nodes instance = new Nodes();
    }

    /**
     * Returns the singleton instance
     */
    public static Nodes getInstance()
    {
        return InstanceHolder.instance;
    }

    /**
     * Returns the singleton instance of {@link Peers}
     */
    public static Peers peers()
    {
        return getInstance().getPeers();
    }

    /**
     * Returns the singleton instance of {@link Local}
     */
    public static Local local()
    {
        return getInstance().getLocal();
    }

    /**
     * Returns information about the node with the given address - if the node address matches the local (broadcast)
     * address, the returned object is a {@link LocalInfo}. Otherwise, it is {@link PeerInfo} (or {@code null} if no
     * informatino is available).
     */
    @Nullable
    public static INodeInfo<?> localOrPeerInfo(InetAddressAndPort endpoint)
    {
        return Objects.equals(endpoint, FBUtilities.getBroadcastAddressAndPort()) ? local().get() : peers().get(endpoint);
    }

    public static Optional<INodeInfo<?>> localOrPeerInfoOpt(InetAddressAndPort endpoint)
    {
        return Optional.ofNullable(localOrPeerInfo(endpoint));
    }

    /**
     * @see #updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     */
    public static INodeInfo<?> updateLocalOrPeer(InetAddressAndPort endpoint, UnaryOperator<NodeInfo<?>> update)
    {
        return updateLocalOrPeer(endpoint, update, false);
    }

    /**
     * @see #updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     */
    public static INodeInfo<?> updateLocalOrPeer(InetAddressAndPort endpoint, UnaryOperator<NodeInfo<?>> update, boolean blocking)
    {
        return updateLocalOrPeer(endpoint, update, blocking, false);
    }

    /**
     * Updates either local or peer information in a thread-safe way, depeending on whether the provided address matches
     * the local (broadcast) address.
     *
     * @see Local#updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     * @see Peers#updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     */
    public static INodeInfo<?> updateLocalOrPeer(InetAddressAndPort endpoint, UnaryOperator<NodeInfo<?>> update, boolean blocking, boolean force)
    {
        if (Objects.equals(endpoint, FBUtilities.getBroadcastAddressAndPort()))
            return local().update(info -> (LocalInfo) update.apply(info), blocking, force);
        else
            return peers().update(endpoint, info -> (PeerInfo) update.apply(info), blocking, force);
    }

    /**
     * Checks whether the provided address is local or known peer address.
     */
    public static boolean isKnownEndpoint(InetAddressAndPort endpoint)
    {
        return localOrPeerInfo(endpoint) != null;
    }


    public static UUID getHostId(InetAddressAndPort endpoint, UUID defaultValue)
    {
        INodeInfo<?> info = localOrPeerInfo(endpoint);
        return info != null ? info.getHostId() : defaultValue;
    }

    public static String getDataCenter(InetAddressAndPort endpoint, String defaultValue)
    {
        INodeInfo<?> info = localOrPeerInfo(endpoint);
        return info != null ? info.getDataCenter() : defaultValue;
    }

    public static String getRack(InetAddressAndPort endpoint, String defaultValue)
    {
        INodeInfo<?> info = localOrPeerInfo(endpoint);
        return info != null ? info.getRack() : defaultValue;
    }

    public static CassandraVersion getReleaseVersion(InetAddressAndPort endpoint, CassandraVersion defaultValue)
    {
        INodeInfo<?> info = localOrPeerInfo(endpoint);
        return info != null ? info.getReleaseVersion() : defaultValue;
    }

    public static UUID getSchemaVersion(InetAddressAndPort endpoint, UUID defaultValue)
    {
        INodeInfo<?> info = localOrPeerInfo(endpoint);
        return info != null ? info.getSchemaVersion() : defaultValue;
    }

    public static Collection<Token> getTokens(InetAddressAndPort endpoint, Collection<Token> defaultValue)
    {
        INodeInfo<?> info = localOrPeerInfo(endpoint);
        return info != null ? info.getTokens() : defaultValue;
    }

    public static InetAddressAndPort getNativeTransportAddressAndPort(InetAddressAndPort endpoint, InetAddressAndPort defaultValue)
    {
        INodeInfo<?> info = localOrPeerInfo(endpoint);
        return info != null ? info.getNativeTransportAddressAndPort() : defaultValue;
    }

    /**
     * Initializes singleton instance of {@link Nodes}. If it is not a Cassandra server process or
     * {@code cassandra.nodes.disablePersitingToSystemKeyspace} is set to {@code true}, the instance does not persist
     * stored information anywhere.
     */
    private Nodes()
    {
        this(!DatabaseDescriptor.isDaemonInitialized() || Boolean.getBoolean("cassandra.nodes.disablePersitingToSystemKeyspace")
             ? INodesPersistence.NO_NODES_PERSISTENCE
             : new NodesPersistence(),
             Executors.newSingleThreadExecutor(new NamedThreadFactory("nodes-info-persistence")));
    }

    @VisibleForTesting
    public Nodes(INodesPersistence nodesPersistence, ExecutorService updateExecutor)
    {
        this.updateExecutor = updateExecutor;
        this.nodesPersistence = nodesPersistence;
        this.local = new Local().load();
        this.peers = new Peers().load();
    }

    public void reload()
    {
        this.local.load();
        this.peers.load();
    }

    public Peers getPeers()
    {
        return peers;
    }

    public Local getLocal()
    {
        return local;
    }

    private Runnable wrapPersistenceTask(String name, Runnable task)
    {
        return () -> {
            try
            {
                task.run();
            }
            catch (RuntimeException ex)
            {
                logger.error("Unexpected exception - " + name, ex);
                throw ex;
            }
        };
    }

    public class Peers
    {
        private final LoadingMap<InetAddressAndPort, PeerInfo> internalMap = new LoadingMap<>();

        public IPeerInfo update(InetAddressAndPort peer, UnaryOperator<PeerInfo> update)
        {
            return update(peer, update, false);
        }

        public IPeerInfo update(InetAddressAndPort peer, UnaryOperator<PeerInfo> update, boolean blocking)
        {
            return update(peer, update, blocking, false);
        }

        /**
         * Updates peer information in a thread-safe way.
         *
         * @param peer     address of a peer to be updated
         * @param update   update function, which receives a copy of the current {@link PeerInfo} and is expected to
         *                 return the updated {@link PeerInfo}; the function may apply updates directly on the received
         *                 copy and return it
         * @param blocking if set, the method will block until the changes are persisted
         * @param force    the update will be persisted even if no changes are made
         * @return the updated object
         */
        public IPeerInfo update(InetAddressAndPort peer, UnaryOperator<PeerInfo> update, boolean blocking, boolean force)
        {
            return internalMap.compute(peer, (key, existingPeerInfo) -> {
                PeerInfo updated = existingPeerInfo == null
                                   ? update.apply(new PeerInfo().setPeerAddressAndPort(peer))
                                   : update.apply(existingPeerInfo.duplicate()); // since we operate on mutable objects, we don't want to let the update function to operate on the live object

                if (updated.getPeerAddressAndPort() == null)
                    updated.setPeerAddressAndPort(peer);
                else
                    Preconditions.checkArgument(Objects.equals(updated.getPeerAddressAndPort(), peer));

                updated.setRemoved(false);
                save(existingPeerInfo, updated, blocking, force);
                return updated;
            });
        }

        /**
         * @param peer peer to remove
         * @param blocking block until the removal is persisted and synced
         * @param hard remove also the transient state instead of just setting {@link PeerInfo#isRemoved()} state
         * @return the remove
         */
        public IPeerInfo remove(InetAddressAndPort peer, boolean blocking, boolean hard)
        {
            AtomicReference<PeerInfo> removed = new AtomicReference<>();
            internalMap.computeIfPresent(peer, (key, existingPeerInfo) -> {
                delete(peer, blocking);
                existingPeerInfo.setRemoved(true);
                removed.set(existingPeerInfo);
                return hard ? null : existingPeerInfo;
            });
            return removed.get();
        }

        /**
         * Returns a peer information for a given address if the peer is known. Otherwise, returns {@code null}.
         * Note that you should never try to manually cast the returned object to a mutable instnace and modify it.
         */
        @Nullable
        public IPeerInfo get(InetAddressAndPort peer)
        {
            return internalMap.get(peer);
        }

        /**
         * Returns optional of a peer information for a given address.
         * Note that you should never try to manually cast the returned object to a mutable instnace and modify it.
         */
        public Optional<IPeerInfo> getOpt(InetAddressAndPort peer)
        {
            return Optional.ofNullable(get(peer));
        }

        /**
         * Returns a stream of all known peers.
         * Note that you should never try to manually cast the returned objects to a mutable instnaces and modify it.
         */
        public Stream<IPeerInfo> get()
        {
            return internalMap.valuesStream().map(IPeerInfo.class::cast);
        }

        private void save(PeerInfo previousInfo, PeerInfo newInfo, boolean blocking, boolean force)
        {
            if (!force && Objects.equals(previousInfo, newInfo))
            {
                logger.trace("Saving peer skipped: {}", previousInfo);
                return;
            }

            logger.trace("Saving peer: {}, blocking = {}, force = {}", newInfo, blocking, force);
            Future<?> f = updateExecutor.submit(wrapPersistenceTask("saving peer information: " + newInfo, () -> {
                nodesPersistence.savePeer(newInfo);
                logger.trace("Saved peer: {}", newInfo);
                if (blocking)
                    nodesPersistence.syncPeers();
            }));
            if (blocking)
                FBUtilities.waitOnFuture(f);
        }

        private Peers load()
        {
            logger.trace("Loading peers...");
            nodesPersistence.loadPeers().forEach(info -> internalMap.compute(info.getPeerAddressAndPort(), (key, existingPeerInfo) -> info));
            if (logger.isTraceEnabled())
                logger.trace("Loaded peers: {}", internalMap.valuesStream().collect(Collectors.toList()));
            return this;
        }

        private void delete(InetAddressAndPort peer, boolean blocking)
        {
            if (logger.isTraceEnabled())
                logger.trace("Deleting peer " + peer + ", blocking = " + blocking, new Throwable());
            Future<?> f = updateExecutor.submit(wrapPersistenceTask("deleting peer information: " + peer, () -> {
                nodesPersistence.deletePeer(peer);
                logger.trace("Deleted peer {}", peer);
                if (blocking)
                    nodesPersistence.syncPeers();
            }));

            if (blocking)
                FBUtilities.waitOnFuture(f);
        }
    }

    public class Local
    {
        private final LoadingMap<InetAddressAndPort, LocalInfo> internalMap = new LoadingMap<>(1);
        private final InetAddressAndPort localInfoKey = FBUtilities.getBroadcastAddressAndPort();

        /**
         * @see #update(UnaryOperator, boolean, boolean)
         */
        public ILocalInfo update(UnaryOperator<LocalInfo> update)
        {
            return update(update, false);
        }

        /**
         * @see #update(UnaryOperator, boolean, boolean)
         */
        public ILocalInfo update(UnaryOperator<LocalInfo> update, boolean blocking)
        {
            return update(update, blocking, false);
        }

        /**
         * Updates local node information in a thread-safe way.
         *
         * @param update   update function, which receives a copy of the current {@link LocalInfo} and is expected to
         *                 return the updated {@link LocalInfo}; the function may apply updates directly on the received
         *                 copy and return it
         * @param blocking if set, the method will block until the changes are persisted
         * @param force    the update will be persisted even if no changes are made
         * @return a copy of updated object
         */
        public ILocalInfo update(UnaryOperator<LocalInfo> update, boolean blocking, boolean force)
        {
            return internalMap.compute(localInfoKey, (key, existingLocalInfo) -> {
                LocalInfo updated = existingLocalInfo == null
                                    ? update.apply(new LocalInfo())
                                    : update.apply(existingLocalInfo.duplicate()); // since we operate on mutable objects, we don't want to let the update function to operate on the live object
                save(existingLocalInfo, updated, blocking, force);
                return updated;
            });
        }

        /**
         * Returns information about the local node (if present).
         * Note that you should never try to manually cast the returned object to a mutable instnace and modify it.
         */
        public ILocalInfo get()
        {
            return internalMap.get(localInfoKey);
        }

        private void save(LocalInfo previousInfo, LocalInfo newInfo, boolean blocking, boolean force)
        {
            if (!force && Objects.equals(previousInfo, newInfo))
            {
                logger.trace("Saving local skipped: {}", previousInfo);
                return;
            }

            Future<?> f = updateExecutor.submit(wrapPersistenceTask("saving local node information: " + newInfo, () -> {
                nodesPersistence.saveLocal(newInfo);
                logger.trace("Saving local: {}, blocking = {}, force = {}", newInfo, blocking, force);
                if (blocking)
                    nodesPersistence.syncLocal();
            }));

            if (blocking)
                FBUtilities.waitOnFuture(f);
        }

        private Local load()
        {
            logger.trace("Loading local...");
            internalMap.compute(localInfoKey, (key, existingLocalInfo) -> {
                LocalInfo info = nodesPersistence.loadLocal();
                return info != null ? info : new LocalInfo();
            });
            if (logger.isTraceEnabled())
                logger.trace("Loaded local: {}", internalMap.get(localInfoKey));
            return this;
        }
    }
}
