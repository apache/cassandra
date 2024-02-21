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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;
import org.msgpack.jackson.dataformat.MessagePackFactory;

/**
 * Manages information about the local node and peers.
 *
 * Information in memory is always consistent - i.e. an update is immediately visible.
 *
 * Updates for the local node can be either persisted immediately, which adds the penalty
 * of serializing the information to disk, or asynchronously, which is pretty quick.
 * Only a handful of changes need to be persisted synchronously. Most changes are safe to
 * be performed asynchronously, because those can be reconstructed during a restart.
 *
 * Updates for peers are always persisted asynchronously. Information about peers always
 * originates from Gossip, which does represent a "best effort" and represents potentially
 * outdated information. Even if information about a peer is lost, it will eventually
 * become updated via Gossip.
 *
 * Data is persisted using jackson using the message-pack serialization format in two
 * files beneath the directory specified via the constructor: {@code local} + {@code peers},
 * which holds the information in {@link LocalInfo} and {@link PeerInfo} respectively.
 *
 * Some performance numbers from a local test:
 * <ul>
 * <li>time to serialize {@link LocalInfo}: ~30µs</li>
 * <li>time to serialize 3 {@link PeerInfo}: ~150µs</li>
 * <li>time to serialize 100 {@link PeerInfo}: ~2.5ms</li>
 * <li>time to serialize 1000 {@link PeerInfo}: ~25ms</li>
 * <li>time to update {@link LocalInfo} in a blocking fashion w/ synchronous persistence: ~90µs</li>
 * <li>time to update {@link LocalInfo}, mark as dirty and trigger async persistence: ~400ns</li>
 * <li>time to update one {@link PeerInfo}, mark as dirty and trigger async persistence: ~400ns</li>
 * </ul>
 */
public class Nodes
{
    private static final Logger logger = LoggerFactory.getLogger(Nodes.class);

    private final ExecutorService updateExecutor;

    private final Path baseDirectory;
    private final Path snapshotsDirectory;

    private final Peers peers;
    private final Local local;

    /**
     * Convenience method to retrieve the production code singleton.
     */
    public static Nodes nodes()
    {
        return Instance.nodes();
    }

    /**
     * Convenience method to retrieve the production code singleton.
     */
    public static Local local()
    {
        return nodes().local;
    }

    /**
     * Convenience method to retrieve the production code singleton.
     */
    public static Peers peers()
    {
        return nodes().peers;
    }

    public void shutdown()
    {
        local.close();
        peers.close();
    }

    public void syncToDisk()
    {
        local.syncToDisk();
        peers.syncToDisk();
    }

    public void snapshot(String snapshotName) throws IOException
    {
        syncToDisk();

        Path snapshotPath = snapshotsDirectory.resolve(snapshotName);
        File snapshotFile = new File(snapshotPath);
        try
        {
            Files.createDirectories(snapshotPath);
            local.snapshot(snapshotPath);
            peers.snapshot(snapshotPath);

            SyncUtil.trySyncDir(snapshotFile);

            logger.info("Created snapshot for local+peers in {}", snapshotPath);
        }
        catch (IOException e)
        {
            FileUtils.deleteRecursive(snapshotFile);
            throw e;
        }
    }

    public void clearSnapshot(String snapshotName)
    {
        Path snapshotPath = baseDirectory.resolve("snapshots");
        if (snapshotName != null && !snapshotName.isEmpty())
            snapshotPath = snapshotPath.resolve(snapshotName);
        if (Files.isDirectory(snapshotPath))
            FileUtils.deleteRecursive(new File(snapshotPath));
    }

    /**
     * Update information about either the local node or a peer.
     *
     * @param peer the peer to update. Routes to {@link Local#update(Consumer, boolean)}, if this is equal
     * to {@link FBUtilities#getBroadcastAddressAndPort()} and to {@link Peers#update(InetAddressAndPort, Consumer)} otherwise.
     * @param updater the update function that takes the base class {@link NodeInfo}
     */
    public void update(InetAddressAndPort peer, Consumer<NodeInfo> updater)
    {
        if (FBUtilities.getBroadcastAddressAndPort().equals(peer))
            local.update(updater::accept, false);
        else
            peers.update(peer, updater::accept);
    }

    /**
     * More convenient variant than {@link #update(InetAddressAndPort, Consumer)} when only a single attribute
     * needs to be updated.
     *
     * @param peer peer addres
     * @param value value to set
     * @param updater setter method
     * @param <V> value type
     */
    public <V> void update(InetAddressAndPort peer, V value, BiConsumer<NodeInfo, V> updater)
    {
        if (FBUtilities.getBroadcastAddressAndPort().equals(peer))
            local.update(value, updater::accept, false);
        else
            peers.update(peer, value, updater::accept);
    }

    @VisibleForTesting
    public void resetUnsafe()
    {
        local.resetUnsafe();
        peers.resetUnsafe();
        listSnapshots().forEach(p -> FileUtils.deleteRecursive(new File(p)));
    }

    /**
     * Returns either the {@link LocalInfo} or a {@link PeerInfo}, if such a peer is known, otherwise {@code null}.
     */
    public NodeInfo get(InetAddressAndPort peer)
    {
        if (FBUtilities.getBroadcastAddressAndPort().equals(peer))
            return local.get();
        return peers.get(peer);
    }

    /**
     * Convenience function to extract some information for either the local node or a peer.
     *
     * @param peer the endpoint to get and map the information for
     * @param mapper function that takes a {@link NodeInfo} and returns the mapped information
     * @param ifNotExists value to return if the peer does not exist or the {@code mapper} function returned {@code
     * null}
     * @param <R> return type
     *
     * @return mapped value for the peer or the result of {@code ifNotExists}
     */
    protected <R> R map(InetAddressAndPort peer, Function<NodeInfo, R> mapper, Supplier<R> ifNotExists)
    {
        NodeInfo nodeInfo = FBUtilities.getBroadcastAddressAndPort().equals(peer) ? local.get() : peers.get(peer);
        R r = null;
        if (nodeInfo != null)
            r = mapper.apply(nodeInfo);
        if (r == null)
            r = ifNotExists.get();
        return r;
    }

    protected <R> R map(InetAddressAndPort peer, Function<NodeInfo, R> mapper)
    {
        return map(peer, mapper, () -> null);
    }

    private Nodes(Path storageDirectory)
    {
        this.updateExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("nodes-info-persistence"));
        this.baseDirectory = storageDirectory;
        this.snapshotsDirectory = this.baseDirectory.resolve("snapshots");
        maybeInitializeDirectories();

        // setup msgpack serialization
        ObjectMapper objectMapper = createObjectMapper();

        this.local = new Local(objectMapper, storageDirectory);
        this.peers = new Peers(objectMapper, storageDirectory);
    }

    @VisibleForTesting
    static ObjectMapper createObjectMapper()
    {
        MessagePackFactory messagePackFactory = new MessagePackFactory();
        messagePackFactory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        ObjectMapper objectMapper = new ObjectMapper(messagePackFactory);
        objectMapper.registerModule(SerHelper.createMsgpackModule());
        return objectMapper;
    }

    private void maybeInitializeDirectories()
    {
        if (!Files.exists(baseDirectory))
            initializeDirectory(baseDirectory);
        if (!Files.exists(snapshotsDirectory))
            initializeDirectory(snapshotsDirectory);
    }

    private void initializeDirectory(Path dir)
    {
        try
        {
            Files.createDirectories(dir);
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    private synchronized <T> T transactionalRead(Path originalPath, Path backupPath, Path tempPath,
                                                 TxReader<T> reader, Supplier<T> onEmpty)
    {
        maybeCleanupPendingTransaction(originalPath, backupPath, tempPath);

        if (!Files.exists(originalPath))
        {
            logger.debug("{} not found, continuing", originalPath);
            return onEmpty.get();
        }

        logger.debug("Reading {}", originalPath);
        try (BufferedInputStream input = new BufferedInputStream(Files.newInputStream(originalPath, StandardOpenOption.READ)))
        {
            return reader.read(input);
        }
        catch (Exception e)
        {
            throw new FSReadError(e, originalPath);
        }
    }

    private synchronized void transactionalWrite(Path originalPath, Path backupPath, Path tempPath,
                                                 TxWriter writer, Path syncDir)
    {
        logger.trace("Writing {}", originalPath);

        // 0. cleanup aborted transaction
        // 1. hard link current file as *.old
        // 2. write data to temp file as *.txn
        // 3. delete current file
        // 4. move *.txn to current file
        // 5. delete *.old file

        try
        {
            maybeCleanupPendingTransaction(originalPath, backupPath, tempPath);
            maybeInitializeDirectories();

            boolean originalExists = Files.exists(originalPath);
            if (originalExists)
                Files.createLink(backupPath, originalPath);

            try (FileChannel fc = FileChannel.open(tempPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 OutputStream output = new BufferedOutputStream(Channels.newOutputStream(fc)))
            {
                writer.write(output);
                output.flush();
                SyncUtil.force(fc, true);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, tempPath.toString());
            }

            if (originalExists)
                Files.delete(originalPath);
            Files.move(tempPath, originalPath, StandardCopyOption.ATOMIC_MOVE);
            if (originalExists)
                Files.delete(backupPath);

            SyncUtil.trySyncDir(new File(syncDir));
        }
        catch (Throwable e)
        {
            logger.error("Failed to perform transactional write of {}", originalPath, e);
            // try to cleanup the probably pending transaction, but don't let that one throw an exception
            maybeCleanupPendingTransactionNoFail(originalPath, backupPath, tempPath);
            if (e instanceof IOException)
                throw new FSWriteError(e, originalPath);
            throw Throwables.cleaned(e);
        }
    }

    private void maybeCleanupPendingTransactionNoFail(Path originalPath, Path backupPath, Path tempPath)
    {
        try
        {
            maybeCleanupPendingTransaction(originalPath, backupPath, tempPath);
        }
        catch (Exception e)
        {
            logger.error("Failed to cleanup a pending transaction after an error condition (see above)", e);
            JVMStabilityInspector.inspectThrowable(e);
        }
    }

    private void maybeCleanupPendingTransaction(Path originalPath, Path backupPath, Path tempPath)
    {
        if (Files.exists(backupPath))
        {
            logger.warn("{} exists - using {} for {}", backupPath, tempPath, originalPath);
            try
            {
                // aborted transaction!
                Files.deleteIfExists(originalPath);
                Files.move(backupPath, originalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                Files.deleteIfExists(tempPath);
            }
            catch (Exception e)
            {
                throw Throwables.cleaned(e);
            }
        }
    }

    private List<Path> listSnapshots()
    {
        if (!Files.isDirectory(snapshotsDirectory))
            return Collections.emptyList();

        try (Stream<Path> snapshots = Files.list(snapshotsDirectory))
        {
            return snapshots.collect(Collectors.toList());
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    @FunctionalInterface
    interface TxReader<T>
    {
        T read(InputStream inputStream) throws Exception;
    }

    @FunctionalInterface
    interface TxWriter
    {
        void write(OutputStream outputStream) throws Exception;
    }

    public final class Local
    {
        private final ObjectReader localReader;
        private final ObjectWriter localWriter;
        private final Path localPath;
        private final Path localBackupPath;
        private final Path localTempPath;

        private volatile LocalInfo localInfo;
        private volatile boolean closed;

        public Local(ObjectMapper objectMapper, Path storageDirectory)
        {
            localPath = storageDirectory.resolve("local");
            localBackupPath = localPath.resolveSibling(localPath.getFileName().toString() + ".old");
            localTempPath = localPath.resolveSibling(localPath.getFileName().toString() + ".txn");

            localReader = objectMapper.readerFor(LocalInfo.class);
            localWriter = objectMapper.writerFor(LocalInfo.class);

            try
            {
                localInfo = transactionalRead(localPath, localBackupPath, localTempPath, localReader::readValue, LocalInfo::new);
            }
            catch (FSReadError e)
            {
                logger.warn("Failed to load localinfo at startup. Creating new local table", e);
                localInfo = new LocalInfo();
            }
            getOrInitializeLocalHostId();
        }

        private synchronized void saveToDisk()
        {
            // called via updateExecutor
            // This one takes about 80µs to serialize
            transactionalWrite(localPath, localBackupPath, localTempPath, this::write, baseDirectory);
        }

        private synchronized void snapshot(Path snapshotPath) throws IOException
        {
            Path localSnapshot = snapshotPath.resolve(localPath.getFileName());
            if (Files.exists(localPath) && !Files.exists(localSnapshot))
                Files.createLink(localSnapshot, localPath);
        }

        private void write(OutputStream output) throws IOException
        {
            localInfo.resetDirty();
            localWriter.writeValue(output, localInfo);
        }

        private void close()
        {
            closed = true;
        }

        private void syncToDisk()
        {
            if (localInfo.isDirty())
                saveToDisk();
        }

        private void resetUnsafe()
        {
            localInfo = new LocalInfo();
            saveToDisk();
        }

        /**
         * Update information about the local node.
         *
         * In blocking mode, exceptions throws by the actual save-to-disk operation are rethrown
         * by this function. It is safe to consider that those exceptions are already handled by
         * the {@link JVMStabilityInspector}.
         *
         * @param updater function that updates the {@link LocalInfo} instance.
         * @param blocking whether the update must be performed in a blocking way, i.e. whether the update
         */
        public void update(Consumer<LocalInfo> updater, boolean blocking)
        {
            if (closed)
                throw new IllegalStateException("Nodes instance already closed");

            LocalInfo local = localInfo;
            updater.accept(local);

            if (local.isDirty())
            {
                Future future = updateExecutor.submit(() -> saveToDisk());
                if (blocking)
                    FBUtilities.waitOnFuture(future);
            }
        }

        /**
         * More convenient variant than {@link #update(Consumer, boolean)} when only a single attribute
         * needs to be updated.
         *
         * In blocking mode, exceptions throws by the actual save-to-disk operation are rethrown
         * by this function. It is safe to consider that those exceptions are already handled by
         * the {@link JVMStabilityInspector}.
         *
         * @param value value to set
         * @param updater setter method
         * @param blocking whether the update must be performed in a blocking way, i.e. whether the update
         * must have been persisted before this method returns.
         */
        public <V> void update(V value, BiConsumer<LocalInfo, V> updater, boolean blocking)
        {
            if (closed)
                throw new IllegalStateException("Nodes instance already closed");

            LocalInfo local = localInfo;
            updater.accept(local, value);

            if (local.isDirty())
            {
                Future future = updateExecutor.submit(() -> saveToDisk());
                if (blocking)
                    FBUtilities.waitOnFuture(future);
            }
        }

        public LocalInfo get()
        {
            return localInfo;
        }

        public long getTruncatedAt(TableId id)
        {
            LocalInfo.TruncationRecord record = get().getTruncationRecords().get(id.asUUID());
            return record == null ? Long.MIN_VALUE : record.truncatedAt;
        }

        /**
         * This removes the truncation record for a table and assumes that such record exists on disk.
         * This is called during CL reply, before the truncation records are loaded into memory.
         */
        public void removeTruncationRecord(TableId id)
        {
            update(l -> {
                Map<UUID, LocalInfo.TruncationRecord> truncatedMap = new HashMap<>(l.getTruncationRecords());
                if (truncatedMap.remove(id.asUUID()) != null)
                    l.setTruncationRecords(truncatedMap);
            }, true);
        }

        public void saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
        {
            saveTruncationRecord(cfs.metadata.id.asUUID(), truncatedAt, position);
        }

        public void saveTruncationRecord(UUID tableId, long truncatedAt, CommitLogPosition position)
        {
            update(l -> {
                Map<UUID, LocalInfo.TruncationRecord> truncatedMap = new HashMap<>(l.getTruncationRecords());

                truncatedMap.put(tableId, new LocalInfo.TruncationRecord(position, truncatedAt));

                l.setTruncationRecords(truncatedMap);
            }, true);
        }

        public void updateTokens(Collection<Token> tokens)
        {
            if (tokens.isEmpty())
                throw new IllegalStateException("removeEndpoint should be used instead");

            update(l -> {
                Collection<Token> savedTokens = l.getTokens();
                if (savedTokens != null && tokens.containsAll(savedTokens) && tokens.size() == savedTokens.size())
                    return;
                l.setTokens(tokens);
            }, true);
        }

        public Collection<Token> getSavedTokens()
        {
            return localInfo.getTokens() == null ? Collections.EMPTY_LIST : localInfo.getTokens();
        }

        public int incrementAndGetGeneration()
        {
            int generation;
            int storedGeneration = localInfo.getGossipGeneration() + 1;
            final int now = (int) (System.currentTimeMillis() / 1000);
            if (storedGeneration >= now)
            {
                logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems",
                            storedGeneration, now);
                generation = storedGeneration;
            }
            else
            {
                generation = now;
            }
            update(l -> { l.setGossipGeneration(generation); }, true);
            return generation;
        }

        private UUID getOrInitializeLocalHostId()
        {
            UUID hostId = localInfo.getHostId();
            if (hostId != null)
                return hostId;
            update(l -> {
                if (l.getHostId() == null)
                {
                    l.setHostId(UUID.randomUUID());
                    logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", l.getHostId());
                }
            }, true);
            return localInfo.getHostId();
        }
    }

    public final class Peers
    {
        private final ObjectReader peerReader;
        private final ObjectWriter peerWriter;
        private final Path peersPath;
        private final Path peersBackupPath;
        private final Path peersTempPath;

        private ConcurrentMap<InetAddressAndPort, PeerInfo> peers;
        private volatile boolean closed;

        public Peers(ObjectMapper objectMapper, Path storageDirectory)
        {
            peersPath = storageDirectory.resolve("peers");
            peersBackupPath = peersPath.resolveSibling(peersPath.getFileName().toString() + ".old");
            peersTempPath = peersPath.resolveSibling(peersPath.getFileName().toString() + ".txn");

            peerReader = objectMapper.readerFor(new TypeReference<Collection<PeerInfo>>() {});
            peerWriter = objectMapper.writerFor(new TypeReference<Collection<PeerInfo>>() {});

            try
            {
                peers = transactionalRead(peersPath, peersBackupPath, peersTempPath, this::read, ConcurrentHashMap::new);
            }
            catch (FSReadError e)
            {
                logger.warn("Failed to read peers at startup. Creating new peers table", e);
                peers = new ConcurrentHashMap<>();
            }
        }

        private synchronized void saveToDisk()
        {
            // called via updateExecutor
            transactionalWrite(peersPath, peersBackupPath, peersTempPath, this::write, baseDirectory);
        }

        private synchronized void snapshot(Path snapshotPath) throws IOException
        {
            // Needs to be synchronized to avoid snapshotting in the middle of
            // a write
            Path peersSnapshot = snapshotPath.resolve(peersPath.getFileName());
            if (Files.exists(peersPath) && !Files.exists(peersSnapshot))
                Files.createLink(peersSnapshot, peersPath);
        }

        private void write(OutputStream output) throws IOException
        {
            peers.values().forEach(NodeInfo::resetDirty);
            peerWriter.writeValue(output, peers.values());
        }

        private void close()
        {
            closed = true;
        }

        private void syncToDisk()
        {
            if (peers.values().stream().anyMatch(NodeInfo::isDirty))
                saveToDisk();
        }

        private ConcurrentMap<InetAddressAndPort, PeerInfo> read(InputStream input) throws IOException
        {
            ConcurrentMap<InetAddressAndPort, PeerInfo> map = new ConcurrentHashMap<>();
            Collection<PeerInfo> collection = peerReader.readValue(input);
            for (PeerInfo peer : collection)
                map.put(peer.getPeer(), peer);
            return map;
        }

        private void resetUnsafe()
        {
            peers.clear();
            saveToDisk();
        }

        /**
         * Update information about a peer.
         *
         * @param peer the peer to update
         * @param updater function that updates the {@link PeerInfo} instance.
         */
        public void update(InetAddressAndPort peer, Consumer<PeerInfo> updater)
        {
            if (closed)
                throw new IllegalStateException("Nodes instance already closed");

            PeerInfo peerInfo = peers.computeIfAbsent(peer, PeerInfo::new);
            updater.accept(peerInfo);
            if (peerInfo.isDirty())
                updateExecutor.submit(() -> saveToDisk());
        }

        /**
         * More convenient variant than {@link #update(InetAddressAndPort, Consumer)} when only a single attribute
         * needs to be updated.
         *
         * @param peer peer addres
         * @param value value to set
         * @param updater setter method
         */
        public <V> void update(InetAddressAndPort peer, V value, BiConsumer<PeerInfo, V> updater)
        {
            if (closed)
                throw new IllegalStateException("Nodes instance already closed");

            PeerInfo peerInfo = peers.computeIfAbsent(peer, PeerInfo::new);
            updater.accept(peerInfo, value);
            if (peerInfo.isDirty())
                updateExecutor.submit(() -> saveToDisk());
        }

        /**
         * Get a {@link Stream} of all {@link PeerInfo}s.
         */
        public Stream<PeerInfo> stream()
        {
            return peers.values().stream();
        }

        public void remove(InetAddressAndPort peer)
        {
            if (closed)
                throw new IllegalStateException("Nodes instance already closed");

            if (peers.remove(peer) != null)
                updateExecutor.submit(() -> saveToDisk());
        }

        /**
         * Convenience function to extract some information for a peer.
         *
         * @param peer the endpoint to get and map the information for
         * @param mapper function that takes a {@link PeerInfo} and returns the mapped information
         * @param ifNotExists value to return if the peer does not exist or the {@code mapper} function returned {@code
         * null}
         *
         * @return mapped value for the peer or the result of {@code ifNotExists}
         */
        public <R> R map(InetAddressAndPort peer, Function<PeerInfo, R> mapper, Supplier<R> ifNotExists)
        {
            PeerInfo peerInfo = peers.get(peer);
            R r = null;
            if (peerInfo != null)
                r = mapper.apply(peerInfo);
            if (r == null)
                r = ifNotExists.get();
            return r;
        }

        public PeerInfo get(InetAddressAndPort peer)
        {
            return peers.get(peer);
        }

        /**
         * Return a map of IP addresses containing a map of dc and rack info
         */
        public Map<InetAddressAndPort, Map<String, String>> getDcRackInfo()
        {
            return stream().collect(Collectors.toMap(PeerInfo::getPeer, peerInfo -> {
                Map<String, String> dcRack = new HashMap<>();
                dcRack.put("data_center", peerInfo.getDataCenter());
                dcRack.put("rack", peerInfo.getRack());
                return dcRack;
            }, (a, b) -> a, HashMap::new));
        }

        /**
         * Returns true if preferred IP for given endpoint is known, otherwise false.
         */
        public boolean hasPreferred(InetAddressAndPort ep)
        {
            return map(ep, PeerInfo::getPreferred, () -> null) != null;
        }

        /**
         * Get preferred IP for given endpoint if it is known. Otherwise this returns given endpoint itself.
         *
         * @param ep endpoint address to check
         * @return Preferred IP for given endpoint if present, otherwise returns given ep
         */
        public InetAddressAndPort getPreferred(InetAddressAndPort ep)
        {
            return map(ep, PeerInfo::getPreferred, () -> ep);
        }

        public void updatePreferredIP(InetAddressAndPort ep, InetAddressAndPort preferred)
        {
            InetAddressAndPort current = getPreferred(ep);
            if (Objects.equals(current, preferred))
                return;

            update(ep, (peerInfo) -> peerInfo.setPreferred(preferred));
        }

        /**
         * Return a map of stored host_ids to IP addresses
         */
        public Map<InetAddressAndPort, UUID> getHostIds()
        {
            return stream().filter(p -> p.getHostId() != null)
                           .collect(Collectors.toMap(PeerInfo::getPeer, NodeInfo::getHostId, (a, b) -> a, HashMap::new));
        }

        public Multimap<InetAddressAndPort, Token> getTokens()
        {
            SetMultimap<InetAddressAndPort, Token> tokenMap = HashMultimap.create();
            peers().stream()
                   .filter(p -> p.getTokens() != null)
                   .forEach(p -> tokenMap.putAll(p.getPeer(), p.getTokens()));
            return tokenMap;
        }

        public void updateTokens(InetAddressAndPort endpoint, Collection<Token> tokens)
        {
            update(endpoint, (peerInfo) -> {
                Collection<Token> savedTokens = peerInfo.getTokens();
                if (savedTokens != null && tokens.containsAll(savedTokens) && tokens.size() == savedTokens.size())
                    return;
                peerInfo.setTokens(tokens);
            });
        }
    }

    /**
     * Holds the default, runtime instance of {@link Nodes}. {@link Nodes} itself doesn'tt have a static reference
     * to "itself" to make testing easier.
     */
    public static final class Instance
    {
        private static volatile Nodes instance;
        private static final Object mutex = new Object();

        public static Nodes nodes()
        {
            Nodes nodes = instance;
            if (nodes == null)
            {
                synchronized (mutex)
                {
                    nodes = instance;
                    if (nodes == null)
                    {
                        logger.debug("Initialising nodes instance at {}", DatabaseDescriptor.getMetadataDirectory().resolve("nodes").toPath());
                        nodes = instance = new Nodes(DatabaseDescriptor.getMetadataDirectory().resolve("nodes").toPath());
                    }
                }
            }
            return nodes;
        }

        @VisibleForTesting
        public static void unsafeSetup(Path directory)
        {
            if (instance != null)
                instance.shutdown();

            instance = new Nodes(directory);
        }

        public static void persistLocalMetadata()
        {
            nodes().local.update((local) -> {
                local.setClusterName(DatabaseDescriptor.getClusterName());
                local.setReleaseVersion(SystemKeyspace.CURRENT_VERSION);
                local.setCqlVersion(QueryProcessor.CQL_VERSION);
                local.setNativeProtocolVersion(String.valueOf(ProtocolVersion.CURRENT.asInt()));
                local.setDataCenter(DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter());
                local.setRack(DatabaseDescriptor.getEndpointSnitch().getLocalRack());
                local.setPartitioner(DatabaseDescriptor.getPartitioner().getClass().getName());
                local.setBroadcastAddressAndPort(FBUtilities.getBroadcastAddressAndPort());
                local.setListenAddressAndPort(FBUtilities.getLocalAddressAndPort());
                local.setNativeTransportAddressAndPort(InetAddressAndPort.getByAddressOverrideDefaults(DatabaseDescriptor.getRpcAddress(),
                                                                                                       DatabaseDescriptor.getNativeTransportPort()));
            }, true);
        }
    }
}
