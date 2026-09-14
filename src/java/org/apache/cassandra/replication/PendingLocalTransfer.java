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

package org.apache.cassandra.replication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Represents a bulk data transfer received on a replica, from completion of streaming into the pending location,
 * through activation when it's made visible to reads. Pending transfers are identified by their streaming plan ID and
 * belong to the {@link CoordinatedTransfer} that streamed them, whose ID can be represented in mutation summaries;
 * they are made live by an {@link ActivationRequest} from that same transfer.
 */
public class PendingLocalTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(PendingLocalTransfer.class);
    private static final String MANIFEST_FILE_NAME = "transfer.manifest";

    private String logPrefix()
    {
        return logPrefix(planId, transferId);
    }

    private static String logPrefix(TimeUUID planId, ShortMutationId transferId)
    {
        return String.format("[PendingLocalTransfer #%s transfer %s]", planId, transferId);
    }

    final TimeUUID planId;
    final TableId tableId;
    final ShortMutationId transferId;
    final Collection<SSTableReader> sstables;
    final long createdAt = currentTimeMillis();
    transient String keyspace;
    transient Range<Token> range;

    volatile boolean activated = false;

    public PendingLocalTransfer(TableId tableId, TimeUUID planId, ShortMutationId transferId, Collection<SSTableReader> sstables)
    {
        Preconditions.checkState(!sstables.isEmpty());
        this.tableId = tableId;
        this.planId = planId;
        this.transferId = Objects.requireNonNull(transferId, "A pending transfer must belong to a coordinated transfer");
        this.sstables = sstables;
        this.keyspace = Objects.requireNonNull(ColumnFamilyStore.getIfExists(tableId)).keyspace.getName();
        this.range = shardRange(keyspace, sstables);
    }

    @VisibleForTesting
    PendingLocalTransfer(TimeUUID planId, ShortMutationId transferId, Collection<SSTableReader> sstables)
    {
        Preconditions.checkState(!sstables.isEmpty());
        this.planId = planId;
        this.transferId = transferId;
        this.tableId = null;
        this.sstables = sstables;
        this.keyspace = null;
        this.range = null;
    }

    /**
     * @return the plan a pending directory was created for, or {@code null} if {@code dir} isn't named after a plan
     */
    static TimeUUID planIdFromDirectory(File dir)
    {
        try
        {
            return TimeUUID.fromString(dir.name());
        }
        catch (IllegalArgumentException e)
        {
            logger.warn("Ignoring pending directory with an unexpected name: {}", dir);
            return null;
        }
    }

    /**
     * Attempts to restore a transfer staged for {@code planId} by a previous run of this Cassandra instance, from the
     * manifests written when the transfer was received.
     *
     * @param cfs    the table the pending directories belong to
     * @param planId the streaming plan the transfer was staged for
     * @param dirs   every {@code pending/<planId>/} directory of {@code cfs}
     * @return the staged pending local transfer if it can be recovered in full, {@code null} otherwise
     */
    static PendingLocalTransfer load(ColumnFamilyStore cfs, TimeUUID planId, Collection<File> dirs)
    {
        try
        {
            return loadInternal(cfs, planId, dirs);
        }
        catch (Throwable t)
        {
            // Recovery runs on startup, right after the crash that may have left a partially written manifest or
            // SSTable behind, so a transfer we cannot read must not keep the node from starting. It has to be streamed
            // again, and its directories are removed when the coordinator fails the transfer.
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("{} Ignoring pending transfer staged in {}: it could not be read, and cannot be activated",
                        logPrefix(planId, null), dirs, t);
            return null;
        }
    }

    private static PendingLocalTransfer loadInternal(ColumnFamilyStore cfs, TimeUUID planId, Collection<File> dirs)
    {
        Manifest manifest = Manifest.load(planId, dirs);
        if (manifest == null)
            return null;

        Map<Descriptor, Set<Component>> byDescriptor = new HashMap<>();
        for (File dir : dirs)
        {
            for (File file : dir.listUnchecked(File::isFile))
            {
                Pair<Descriptor, Component> parsed = SSTable.tryComponentFromFilename(file, cfs.getKeyspaceName(), cfs.getTableName());
                if (parsed != null)
                    byDescriptor.computeIfAbsent(parsed.left, k -> new HashSet<>()).add(parsed.right);
            }
        }

        if (byDescriptor.size() != manifest.sstableCount)
        {
            logger.warn("{} Ignoring pending transfer staged in {}. The manifest expects {} SSTables, but {} were " +
                        "found on disk. Such a transfer cannot be activated, and has to be streamed again",
                        logPrefix(planId, manifest.transferId), dirs, manifest.sstableCount, byDescriptor.size());
            return null;
        }

        Collection<SSTableReader> sstables = new ArrayList<>(byDescriptor.size());
        try
        {
            for (Map.Entry<Descriptor, Set<Component>> entry : byDescriptor.entrySet())
                sstables.add(SSTableReader.open(cfs, entry.getKey(), entry.getValue(), cfs.metadata));

            logger.info("{} Recovered pending transfer with {} SSTables staged in {}",
                        logPrefix(planId, manifest.transferId), sstables.size(), dirs);
            PendingLocalTransfer transfer = new PendingLocalTransfer(cfs.metadata().id, planId, manifest.transferId, sstables);
            transfer.activated = manifest.activated;
            return transfer;
        }
        catch (Throwable t)
        {
            // Don't hold on to the readers we did open when the transfer ends up being rejected
            sstables.forEach(sstable -> sstable.selfRef().release());
            throw t;
        }
    }

    /**
     * Writes the manifest of this transfer into every pending directory it was staged into, so that a restart can
     * recover it in full even when its SSTables span several data directories.
     */
    public void writeManifestFile()
    {
        Manifest manifest = new Manifest(transferId, sstables.size(), activated);
        for (File dir : directories())
            manifest.store(new File(dir, MANIFEST_FILE_NAME));
    }

    private void markActivated()
    {
        try
        {
            writeManifestFile();
        }
        catch (Throwable t)
        {
            logger.warn("{} Could not record the activation of this transfer in its manifests. Should this node restart " +
                        "before they are cleaned up, the transfer may be activated a second time", logPrefix(), t);
        }
    }

    /**
     * @return the distinct pending directories holding the SSTables of this transfer, one per data directory it spans
     */
    Set<File> directories()
    {
        Set<File> directories = new LinkedHashSet<>();
        for (SSTableReader sstable : sstables)
            directories.add(sstable.descriptor.directory);
        return directories;
    }

    /**
     * Manifest of staged transfer for durability on node restart.
     *
     * <p>Layout of the Manifest file
     * <ol>
     *     <li>version: the manifest file version
     *     <li>transferId: the id of the transfer
     *     <li>sstableCount: the number of sstables in the pending transfer
     *     <li>activated: whether the transfer has been made live already
     *     <li>crc32: checksum that covers everything that precedes it
     * </ol>
     */
    private static class Manifest
    {
        private static final int VERSION_1 = 1;
        private static final int CURRENT_VERSION = VERSION_1;
        private static final int CHECKSUM_SIZE = TypeSizes.INT_SIZE;

        final ShortMutationId transferId;
        final int sstableCount;
        final boolean activated;

        Manifest(ShortMutationId transferId, int sstableCount, boolean activated)
        {
            this.transferId = transferId;
            this.sstableCount = sstableCount;
            this.activated = activated;
        }

        /**
         * Reads the manifest shared by the pending directories of a plan.
         *
         * @return the manifest, or {@code null} if none of the directories hold one, or they disagree
         */
        static Manifest load(TimeUUID planId, Collection<File> dirs)
        {
            Manifest manifest = null;
            for (File dir : dirs)
            {
                File file = new File(dir, MANIFEST_FILE_NAME);
                if (!file.exists())
                    continue;

                Manifest read = read(file);
                if (manifest == null)
                {
                    manifest = read;
                }
                else if (!manifest.equals(read))
                {
                    logger.warn("{} Ignoring pending transfer staged in {}: its manifests disagree ({} != {})",
                                logPrefix(planId, manifest.transferId), dirs, manifest, read);
                    return null;
                }
            }

            if (manifest == null)
                logger.warn("{} Ignoring pending transfer staged in {} with no manifest. SSTables from this pending " +
                            "transfer cannot be activated", logPrefix(planId, null), dirs);

            return manifest;
        }

        private static Manifest read(File file)
        {
            byte[] contents = new byte[Ints.checkedCast(file.length())];
            try (FileInputStreamPlus in = file.newInputStream())
            {
                in.readFully(contents);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Could not read " + file, e);
            }

            int length = contents.length - CHECKSUM_SIZE;
            if (length <= 0)
                throw new IllegalStateException(String.format("%s only holds %d bytes. Manifest file was not written completely",
                                                             file, contents.length));

            int checksum = ByteBufferUtil.toInt(ByteBuffer.wrap(contents, length, CHECKSUM_SIZE));
            if (checksum != checksum(contents, length))
                throw new IllegalStateException(String.format("%s does not match its checksum. Manifest file was not written completely", file));

            try (DataInputBuffer in = new DataInputBuffer(ByteBuffer.wrap(contents, 0, length), false))
            {
                int version = in.readInt();
                if (version > CURRENT_VERSION)
                    throw new IllegalStateException(String.format("%s was written with an unsupported manifest version %d",
                                                                 file, version));

                return new Manifest(ShortMutationId.serializer.deserialize(in), in.readInt(), in.readBoolean());
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Could not read " + file, e);
            }
        }

        void store(File file)
        {
            byte[] contents = serialize();
            try (FileOutputStreamPlus out = file.newOutputStream(File.WriteMode.OVERWRITE))
            {
                out.write(contents);
                out.writeInt(checksum(contents, contents.length));
                out.flush();
                // The manifest is only of any use if it survives the crash it is written for
                out.sync();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Could not write " + file, e);
            }

            SyncUtil.trySyncDir(file.parent());
        }

        private byte[] serialize()
        {
            long size = TypeSizes.INT_SIZE
                        + ShortMutationId.serializer.serializedSize(transferId)
                        + TypeSizes.INT_SIZE
                        + TypeSizes.BOOL_SIZE;
            try (DataOutputBuffer out = new DataOutputBuffer(Ints.checkedCast(size)))
            {
                out.writeInt(CURRENT_VERSION);
                ShortMutationId.serializer.serialize(transferId, out);
                out.writeInt(sstableCount);
                out.writeBoolean(activated);
                return out.toByteArray();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Could not serialize " + this, e);
            }
        }

        private static int checksum(byte[] contents, int length)
        {
            return (int) ChecksumType.CRC32.of(contents, 0, length);
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            Manifest manifest = (Manifest) o;
            return sstableCount == manifest.sstableCount
                   && activated == manifest.activated
                   && Objects.equals(transferId, manifest.transferId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(transferId, sstableCount, activated);
        }

        @Override
        public String toString()
        {
            return "Manifest{transferId=" + transferId + ", sstableCount=" + sstableCount + ", activated=" + activated + '}';
        }
    }

    /**
     * Pending transfers should be within a single shard, which are aligned to natural ranges.
     * See ({@link MutationTrackingService.KeyspaceShards#make}).
     */
    private static Range<Token> shardRange(String keyspace, Collection<SSTableReader> sstables)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        ReplicaGroups writes = cm.placements.get(Keyspace.open(keyspace).getMetadata().params.replication).writes;
        Range<Token> range = null;
        for (SSTableReader sstable : sstables)
        {
            if (range == null)
            {
                Token first = sstable.getFirst().getToken();
                range = writes.forRange(first).range();
            }
            else
            {
                AbstractBounds<Token> bounds = sstable.getBounds();
                Preconditions.checkState(!range.isTrulyWrapAround());
                Preconditions.checkState(range.contains(bounds.left));
                Preconditions.checkState(range.contains(bounds.right));
            }
        }

        Preconditions.checkNotNull(range);
        return range;
    }

    private boolean isFullReplica()
    {
        ClusterMetadata cm = ClusterMetadata.current();
        Keyspace ks = Keyspace.open(keyspace);
        ReplicaGroups writes = cm.placements.get(ks.getMetadata().params.replication).writes;
        EndpointsForRange replicas = writes.forRange(range.right).get();
        return replicas.containsSelf() && replicas.selfIfPresent().isFull();
    }

    /**
     * Safely move a transfer into the live set. This must be crash-safe, and the primary invariant we need to
     * preserve is a transfer is only added to the live set iff the transfer ID is present in its mutation summaries.
     * <p>
     * We don't validate checksums here, mostly because a transfer can be activated during a read, if one replica
     * missed the {@link ActivationRequest}. Transfers should not be pending for very long, and should be protected by
     * internode integrity checks provided by TLS.
     * <p>
     * Synchronized to prevent a single activation from running multiple times if requested during read reconciliation
     * and in the background via {@link ActiveLogReconciler}.
     */
    public synchronized boolean activate(ActivationRequest request, Bounds<Token> bounds)
    {
        if (activated)
            return false;

        Preconditions.checkState(transferId.equals(request.transferId),
                                 "%s Cannot activate a transfer staged for %s with an activation for %s (%s)",
                                 logPrefix(), transferId, request.transferId, request);
        Preconditions.checkState(isFullReplica());

        long startedActivation = currentTimeMillis();
        logger.info("{} Activating transfer {}, {} ms since pending", logPrefix(), this, startedActivation - createdAt);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        Preconditions.checkNotNull(cfs);
        Preconditions.checkState(!sstables.isEmpty());

        if (request.isPrepare())
        {
            logger.info("{} Not adding SSTables to live set for dryRun {}", logPrefix(), request);
            return false;
        }

        // Modify SSTables metadata to durably set transfer ID before importing
        ImmutableCoordinatorLogOffsets logOffsets =
            new ImmutableCoordinatorLogOffsets.Builder().addTransfer(request.transferId, bounds).build();

        // Ensure no lingering mutation IDs, only activation IDs
        for (SSTableReader sstable : sstables)
        {
            try
            {
                sstable.mutateCoordinatorLogOffsetsAndReload(logOffsets);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            Preconditions.checkState(sstable.getCoordinatorLogOffsets().mutations().isEmpty());
            ActivatedTransfers transfers = sstable.getCoordinatorLogOffsets().transfers();
            Preconditions.checkState(!transfers.isEmpty());
        }

        File dst = cfs.getDirectories().getDirectoryForNewSSTables();

        // Retain the original SSTables in pending/ dir on the coordinator, so future streams can get the originals, and
        // we don't need to isolate activated SSTables during compaction
        boolean isCoordinator = request.transferId.hostId == ClusterMetadata.current().myNodeId().id();
        logger.debug("{} {} pending SSTables for activation to {}", isCoordinator ? "Copying" : "Moving", logPrefix(), dst);

        dst.createFileIfNotExists();
        Collection<SSTableReader> moved = new ArrayList<>(sstables.size());
        for (SSTableReader sstable : sstables)
            moved.add(SSTableReader.moveAndOpenSSTable(cfs, sstable.descriptor, cfs.getUniqueDescriptorFor(sstable.descriptor, dst), sstable.getComponents(), isCoordinator));

        // Add all SSTables atomically
        cfs.getTracker().addSSTablesTracked(moved);
        activated = true;
        // The SSTables are live now: record that in the manifests so a restart doesn't activate this transfer again
        markActivated();

        Consumer<Integer> onRowCacheInvalidation = invalidatedKeys -> {
            logger.debug("{} Invalidated {} row cache entries on table {}.{} after activating transfer",
                    logPrefix(), invalidatedKeys, cfs.getKeyspaceName(), cfs.getTableName());
        };
        Consumer<Integer> onCounterCacheInvalidation = invalidatedKeys -> {
            logger.debug("{} Invalidated {} counter cache entries on table {}.{} after activating transfer",
                    logPrefix(), invalidatedKeys, cfs.getKeyspaceName(), cfs.getTableName());
        };
        cfs.invalidateRowAndCounterCache(moved, onRowCacheInvalidation, onCounterCacheInvalidation);

        long finishedActivation = currentTimeMillis();
        logger.info("{} Finished activating transfer {} in {} ms", logPrefix(), this, finishedActivation - startedActivation);

        TransferTrackingService.instance().scheduleCleanup();
        return true;
    }

    @Override
    public String toString()
    {
        return "PendingLocalTransfer{" +
               "activated=" + activated +
               ", transferId=" + transferId +
               ", range=" + range +
               ", keyspace='" + keyspace + '\'' +
               ", createdAt=" + createdAt +
               ", sstables=" + sstables +
               ", tableId=" + tableId +
               ", planId=" + planId +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        PendingLocalTransfer transfer = (PendingLocalTransfer) o;
        return Objects.equals(planId, transfer.planId) && Objects.equals(tableId, transfer.tableId) && Objects.equals(sstables, transfer.sstables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(planId, tableId, sstables);
    }
}
