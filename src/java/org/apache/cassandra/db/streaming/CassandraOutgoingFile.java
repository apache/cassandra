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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * used to transfer the part(or whole) of a SSTable data file
 */
public class CassandraOutgoingFile implements OutgoingStream
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOutgoingFile.class);

    /**
     * The lowest release a node must be on to be sent a slice. BIG {@code pa} and BTI {@code ea} are 6.0's sstable
     * versions and {@code pb}/{@code eb} -- the ones that can carry {@code StatsMetadata#hasUnindexedRegions} -- are
     * 7.0's, so a peer below 7.0 is a peer that will read a slice with the marker ignored. The sstable version bump
     * alone does not protect against that: {@code Version.isCompatibleForStreaming()} is
     * {@code version.charAt(0) == current_version.charAt(0)}, so a 6.0 node ACCEPTS a {@code pb} file, reads it with
     * {@code pa} semantics -- scanning linearly past the interior dead regions -- and then, because
     * {@code CassandraEntireSSTableStreamReader} unconditionally calls {@code mutate()}, rewrites its Statistics.db
     * and erases the marker for good.
     */
    private static final CassandraVersion MIN_SLICE_PEER_VERSION = new CassandraVersion("7.0").familyLowerBound.get();

    private final Ref<SSTableReader> ref;
    private final long estimatedKeys;
    private final List<SSTableReader.PartitionPositionBounds> sections;
    private final String filename;
    private final boolean shouldStreamEntireSSTable;
    /** Set when the sections do not cover the whole sstable but can still go out as a synthesised slice. */
    private final ZeroCopySSTableSlice.Plan slicePlan;
    /** Expected slice component sizes, for progress totals only; never goes on the wire. Null unless sliced. */
    private final ComponentManifest estimatedSliceManifest;
    private final StreamOperation operation;
    private final CassandraStreamHeader header;
    private final List<Range<Token>> ranges;

    public CassandraOutgoingFile(StreamOperation operation, Ref<SSTableReader> ref,
                                 List<SSTableReader.PartitionPositionBounds> sections, List<Range<Token>> normalizedRanges,
                                 long estimatedKeys)
    {
        Preconditions.checkNotNull(ref.get());
        Range.assertNormalized(normalizedRanges);
        this.operation = operation;
        this.ref = ref;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.ranges = normalizedRanges;

        SSTableReader sstable = ref.get();

        this.filename = sstable.getFilename();
        this.shouldStreamEntireSSTable = computeShouldStreamEntireSSTables();
        this.slicePlan = shouldStreamEntireSSTable ? null : computeSlicePlan();
        this.estimatedSliceManifest = isSliced() ? estimateSliceManifest(sstable, slicePlan) : null;

        // Describes the fallback stream, never the slice: a slice's manifest is measured and its first key is its own,
        // so writeSlice builds its own header. The receiver dispatches on isEntireSSTable
        // (CassandraIncomingFile.read), so an entire-sstable header in front of a partition-by-partition stream would
        // be misparsed.
        ComponentManifest manifest = ComponentManifest.create(sstable);
        this.header = makeHeader(sstable, operation, sections, estimatedKeys, shouldStreamEntireSSTable, manifest,
                                 sstable.getFirst());
    }

    private static CassandraStreamHeader makeHeader(SSTableReader sstable,
                                                    StreamOperation operation,
                                                    List<SSTableReader.PartitionPositionBounds> sections,
                                                    long estimatedKeys,
                                                    boolean isEntireSSTable,
                                                    ComponentManifest manifest,
                                                    DecoratedKey firstKey)
    {
        CompressionInfo compressionInfo = sstable.compression
                ? CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections)
                : null;

        return CassandraStreamHeader.builder()
                                    .withSSTableVersion(sstable.descriptor.version)
                                    .withSSTableLevel(operation.keepSSTableLevel() ? sstable.getSSTableLevel() : 0)
                                    .withEstimatedKeys(estimatedKeys)
                                    .withSections(sections)
                                    .withCompressionInfo(compressionInfo)
                                    .withSerializationHeader(sstable.header.toComponent())
                                    .isEntireSSTable(isEntireSSTable)
                                    .withComponentManifest(manifest)
                                    .withFirstKey(firstKey)
                                    .withTableId(sstable.metadata().id)
                                    .build();
    }

    @VisibleForTesting
    public static CassandraOutgoingFile fromStream(OutgoingStream stream)
    {
        Preconditions.checkArgument(stream instanceof CassandraOutgoingFile);
        return (CassandraOutgoingFile) stream;
    }

    @VisibleForTesting
    public Ref<SSTableReader> getRef()
    {
        return ref;
    }

    @Override
    public String getName()
    {
        return filename;
    }

    @Override
    public long getEstimatedSize()
    {
        // header is the fallback's; for a slice it would report the row-by-row size, not the components sent.
        return isSliced() ? estimatedSliceManifest.totalSize() : header.size();
    }

    @Override
    public TableId getTableId()
    {
        return ref.get().metadata().id;
    }

    @Override
    public int getNumFiles()
    {
        // This count is a PROMISE made before the transfer: StreamTransferTask sums it into the peer's
        // StreamSummary, whose StreamReceiveTask completes only on remoteStreamsReceived == totalStreams
        // (StreamReceiveTask.received) -- counting manifest components for an entire-sstable header, 1 for a
        // row-by-row one (CassandraIncomingFile.numFiles). So sending anything but the promised slice hangs the peer
        // for ever with its data never made live: writeSlice cannot fall back, and the estimate stands in here.
        if (isSliced())
            return estimatedSliceManifest.components().size();

        return shouldStreamEntireSSTable ? header.componentManifest.components().size() : 1;
    }

    @Override
    public List<Range<Token>> ranges()
    {
        return ranges;
    }

    @Override
    public long getRepairedAt()
    {
        return ref.get().getRepairedAt();
    }

    @Override
    public TimeUUID getPendingRepair()
    {
        return ref.get().getPendingRepair();
    }

    @Override
    public void write(StreamSession session, StreamingDataOutputPlus out, int version) throws IOException
    {
        SSTableReader sstable = ref.get();

        if (shouldStreamEntireSSTable)
        {
            // Acquire lock to avoid concurrent sstable component mutation because of stats update or index summary
            // redistribution, otherwise file sizes recorded in component manifest will be different from actual
            // file sizes.
            // Recreate the latest manifest and hard links for mutatable components in case they are modified.
            try (ComponentContext context = sstable.runWithLock(ignored -> ComponentContext.create(sstable)))
            {
                CassandraStreamHeader current = makeHeader(sstable, operation, sections, estimatedKeys, true,
                                                           context.manifest(), sstable.getFirst());
                CassandraStreamHeader.serializer.serialize(current, out, version);
                out.flush();

                CassandraEntireSSTableStreamWriter writer = new CassandraEntireSSTableStreamWriter(sstable, session, context);
                writer.write(out);
            }
        }
        // A slice also uses the entire-sstable protocol, and getNumFiles() has already promised its file count.
        else if (isSliced())
        {
            writeSlice(sstable, session, out, version);
        }
        else
        {
            // legacy streaming is not affected by stats metadata mutation and index sumary redistribution
            CassandraStreamHeader.serializer.serialize(header, out, version);
            out.flush();

            CassandraStreamWriter writer = header.isCompressed() ?
                                           new CassandraCompressedStreamWriter(sstable, header, session) :
                                           new CassandraStreamWriter(sstable, header, session);
            writer.write(out);
        }
    }

    /**
     * Send the planned slice: synthesise every component but Data.db for the chunk run covering the requested
     * sections, then stream those plus the run itself as if they were a whole sstable.
     * <p>
     * THIS CANNOT FALL BACK, even though synthesis can fail with nothing yet written to {@code out}:
     * {@link #getNumFiles()} has already promised the peer this slice's component count, a row-by-row stream makes
     * the receiver count 1, and {@code StreamReceiveTask.received} completes on exact equality, so the peer's task
     * would hang for ever with what it had written correctly never made live. Predictable refusals (format,
     * storage-attached indexes, sstable version, peer version, compression dictionary, legacy counter shards,
     * dead-space ratio, either kill switch) are all made by {@link #computeSlicePlan()} before the count is promised;
     * only IO error and genuine corruption reach here, which the row-by-row path would very likely not survive either,
     * so they propagate and fail the session loudly.
     * <p>
     * That makes every ORDINARY condition that can first show up here a bug, and the two that could have been left to
     * are closed rather than moved. Which components exist no longer depends on anything mutable -- FILTER follows
     * {@code Plan.writesFilter()}, frozen when the plan was made, so an {@code ALTER TABLE bloom_filter_fp_chance}
     * crossing 1.0 in between can no longer make the promise and the stream disagree. Free space is still only checked
     * before the promise, and a directory that fills in between is still fatal: the honest alternative would be to
     * synthesise in the constructor, before the count is promised, and that would hold every sstable's index
     * components on disk for the length of the session and move thousands of index passes into the prepare phase --
     * strictly worse than the failure it prevents. The 2x margin in {@link #computeSlicePlan()} is what buys against
     * it.
     */
    private void writeSlice(SSTableReader sstable, StreamSession session, StreamingDataOutputPlus out, int version)
    throws IOException
    {
        Descriptor target = null;
        ZeroCopySSTableSlice.Slice slice;
        ComponentManifest manifest;
        Map<Component, File> synthesised = new HashMap<>(ZeroCopySSTableSlice.ALL_SYNTHESISED.size());
        List<ComponentContext.ByteRange> dataRanges = new ArrayList<>(slicePlan.runs.size());
        try
        {
            target = ZeroCopySSTableSlice.newDescriptor(sstable);
            // write() takes the parent's lock for the one step that needs it, the read of its Statistics.db; see there
            // for why the index passes, the filter build and the summary build must not be under it.
            slice = ZeroCopySSTableSlice.write(sstable, slicePlan, target);
            // Off the live-sstable naming and onto streaming temporaries before the long wait on the socket: see
            // ZeroCopySSTableSlice.toStreamingTemporaries.
            synthesised.putAll(ZeroCopySSTableSlice.toStreamingTemporaries(sstable.descriptor, slice));

            Map<Component, Long> sizes = new HashMap<>(slice.components.size() + 1);
            for (Component component : slice.components)
                sizes.put(component, slice.sizes.get(component));
            // The only component that is not a file of the slice's own: it is byte ranges of the parent's.
            for (ZeroCopySSTableSlice.Run run : slicePlan.runs)
                dataRanges.add(new ComponentContext.ByteRange(run.srcStart, run.physicalBytes()));
            sizes.put(Components.DATA, slicePlan.physicalBytes);
            manifest = ComponentManifest.ordered(sstable.descriptor, sizes);
        }
        catch (Throwable t)
        {
            // Only orphan window: everything that can throw is above, and once the ComponentContext below exists it
            // takes over deleting. Both names are tried, since a file may be under either side of the rename.
            if (target != null)
                ZeroCopySSTableSlice.delete(target, ZeroCopySSTableSlice.ALL_SYNTHESISED);
            deleteQuietly(synthesised.values());
            // These calls write into a live data directory: swallowing their FSError skips disk_failure_policy.
            JVMStabilityInspector.inspectThrowable(t);
            StreamingMetrics.slicedZeroCopyStreamsFailed.inc();
            logger.error("[Stream #{}] Failed slicing {} for {}; failing the stream, because getNumFiles() has" +
                         " already promised {} files to the peer and a fallback would leave its receive task" +
                         " permanently short", session.planId(), sstable.getFilename(), session.peer,
                         getNumFiles(), t);
            if (t instanceof IOException)
                throw (IOException) t;
            throw Throwables.throwAsUncheckedException(t);
        }

        // Both sets now come from Plan.components() and Plan.writesFilter(), so this is an invariant and not an
        // expected condition -- but a mismatch would hang the peer for ever, so it stays, and fails diagnosably.
        if (manifest.components().size() != getNumFiles())
        {
            deleteQuietly(synthesised.values());
            throw new IllegalStateException(String.format(
                "Slice of %s measured %d components (%s) but %d were promised to the peer (%s); refusing to send a" +
                " stream its receive task could never complete",
                sstable.getFilename(), manifest.components().size(), manifest.components(),
                getNumFiles(), estimatedSliceManifest.components()));
        }

        try (ComponentContext context = ComponentContext.slice(synthesised, dataRanges, manifest))
        {
            // The receiver picks a data directory from the first key and the sstable's identity from the manifest,
            // so both must describe the SLICE, not the parent. partitionCount is exact here, unlike the estimate.
            CassandraStreamHeader current = makeHeader(sstable, operation, sections, slice.partitionCount, true,
                                                       context.manifest(), slice.first);
            CassandraStreamHeader.serializer.serialize(current, out, version);
            out.flush();

            logger.debug("[Stream #{}] Streaming slice of {} to {}: {}, plan {}",
                         session.planId(), sstable.getFilename(), session.peer, slice, slicePlan);
            StreamingMetrics.slicedZeroCopyStreamsOut.inc();
            StreamingMetrics.slicedZeroCopyStreamsDeadBytes.inc(slicePlan.deadBytes + slicePlan.suffixBytes);

            new CassandraEntireSSTableStreamWriter(sstable, session, context).write(out);
        }
    }

    /** Cleanup on a path that is already failing, so it must not replace the failure with its own. */
    private static void deleteQuietly(Iterable<File> files)
    {
        for (File file : files)
        {
            try
            {
                file.deleteIfExists();
            }
            catch (Throwable t)
            {
                logger.warn("Failed removing streaming temporary {}", file, t);
            }
        }
    }

    @VisibleForTesting
    public boolean computeShouldStreamEntireSSTables()
    {
        // don't stream if full sstable transfers are disabled, legacy counter shards are present,
        // or sstable uses old bloom filter format (pre-4.0) which is incompatible with zero-copy streaming
        if (!DatabaseDescriptor.streamEntireSSTables() ||
            ref.get().getSSTableMetadata().hasLegacyCounterShards ||
            ref.get().descriptor.version.hasOldBfFormat())
            return false;

        return contained(sections, ref.get());
    }

    /**
     * Whether sections that do NOT cover the whole sstable can still go through the entire-sstable protocol, as a
     * verbatim chunk run with synthesised components. Arithmetic over the compression metadata, plus the two questions
     * that are not about this sstable at all -- can the cluster read a slice, and is there room to write one; no index
     * read yet. EVERY refusal has to be made here, before {@link #getNumFiles()} promises the peer a count.
     */
    @VisibleForTesting
    ZeroCopySSTableSlice.Plan computeSlicePlan()
    {
        // Same protocol, same rate limiter, so the same switch governs it.
        if (!DatabaseDescriptor.streamEntireSSTables() || !DatabaseDescriptor.getZeroCopyPartialStreamEnabled())
            return null;

        // No configuration on either node changes this one; it stands until the cluster is upgraded.
        if (!clusterUnderstandsSlices())
        {
            StreamingMetrics.countSliceRefusedAsUnsliceable();
            return null;
        }

        ZeroCopySSTableSlice.Plan plan =
            ZeroCopySSTableSlice.plan(ref.get(), sections, DatabaseDescriptor.getZeroCopyPartialStreamMaxDeadSpaceRatio());

        if (!plan.isEligible())
        {
            logger.debug("Not streaming {} as a zero-copy slice: {}", filename, plan.reason);
            // DEAD_SPACE is the only reason zero_copy_partial_stream_max_dead_space_ratio can take back; every other
            // one is a property of the sstable, the request shape or the cluster.
            if (plan.reason == ZeroCopySSTableSlice.Reason.DEAD_SPACE)
                StreamingMetrics.countSliceRefusedByDeadSpaceRatio();
            else
                StreamingMetrics.countSliceRefusedAsUnsliceable();
            return null;
        }

        // Synthesis WRITES into this node's own data directory and writeSlice cannot fall back if that fails, so
        // refuse while refusing is free: a full disk is the one failure the row-by-row path (writing nothing) would
        // have survived. 2x margin as these are estimates and other writers exist; a later fill is a loud IO error.
        SSTableReader sstable = ref.get();
        long synthesisedBytes = estimateSynthesisedBytes(sstable, plan);
        long usable = PathUtils.tryGetSpace(sstable.descriptor.directory.toPath(), FileStore::getUsableSpace);
        if (usable > 0 && usable < 2 * synthesisedBytes)
        {
            logger.info("Not streaming {} as a zero-copy slice: synthesising its components needs about {} bytes in" +
                        " {} and only {} are usable; falling back to partition-by-partition streaming",
                        filename, synthesisedBytes, sstable.descriptor.directory, usable);
            StreamingMetrics.countSliceRefusedAsUnsliceable();
            return null;
        }

        return plan;
    }

    /**
     * Whether a slice can be sent at all, which is a property of the CLUSTER and not of this sstable: a peer below
     * {@link #MIN_SLICE_PEER_VERSION} accepts a slice, ignores its {@code hasUnindexedRegions} marker and then erases
     * it, so a slice must not be planned for one. The right question is the PEER's version, and the peer is not
     * reachable here -- {@link CassandraOutgoingFile} is built before the session hands it to a
     * {@code StreamTransferTask} -- so this asks the strictly stronger question the cluster metadata can answer
     * without plumbing: whether EVERY node is new enough. That over-refuses for the length of an upgrade, which costs
     * only the row-by-row path; the alternative is unrecoverable, since {@code getNumFiles()} promises the slice's
     * component count before {@code write()} ever sees a session.
     * <p>
     * Unavailable cluster metadata (offline tooling, {@code sstableloader}) is also a refusal: not knowing is not the
     * same as knowing it is safe.
     */
    private static boolean clusterUnderstandsSlices()
    {
        CassandraVersion minVersion;
        try
        {
            minVersion = ClusterMetadata.current().directory.clusterMinVersion.cassandraVersion;
        }
        catch (Throwable t)
        {
            logger.debug("Not streaming zero-copy slices: the cluster's minimum version cannot be determined", t);
            return false;
        }

        if (minVersion == null || minVersion.compareTo(MIN_SLICE_PEER_VERSION) < 0)
        {
            logger.debug("Not streaming zero-copy slices: the cluster's minimum version is {}, and a peer below {}" +
                         " would ignore and then erase the unindexed-regions marker a slice depends on",
                         minVersion, MIN_SLICE_PEER_VERSION);
            return false;
        }

        return true;
    }

    @VisibleForTesting
    public boolean isSliced()
    {
        return slicePlan != null;
    }

    @VisibleForTesting
    public ZeroCopySSTableSlice.Plan slicePlan()
    {
        return slicePlan;
    }

    /**
     * The component sizes a slice is expected to have, for the progress totals a stream plan is assembled from.
     * Data.db is exact; every other component -- Statistics.db INCLUDED, see
     * {@link #estimateSliceComponentSizes} -- is the parent's scaled by the fraction being sent, since measuring them
     * means an Index.db pass and this runs per sstable before the peer has even been asked. {@code bytes_to_send} is
     * therefore approximate, which is harmless: the receiver sizes from the measured manifest {@link #writeSlice} puts
     * on the wire, and {@code StreamingState.progress} clamps at 0.99 until the session ends. Which components are
     * named IS exact ({@code files_to_send} is a count), so FILTER follows the plan's condition, not the parent's
     * files.
     */
    private static ComponentManifest estimateSliceManifest(SSTableReader sstable, ZeroCopySSTableSlice.Plan plan)
    {
        Map<Component, Long> sizes = estimateSliceComponentSizes(sstable, plan, true);
        sizes.put(Components.DATA, plan.physicalBytes);
        return ComponentManifest.ordered(sstable.descriptor, sizes);
    }

    /**
     * Bytes {@link #writeSlice} is expected to WRITE into the parent's data directory, for the pre-promise disk-space
     * guard. Deliberately not the total of {@link #estimateSliceManifest}: Statistics.db is charged in full, because
     * one whole one is what the writer really produces, whereas the manifest's job is to be honest about what this one
     * slice contributes to a total. It is still only an estimate -- the caller's 2x margin is the actual safety.
     */
    private static long estimateSynthesisedBytes(SSTableReader sstable, ZeroCopySSTableSlice.Plan plan)
    {
        long total = 0;
        for (long size : estimateSliceComponentSizes(sstable, plan, false).values())
            total += size;
        return total;
    }

    /**
     * @param proRataStats how to charge Statistics.db, which is the one component that does not shrink with the range:
     *                     its contents are inherited from the parent almost verbatim, so the writer produces about the
     *                     parent's size however narrow the slice. Billing every slice that full size made
     *                     {@code bytes_to_send} and {@code TotalOutgoingBytes} report a multiple of a file the sstable
     *                     only has one of, because one sstable is sliced once per repair session and once per
     *                     bootstrapping peer whose ranges it overlaps. True bills each slice its share, so the shares
     *                     over one sstable sum to about the file; false is the writer's-eye view a disk reservation
     *                     needs.
     */
    private static Map<Component, Long> estimateSliceComponentSizes(SSTableReader sstable,
                                                                    ZeroCopySSTableSlice.Plan plan,
                                                                    boolean proRataStats)
    {
        double fraction = sstable.uncompressedLength() <= 0
                          ? 1.0
                          : Math.min(1.0, (double) plan.usefulBytes / sstable.uncompressedLength());

        Map<Component, Long> sizes = new HashMap<>();
        for (Component component : plan.components())
        {
            // fp chance 1.0 means AlwaysPresentFilter, which has nothing to serialise, so the writer emits none. From
            // the plan, so this and the writer cannot disagree if the table is altered in between.
            if (component == Components.FILTER && !plan.writesFilter())
                continue;

            long parentSize = sstable.descriptor.fileFor(component).length();
            // CRC.db is four bytes per cell, exactly.
            long size;
            if (component == Components.CRC)
                size = 4 + 4 * plan.cellCount();
            else if (component == Components.STATS && !proRataStats)
                size = parentSize;
            else
                size = (long) (parentSize * fraction);
            sizes.put(component, Math.max(1, size));
        }
        return sizes;
    }

    @VisibleForTesting
    public boolean contained(List<SSTableReader.PartitionPositionBounds> sections, SSTableReader sstable)
    {
        if (sections == null || sections.isEmpty())
            return false;

        // Entire-SSTable streaming copies every component file verbatim, so it is eligible whenever the sections
        // cover all LIVE data, not only when the byte span equals the physical data length: a zero-copy split child
        // can carry a dead prefix -- the head of a boundary compression chunk, copied verbatim, that no read path
        // ever enters -- and getPositionsForRanges() starts the first section past it. For an ordinary sstable
        // firstPosition == 0, reducing this to transferLength == uncompressedLength; getPosition() applies a
        // MOVED_START reader's moved start, matching getPositionsForFullRange() for one of those.
        long firstPosition = sstable.getPosition(sstable.getFirst().getToken().minKeyBound(), SSTableReader.Operator.GT);
        if (firstPosition < 0)  // nothing at or after the first key; fall back to the whole-file comparison
            firstPosition = 0;

        long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
        return transferLength == sstable.uncompressedLength() - firstPosition;
    }

    @Override
    public void finish()
    {
        ref.release();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraOutgoingFile that = (CassandraOutgoingFile) o;
        return estimatedKeys == that.estimatedKeys &&
               Objects.equals(ref, that.ref) &&
               Objects.equals(sections, that.sections);
    }

    public int hashCode()
    {
        return Objects.hash(ref, estimatedKeys, sections);
    }

    @Override
    public String toString()
    {
        return "CassandraOutgoingFile{" + filename + '}';
    }
}
