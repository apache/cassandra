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
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * used to transfer the part(or whole) of a SSTable data file
 */
public class CassandraOutgoingFile implements OutgoingStream
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOutgoingFile.class);

    private final Ref<SSTableReader> ref;
    private final long estimatedKeys;
    private final List<SSTableReader.PartitionPositionBounds> sections;
    private final String filename;
    private final boolean shouldStreamEntireSSTable;
    /**
     * Set when the sections do not cover the whole sstable but can still be sent through the entire-sstable
     * protocol as a synthesised slice; null otherwise. See {@link ZeroCopySSTableSlice}.
     */
    private final ZeroCopySSTableSlice.Plan slicePlan;
    /**
     * The component sizes a slice is expected to have, for a stream plan's progress totals only. Null unless
     * {@link #isSliced()}. This never goes on the wire; see {@link #estimateSliceManifest}.
     */
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

        // This header describes the stream this file falls back to, never the slice. A slice's header cannot be
        // built before the slice exists -- its manifest is measured, and its first key is its own rather than the
        // parent's -- and writeSlice can still give up and fall back after this point. The receiver dispatches on
        // isEntireSSTable (CassandraIncomingFile.read), so a header claiming an entire sstable in front of a
        // partition-by-partition stream would be misparsed. The entire-sstable header for a slice is therefore
        // built inside writeSlice, once there is something to describe.
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
        // header is the fallback's, so for a slice it would report the row-by-row transfer size rather than the
        // component bytes actually going out over the entire-sstable protocol.
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
        // Keyed off what is actually going to be sent as an entire sstable, which for a slice is not what
        // header says: a slice sends one file per manifest component, but its manifest is not known until it has
        // been synthesised, so the estimate stands in for the count.
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
        // A slice goes through the entire-sstable protocol too; writeSlice returns false only if it gave up
        // before writing anything, in which case this falls through to the row-by-row path below.
        else if (!isSliced() || !writeSlice(sstable, session, out, version))
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
     * All of the work that can fail happens BEFORE the first byte reaches {@code out}, so a failure here is
     * recoverable: nothing has been written, and the caller can fall back to the row-by-row path, which has no
     * preconditions to fail. That is deliberately true even of failures that look like corruption -- a stream is
     * not the place to refuse service over one -- but they are logged at WARN because that is what they are.
     *
     * @return false if nothing was written and the caller must fall back
     */
    private boolean writeSlice(SSTableReader sstable, StreamSession session, StreamingDataOutputPlus out, int version)
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
            Descriptor sliceDescriptor = target;
            // The slice inherits the parent's Statistics.db, which a stats mutation or an index summary
            // redistribution can rewrite underneath it; this is the lock those take.
            slice = sstable.runWithLock(ignored -> ZeroCopySSTableSlice.write(sstable, slicePlan, sliceDescriptor));

            Map<Component, Long> sizes = new HashMap<>(slice.components.size() + 1);
            for (Component component : slice.components)
            {
                synthesised.put(component, slice.descriptor.fileFor(component));
                sizes.put(component, slice.sizes.get(component));
            }
            // The only component that is not a file of the slice's own: it is byte ranges of the parent's.
            for (ZeroCopySSTableSlice.Run run : slicePlan.runs)
                dataRanges.add(new ComponentContext.ByteRange(run.srcStart, run.physicalBytes()));
            sizes.put(Components.DATA, slicePlan.physicalBytes);
            manifest = ComponentManifest.ordered(sstable.descriptor, sizes);
        }
        catch (Throwable t)
        {
            // Everything that can throw is in here, so this is the only place a synthesised file can be orphaned
            // before the ComponentContext below takes over deleting them.
            if (target != null)
                ZeroCopySSTableSlice.delete(target, ZeroCopySSTableSlice.ALL_SYNTHESISED);
            logger.warn("[Stream #{}] Failed slicing {} for {}, falling back to partition-by-partition streaming",
                        session.planId(), sstable.getFilename(), session.peer, t);
            StreamingMetrics.slicedZeroCopyStreamsFailed.inc();
            return false;
        }

        try (ComponentContext context = ComponentContext.slice(synthesised, dataRanges, manifest))
        {
            // The receiver picks a data directory from the first key and takes the sstable's identity from the
            // manifest, so both have to describe the SLICE rather than the parent it was cut from. The partition
            // count is exact here, unlike the estimate the plan was assembled with.
            CassandraStreamHeader current = makeHeader(sstable, operation, sections, slice.partitionCount, true,
                                                       context.manifest(), slice.first);
            CassandraStreamHeader.serializer.serialize(current, out, version);
            out.flush();

            logger.debug("[Stream #{}] Streaming slice of {} to {}: {}, plan {}",
                         session.planId(), sstable.getFilename(), session.peer, slice, slicePlan);
            StreamingMetrics.slicedZeroCopyStreamsOut.inc();
            StreamingMetrics.slicedZeroCopyStreamDeadBytes.inc(slicePlan.deadBytes);

            new CassandraEntireSSTableStreamWriter(sstable, session, context).write(out);
        }

        return true;
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
     * Whether the sections that do NOT cover the whole sstable can still go through the entire-sstable protocol,
     * as a verbatim compression chunk run with synthesised components. Pure arithmetic over the compression
     * metadata; the index is not read until the stream is actually written.
     */
    @VisibleForTesting
    ZeroCopySSTableSlice.Plan computeSlicePlan()
    {
        // Same protocol, same rate limiter, so the same switch governs it.
        if (!DatabaseDescriptor.streamEntireSSTables() || !DatabaseDescriptor.getZeroCopyPartialStreamEnabled())
            return null;

        ZeroCopySSTableSlice.Plan plan =
            ZeroCopySSTableSlice.plan(ref.get(), sections, DatabaseDescriptor.getZeroCopyPartialStreamMaxDeadSpaceRatio());

        if (!plan.isEligible())
        {
            logger.debug("Not streaming {} as a zero-copy slice: {}", filename, plan.reason);
            return null;
        }
        return plan;
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
     * <p>
     * Data.db is exact. The others are the parent's scaled by the fraction of it being sent, because measuring
     * them means an Index.db pass and this runs once per sstable in the plan, before the peer has even been asked
     * whether it wants them. The manifest that goes on the wire is the measured one, built in {@link #writeSlice}.
     * <p>
     * So {@code bytes_to_send} for a slice is an approximation of {@code bytes_sent}, off by the error in the
     * index, filter and summary estimates -- a few percent of a few percent of the transfer. Nothing depends on
     * the two agreeing: the receiver sizes everything from the manifest it is sent, and
     * {@code StreamingState.progress} clamps at 0.99 until the session ends, so this cannot report more than
     * 100%. Which components are named IS exact, since {@code files_to_send} is a count and cheap to get right --
     * hence conditioning FILTER on the same thing the writer conditions it on rather than on the parent's files.
     */
    private static ComponentManifest estimateSliceManifest(SSTableReader sstable, ZeroCopySSTableSlice.Plan plan)
    {
        double fraction = sstable.uncompressedLength() <= 0
                          ? 1.0
                          : Math.min(1.0, (double) plan.usefulBytes / sstable.uncompressedLength());

        Map<Component, Long> sizes = new HashMap<>();
        sizes.put(Components.DATA, plan.physicalBytes);
        for (Component component : plan.components())
        {
            // A filter is written for the slice exactly when one can be: fp chance 1.0 means AlwaysPresentFilter,
            // which has nothing to serialise.
            if (component == Components.FILTER && sstable.metadata().params.bloomFilterFpChance >= 1.0)
                continue;

            long parentSize = sstable.descriptor.fileFor(component).length();
            // Statistics.db is per-sstable rather than per-partition, so it does not shrink with the range. CRC.db
            // is four bytes per cell, and the slice has as many cells as it has.
            long size;
            if (component == Components.STATS)
                size = parentSize;
            else if (component == Components.CRC)
                size = 4 + 4 * plan.cellCount();
            else
                size = (long) (parentSize * fraction);
            sizes.put(component, Math.max(1, size));
        }
        return ComponentManifest.ordered(sstable.descriptor, sizes);
    }

    @VisibleForTesting
    public boolean contained(List<SSTableReader.PartitionPositionBounds> sections, SSTableReader sstable)
    {
        if (sections == null || sections.isEmpty())
            return false;

        // Entire-SSTable streaming copies every component file verbatim, so it is eligible whenever the
        // requested sections cover all of the sstable's live data - not only when the byte span equals the
        // physical data length. A zero-copy split child can carry a "dead prefix": bytes before its first
        // indexed partition (the head of a boundary compression chunk copied verbatim) that no read path
        // ever enters. getPositionsForRanges() starts the first section at the first partition's data
        // position, so the eligible span runs from there to the end of the file. Comparing against
        // (uncompressedLength - firstPosition) accounts for that prefix; for an ordinary sstable
        // firstPosition == 0 and this reduces to the original transferLength == uncompressedLength check.
        // getPosition() applies a MOVED_START reader's moved start, so for one of those this is the same span
        // getPositionsForFullRange() reports.
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
