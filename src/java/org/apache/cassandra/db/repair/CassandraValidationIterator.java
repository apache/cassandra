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

package org.apache.cassandra.db.repair;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongPredicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.ActiveCompactions;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.repair.ValidationPartitionIterator;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;

public class CassandraValidationIterator extends ValidationPartitionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraValidationIterator.class);

    /*
     * Controller for validation compaction that always purges.
     * Note that we should not call cfs.getOverlappingSSTables on the provided
     * sstables because those sstables are not guaranteed to be active sstables
     * (since we can run repair on a snapshot).
     */
    private static class ValidationCompactionController extends CompactionController
    {
        public ValidationCompactionController(ColumnFamilyStore cfs, int gcBefore)
        {
            super(cfs, gcBefore);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            /*
             * The main reason we always purge is that including gcable tombstone would mean that the
             * repair digest will depends on the scheduling of compaction on the different nodes. This
             * is still not perfect because gcbefore is currently dependend on the current time at which
             * the validation compaction start, which while not too bad for normal repair is broken for
             * repair on snapshots. A better solution would be to agree on a gcbefore that all node would
             * use, and we'll do that with CASSANDRA-4932.
             * Note validation compaction includes all sstables, so we don't have the problem of purging
             * a tombstone that could shadow a column in another sstable, but this is doubly not a concern
             * since validation compaction is read-only.
             */
            return time -> true;
        }
    }

    public static int getDefaultGcBefore(ColumnFamilyStore cfs, int nowInSec)
    {
        // 2ndary indexes have ExpiringColumns too, so we need to purge tombstones deleted before now. We do not need to
        // add any GcGrace however since 2ndary indexes are local to a node.
        return cfs.isIndex() ? nowInSec : cfs.gcBefore(nowInSec);
    }

    private static class ValidationCompactionIterator extends CompactionIterator
    {
        public ValidationCompactionIterator(List<ISSTableScanner> scanners, ValidationCompactionController controller, int nowInSec, ActiveCompactionsTracker activeCompactions)
        {
            super(OperationType.VALIDATION, scanners, controller, nowInSec, UUIDGen.getTimeUUID(), activeCompactions);
        }
    }

    private static Predicate<SSTableReader> getPreviewPredicate(PreviewKind previewKind)
    {
        switch (previewKind)
        {
            case ALL:
                return (s) -> true;
            case REPAIRED:
                return (s) -> s.isRepaired();
            case UNREPAIRED:
                return (s) -> !s.isRepaired();
            default:
                throw new RuntimeException("Can't get preview predicate for preview kind " + previewKind);
        }
    }

    @VisibleForTesting
    static synchronized Refs<SSTableReader> getSSTablesToValidate(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, UUID parentId, boolean isIncremental)
    {
        Refs<SSTableReader> sstables;

        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(parentId);
        if (prs == null)
        {
            // this means the parent repair session was removed - the repair session failed on another node and we removed it
            return new Refs<>();
        }

        Set<SSTableReader> sstablesToValidate = new HashSet<>();

        com.google.common.base.Predicate<SSTableReader> predicate;
        if (prs.isPreview())
        {
            predicate = getPreviewPredicate(prs.previewKind);

        }
        else if (isIncremental)
        {
            predicate = s -> parentId.equals(s.getSSTableMetadata().pendingRepair);
        }
        else
        {
            // note that we always grab all existing sstables for this - if we were to just grab the ones that
            // were marked as repairing, we would miss any ranges that were compacted away and this would cause us to overstream
            predicate = (s) -> !prs.isIncremental || !s.isRepaired();
        }

        try (ColumnFamilyStore.RefViewFragment sstableCandidates = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, predicate)))
        {
            for (SSTableReader sstable : sstableCandidates.sstables)
            {
                if (new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges))
                {
                    sstablesToValidate.add(sstable);
                }
            }

            sstables = Refs.tryRef(sstablesToValidate);
            if (sstables == null)
            {
                logger.error("Could not reference sstables");
                throw new RuntimeException("Could not reference sstables");
            }
        }

        return sstables;
    }

    private final ColumnFamilyStore cfs;
    private final Refs<SSTableReader> sstables;
    private final String snapshotName;
    private final boolean isGlobalSnapshotValidation;

    private final boolean isSnapshotValidation;
    private final AbstractCompactionStrategy.ScannerList scanners;
    private final ValidationCompactionController controller;

    private final CompactionIterator ci;

    private final long estimatedBytes;
    private final long estimatedPartitions;
    private final Map<Range<Token>, Long> rangePartitionCounts;

    public CassandraValidationIterator(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, UUID parentId, UUID sessionID, boolean isIncremental, int nowInSec) throws IOException
    {
        this.cfs = cfs;

        isGlobalSnapshotValidation = cfs.snapshotExists(parentId.toString());
        if (isGlobalSnapshotValidation)
            snapshotName = parentId.toString();
        else
            snapshotName = sessionID.toString();
        isSnapshotValidation = cfs.snapshotExists(snapshotName);

        if (isSnapshotValidation)
        {
            // If there is a snapshot created for the session then read from there.
            // note that we populate the parent repair session when creating the snapshot, meaning the sstables in the snapshot are the ones we
            // are supposed to validate.
            sstables = cfs.getSnapshotSSTableReaders(snapshotName);
        }
        else
        {
            if (!isIncremental)
            {
                // flush first so everyone is validating data that is as similar as possible
                StorageService.instance.forceKeyspaceFlush(cfs.keyspace.getName(), cfs.name);
            }
            sstables = getSSTablesToValidate(cfs, ranges, parentId, isIncremental);
        }

        Preconditions.checkArgument(sstables != null);
        controller = new ValidationCompactionController(cfs, getDefaultGcBefore(cfs, nowInSec));
        scanners = cfs.getCompactionStrategyManager().getScanners(sstables, ranges);
        ci = new ValidationCompactionIterator(scanners.scanners, controller, nowInSec, CompactionManager.instance.active);

        long allPartitions = 0;
        rangePartitionCounts = Maps.newHashMapWithExpectedSize(ranges.size());
        for (Range<Token> range : ranges)
        {
            long numPartitions = 0;
            for (SSTableReader sstable : sstables)
                numPartitions += sstable.estimatedKeysForRanges(Collections.singleton(range));
            rangePartitionCounts.put(range, numPartitions);
            allPartitions += numPartitions;
        }
        estimatedPartitions = allPartitions;

        long estimatedTotalBytes = 0;
        for (SSTableReader sstable : sstables)
        {
            for (SSTableReader.PartitionPositionBounds positionsForRanges : sstable.getPositionsForRanges(ranges))
                estimatedTotalBytes += positionsForRanges.upperPosition - positionsForRanges.lowerPosition;
        }
        estimatedBytes = estimatedTotalBytes;
    }

    @Override
    public void close()
    {
        // TODO: can any of this fail and leave stuff unreleased?
        super.close();

        if (ci != null)
            ci.close();

        if (scanners != null)
            scanners.close();

        if (controller != null)
            controller.close();

        if (isSnapshotValidation && !isGlobalSnapshotValidation)
        {
            // we can only clear the snapshot if we are not doing a global snapshot validation (we then clear it once anticompaction
            // is done).
            cfs.clearSnapshot(snapshotName);
        }

        if (sstables != null)
            sstables.release();
    }

    @Override
    public TableMetadata metadata()
    {
        return cfs.metadata.get();
    }

    @Override
    public boolean hasNext()
    {
        return ci.hasNext();
    }

    @Override
    public UnfilteredRowIterator next()
    {
        return ci.next();
    }

    @Override
    public long getEstimatedBytes()
    {
        return estimatedBytes;
    }

    @Override
    public long estimatedPartitions()
    {
        return estimatedPartitions;
    }

    @Override
    public Map<Range<Token>, Long> getRangePartitionCounts()
    {
        return rangePartitionCounts;
    }
}
