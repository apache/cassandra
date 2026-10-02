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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.journal.DeserializedRecordConsumer;
import org.apache.cassandra.repair.ValidationPartitionIterator;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.replication.ValidationOffsets;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Merges a {@link CassandraValidationIterator}'s SSTable-sourced partitions with
 * reconciled-but-unflushed mutations read from this node's mutation journal, both filtered
 * to the same {@link ValidationOffsets}. Used only on the tracked keyspace validation path, so
 * the journal fills in exactly the data the SSTable predicate can't see because it was never
 * flushed.
 */
public class JournalMergingValidationIterator extends ValidationPartitionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(JournalMergingValidationIterator.class);

    private final CassandraValidationIterator sstableIterator;
    private final UnfilteredPartitionIterator merged;
    private final long estimatedBytes;
    private final long estimatedPartitions;
    private final Map<Range<Token>, Long> rangePartitionCounts;

    public JournalMergingValidationIterator(ColumnFamilyStore cfs,
                                            CassandraValidationIterator sstableIterator,
                                            Collection<Range<Token>> ranges,
                                            ValidationOffsets validationOffsets)
    {
        this.sstableIterator = sstableIterator;
        JournalPartitions journalPartitions = readJournalPartitions(cfs, ranges, validationOffsets);
        this.estimatedBytes = sstableIterator.getEstimatedBytes() + journalPartitions.estimatedBytes;
        this.estimatedPartitions = sstableIterator.estimatedPartitions() + journalPartitions.byKey.size();
        this.rangePartitionCounts = new HashMap<>(sstableIterator.getRangePartitionCounts());
        for (Map.Entry<Range<Token>, Long> entry : journalPartitions.rangeCounts.entrySet())
            rangePartitionCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
        UnfilteredPartitionIterator journalIterator = new JournalPartitionIterator(cfs.metadata(), journalPartitions.byKey);
        this.merged = UnfilteredPartitionIterators.merge(List.of(sstableIterator, journalIterator), UnfilteredPartitionIterators.MergeListener.NOOP);
    }

    /**
     * Journal partitions grouped by key, alongside the summed {@link PartitionUpdate#dataSize()}
     * of everything read -- the journal's counterpart to the SSTable stream's file-position-based
     * {@link CassandraValidationIterator#getEstimatedBytes()}, since journal-resident data has no
     * file byte range to measure. {@code rangeCounts} is the same idea for partition counts: one
     * entry per range in {@code ranges}, counting distinct journal partitions in that range.
     */
    private static final class JournalPartitions
    {
        final NavigableMap<DecoratedKey, List<PartitionUpdate>> byKey;
        final long estimatedBytes;
        final Map<Range<Token>, Long> rangeCounts;

        JournalPartitions(NavigableMap<DecoratedKey, List<PartitionUpdate>> byKey, long estimatedBytes, Map<Range<Token>, Long> rangeCounts)
        {
            this.byKey = byKey;
            this.estimatedBytes = estimatedBytes;
            this.rangeCounts = rangeCounts;
        }
    }

    private static JournalPartitions readJournalPartitions(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, ValidationOffsets validationOffsets)
    {
        TableMetadata metadata = cfs.metadata();
        TableId tableId = metadata.id;
        NavigableMap<DecoratedKey, List<PartitionUpdate>> byKey = new TreeMap<>(DecoratedKey.comparator);
        Map<Range<Token>, Long> rangeCounts = new HashMap<>();
        long[] estimatedBytes = {0};

        try (MutationJournal.Snapshot snapshot = MutationJournal.instance().snapshot())
        {
            snapshot.readAll(new DeserializedRecordConsumer<ShortMutationId, Mutation>(MutationJournal.MutationSerializer.INSTANCE)
            {
                @Override
                protected void accept(long segment, int position, ShortMutationId key, Mutation mutation)
                {
                    if (!validationOffsets.containsMutation(key))
                        return;
                    PartitionUpdate update = mutation.modifications().get(tableId);
                    if (update == null)
                        return;
                    DecoratedKey partitionKey = update.partitionKey();
                    Range<Token> range = findRange(partitionKey.getToken(), ranges);
                    if (range == null)
                        return;
                    List<PartitionUpdate> updatesForKey = byKey.computeIfAbsent(partitionKey, k -> new ArrayList<>());
                    if (updatesForKey.isEmpty())
                        rangeCounts.merge(range, 1L, Long::sum);
                    updatesForKey.add(update);
                    estimatedBytes[0] += update.dataSize();
                }
            });
        }

        logger.info("Performing journal validation on {} partitions ({} bytes) in {}.{}", byKey.size(), estimatedBytes[0], metadata.keyspace, metadata.name);
        return new JournalPartitions(byKey, estimatedBytes[0], rangeCounts);
    }

    private static Range<Token> findRange(Token token, Collection<Range<Token>> ranges)
    {
        for (Range<Token> range : ranges)
        {
            if (range.contains(token))
                return range;
        }
        return null;
    }

    @Override
    public boolean hasNext()
    {
        return merged.hasNext();
    }

    @Override
    public UnfilteredRowIterator next()
    {
        return merged.next();
    }

    @Override
    public TableMetadata metadata()
    {
        return sstableIterator.metadata();
    }

    @Override
    public void close()
    {
        merged.close();
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
    public long getBytesRead()
    {
        return sstableIterator.getBytesRead();
    }

    @Override
    public Map<Range<Token>, Long> getRangePartitionCounts()
    {
        return rangePartitionCounts;
    }

    /**
     * Adapts the per-key journal partitions collected by {@link #readJournalPartitions} into an
     * {@link UnfilteredPartitionIterator}, so they can be merged against the SSTable stream via
     * {@link UnfilteredPartitionIterators#merge} instead of a hand-written two-way merge. A key
     * with more than one journal-resident update (multiple reconciled mutations touching the
     * same partition before either was flushed) is itself merged via
     * {@link UnfilteredRowIterators#merge}.
     */
    private static class JournalPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<Map.Entry<DecoratedKey, List<PartitionUpdate>>> iterator;

        JournalPartitionIterator(TableMetadata metadata, NavigableMap<DecoratedKey, List<PartitionUpdate>> byKey)
        {
            this.metadata = metadata;
            this.iterator = byKey.entrySet().iterator();
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            List<PartitionUpdate> updates = iterator.next().getValue();
            if (updates.size() == 1)
                return updates.get(0).unfilteredIterator();

            List<UnfilteredRowIterator> toMerge = new ArrayList<>(updates.size());
            for (PartitionUpdate update : updates)
                toMerge.add(update.unfilteredIterator());
            return UnfilteredRowIterators.merge(toMerge);
        }
    }
}
