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
import java.io.UncheckedIOException;
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
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
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
 * the journal fills in exactly the data the SSTable predicate can't see because it was never flushed.
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
        // Narrow the offsets for the journal to exclude anything the selected SSTables already cover.
        // The journal isn't pruned on flush, so without this, we could merge the same data from both sources.
        ValidationOffsets journalOffsets = validationOffsets.without(sstableIterator.sstableCoordinatorLogOffsets());

        JournalPartitionAccessor journalAccessor = JournalPartitionAccessor.build(cfs, ranges, journalOffsets);
        try
        {
            JournalPartitionIterator journalIterator = new JournalPartitionIterator(journalAccessor);

            this.estimatedBytes = sstableIterator.getEstimatedBytes() + journalAccessor.estimatedBytes;
            this.estimatedPartitions = sstableIterator.estimatedPartitions() + journalAccessor.byKey.size();
            this.rangePartitionCounts = new HashMap<>(sstableIterator.getRangePartitionCounts());
            for (Map.Entry<Range<Token>, Long> entry : journalAccessor.rangeCounts.entrySet())
                rangePartitionCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
            this.merged = UnfilteredPartitionIterators.merge(List.of(sstableIterator, journalIterator), UnfilteredPartitionIterators.MergeListener.NOOP);
        }
        catch (Throwable t)
        {
            journalAccessor.close();
            throw t;
        }
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

    private static final class JournalPartitionAccessor implements AutoCloseable
    {
        final TableMetadata metadata;
        final TableId tableId;
        final NavigableMap<DecoratedKey, List<ShortMutationId>> byKey;
        final long estimatedBytes;
        final Map<Range<Token>, Long> rangeCounts;
        final MutationJournal.Snapshot snapshot;

        private JournalPartitionAccessor(TableMetadata metadata,
                                         NavigableMap<DecoratedKey, List<ShortMutationId>> byKey,
                                         long estimatedBytes,
                                         Map<Range<Token>, Long> rangeCounts,
                                         MutationJournal.Snapshot snapshot)
        {
            this.metadata = metadata;
            this.tableId = metadata.id;
            this.byKey = byKey;
            this.estimatedBytes = estimatedBytes;
            this.rangeCounts = rangeCounts;
            this.snapshot = snapshot;
        }

        static JournalPartitionAccessor build(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, ValidationOffsets validationOffsets)
        {
            MutationJournal.Snapshot snapshot = MutationJournal.instance().snapshot();
            try
            {
                return buildFromSnapshot(snapshot, cfs, ranges, validationOffsets);
            }
            catch (Throwable t)
            {
                snapshot.close();
                throw t;
            }
        }

        private static JournalPartitionAccessor buildFromSnapshot(MutationJournal.Snapshot snapshot, ColumnFamilyStore cfs, Collection<Range<Token>> ranges, ValidationOffsets validationOffsets)
        {
            TableMetadata metadata = cfs.metadata();
            TableId tableId = metadata.id;
            NavigableMap<DecoratedKey, List<ShortMutationId>> byKey = new TreeMap<>(DecoratedKey.comparator);
            Map<Range<Token>, Long> rangeCounts = new HashMap<>();
            long[] estimatedBytes = {0};

            snapshot.readAll((segment, position, key, buffer, userVersion) ->
            {
                // Check the offset before we actually deserialize a mutation:
                if (!validationOffsets.containsMutation(key))
                    return;

                Mutation mutation;
                try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                {
                    mutation = MutationJournal.MutationSerializer.INSTANCE.deserialize(key, in, userVersion);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }

                // Ignore the mutation if it doesn't belong to this table:
                PartitionUpdate update = mutation.modifications().get(tableId);
                if (update == null)
                    return;

                // Ignore the mutation if it doesn't belong to a validating range:
                DecoratedKey partitionKey = update.partitionKey();
                Range<Token> range = findRange(partitionKey.getToken(), ranges);
                if (range == null)
                    return;

                List<ShortMutationId> idsForKey = byKey.computeIfAbsent(partitionKey, k -> new ArrayList<>());
                if (idsForKey.isEmpty())
                    rangeCounts.merge(range, 1L, Long::sum);

                // A rewritten id is one logical entry. Don't read and merge it with itself.
                if (!idsForKey.contains(key))
                {
                    idsForKey.add(key);
                    estimatedBytes[0] += update.dataSize();
                }
            });

            logger.info("Performing journal validation on {} partitions ({} bytes) in {}.{}", byKey.size(), estimatedBytes[0], metadata.keyspace, metadata.name);
            return new JournalPartitionAccessor(metadata, byKey, estimatedBytes[0], rangeCounts, snapshot);
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

        PartitionUpdate readUpdate(ShortMutationId id)
        {
            PartitionUpdate[] holder = new PartitionUpdate[1];
            boolean found = snapshot.read(id, (segment, position, key, buffer, userVersion) ->
            {
                try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                {
                    Mutation mutation = MutationJournal.MutationSerializer.INSTANCE.deserialize(key, in, userVersion);
                    holder[0] = mutation.modifications().get(tableId);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            });

            // Already confirmed to exist and belong to this table during the initial scan, so this
            // should be unreachable while the snapshot is still open.
            if (!found || holder[0] == null)
                throw new IllegalStateException("Journal record " + id + " for " + metadata + " vanished while its validation snapshot was still open");
            return holder[0];
        }

        @Override
        public void close()
        {
            snapshot.close();
        }
    }

    private static class JournalPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final JournalPartitionAccessor accessor;
        private final Iterator<Map.Entry<DecoratedKey, List<ShortMutationId>>> iterator;

        JournalPartitionIterator(JournalPartitionAccessor accessor)
        {
            this.accessor = accessor;
            this.iterator = accessor.byKey.entrySet().iterator();
        }

        @Override
        public TableMetadata metadata()
        {
            return accessor.metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            List<ShortMutationId> ids = iterator.next().getValue();
            UnfilteredRowIterator update = rowIteratorFromIds(ids);

            // PartitionUpdate only reflects columns that are part of a mutation, but the SSTable iterator reflects all
            // columns in the schema. Expand the columns here to avoid digest mismatches on identical rows.
            return withFullSchemaColumns(update, accessor.metadata.regularAndStaticColumns());
        }

        private UnfilteredRowIterator rowIteratorFromIds(List<ShortMutationId> ids)
        {
            if (ids.size() == 1)
                return accessor.readUpdate(ids.get(0)).unfilteredIterator();

            List<UnfilteredRowIterator> toMerge = new ArrayList<>(ids.size());
            for (ShortMutationId id : ids)
                toMerge.add(accessor.readUpdate(id).unfilteredIterator());
            return UnfilteredRowIterators.merge(toMerge);
        }

        @Override
        public void close()
        {
            accessor.close();
        }

        private static UnfilteredRowIterator withFullSchemaColumns(UnfilteredRowIterator wrapped, RegularAndStaticColumns columns)
        {
            return new WrappingUnfilteredRowIterator()
            {
                @Override
                public UnfilteredRowIterator wrapped()
                {
                    return wrapped;
                }

                @Override
                public RegularAndStaticColumns columns()
                {
                    return columns;
                }
            };
        }
    }
}
