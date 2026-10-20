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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.agrona.collections.Long2ObjectHashMap;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.CollectionSerializers;

public class MutationSummary
{
    public static class CoordinatorSummary
    {
        private static final Comparator<CoordinatorSummary> idComparator =
            (l, r) -> CoordinatorLogId.comparator.compare(l.logId(), r.logId());

        public final Offsets.Immutable reconciled;
        public final Offsets.Immutable unreconciled;

        public CoordinatorSummary(Offsets.Immutable reconciled, Offsets.Immutable unreconciled)
        {
            Preconditions.checkArgument(reconciled.logId().equals(unreconciled.logId()));
            this.reconciled = reconciled;
            this.unreconciled = unreconciled;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            CoordinatorSummary summary = (CoordinatorSummary) o;
            return reconciled.equals(summary.reconciled) && unreconciled.equals(summary.unreconciled);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(reconciled, unreconciled);
        }

        @Override
        public String toString()
        {
            return "CoordinatorSummary{" +
                    "logId=" + logId() +
                    ", reconciled=" + reconciled +
                    ", unreconciled=" + unreconciled +
                    '}';
        }

        public CoordinatorLogId logId()
        {
            return reconciled.logId();
        }

        boolean contains(int offset)
        {
            return reconciled.contains(offset) || unreconciled.contains(offset);
        }

        Offsets.RangeIterator rangeIterator()
        {
            return Offsets.union(reconciled.rangeIterator(), unreconciled.rangeIterator());
        }

        private static Offsets.RangeIterator unreconciledDifferenceIterator(CoordinatorSummary source, CoordinatorSummary target)
        {
            if (source.unreconciled.isEmpty())
                return null;
            return Offsets.difference(source.unreconciled.rangeIterator(), target.rangeIterator());
        }

        /**
         * Finds all elements that are contained by {@code left} and not contained by {@code right}
         */
        static void difference(CoordinatorSummary left, CoordinatorSummary right, Collection<ShortMutationId> into)
        {
            Offsets.RangeIterator missing = Offsets.difference(left.rangeIterator(), right.rangeIterator());
            Offsets.forEachOffset(missing, (logId, offset) -> into.add(new ShortMutationId(logId, offset)));
        }

        static void unreconciledDifference(CoordinatorSummary source, CoordinatorSummary target, Collection<ShortMutationId> into)
        {
            Offsets.Immutable diff = unreconciledDifference(source, target);
            if (diff != null)
                diff.collectIds(into);
        }

        static Offsets.Immutable unreconciledDifference(CoordinatorSummary source, CoordinatorSummary target)
        {
            Offsets.RangeIterator missing = unreconciledDifferenceIterator(source, target);
            if (missing == null)
                return null;
            Offsets.Immutable.Builder builder = Offsets.Immutable.Builder.createOrNull(missing);
            return builder != null ? builder.build() : null;
        }

        private static int minOffset(Offsets offsets)
        {
            return offsets.bounds[0];
        }

        private static int maxOffset(Offsets offsets)
        {
            return offsets.bounds[offsets.size - 1];
        }

        private static boolean hasMissingOffsets(Offsets source, Offsets target)
        {
            if (minOffset(source) < minOffset(target) || maxOffset(source) > maxOffset(target))
                return true;

            for (int i = 0; i < source.size; i += 2)
            {
                if (!target.containsRange(source.bounds[i], source.bounds[i + 1]))
                    return true;
            }
            return false;
        }

        private static boolean allRangesContainedInEither(Offsets source, Offsets a, Offsets b)
        {
            for (int i = 0; i < source.size; i += 2)
            {
                int start = source.bounds[i];
                int end = source.bounds[i + 1];
                if (!a.containsRange(start, end) && !b.containsRange(start, end))
                    return false;
            }
            return true;
        }

        static boolean hasMissingUnreconciled(CoordinatorSummary source, CoordinatorSummary target)
        {
            if (source == target || source.unreconciled.isEmpty())
                return false;
            if (target.reconciled.isEmpty() && target.unreconciled.isEmpty())
                return true;

            if (target.reconciled.isEmpty())
                return hasMissingOffsets(source.unreconciled, target.unreconciled);

            if (target.unreconciled.isEmpty())
                return hasMissingOffsets(source.unreconciled, target.reconciled);

            int tMin = Math.min(minOffset(target.reconciled), minOffset(target.unreconciled));
            int tMax = Math.max(maxOffset(target.reconciled), maxOffset(target.unreconciled));
            if (minOffset(source.unreconciled) < tMin || maxOffset(source.unreconciled) > tMax)
                return true;

            if (allRangesContainedInEither(source.unreconciled, target.reconciled, target.unreconciled))
                return false;

            Offsets.RangeIterator missing = unreconciledDifferenceIterator(source, target);
            return missing != null && missing.tryAdvance();
        }

        void digest(Digest digest)
        {
            reconciled.digest(digest);
            unreconciled.digest(digest);
        }

        public static class Builder
        {
            public final CoordinatorLogId logId;
            public final Offsets.Immutable.Builder reconciled;
            public final Offsets.Immutable.Builder unreconciled;

            public Builder(CoordinatorLogId logId)
            {
                this.logId = logId;
                reconciled = new Offsets.Immutable.Builder(logId);
                unreconciled = new Offsets.Immutable.Builder(logId);
            }

            boolean isEmpty()
            {
                return reconciled.isEmpty() && unreconciled.isEmpty();
            }

            public CoordinatorSummary build()
            {
                return new CoordinatorSummary(reconciled.build(), unreconciled.build());
            }
        }

        public static final UnversionedSerializer<CoordinatorSummary> serializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(CoordinatorSummary t, DataOutputPlus out) throws IOException
            {
                Offsets.serializer.serialize(t.reconciled, out);
                Offsets.serializer.serialize(t.unreconciled, out);
            }

            @Override
            public CoordinatorSummary deserialize(DataInputPlus in) throws IOException
            {
                return new CoordinatorSummary(Offsets.serializer.deserialize(in), Offsets.serializer.deserialize(in));
            }

            @Override
            public long serializedSize(CoordinatorSummary t)
            {
                return Offsets.serializer.serializedSize(t.reconciled)
                     + Offsets.serializer.serializedSize(t.unreconciled);
            }
        };
    }

    public static class Builder
    {
        public final TableId tableId;
        private final Long2ObjectHashMap<CoordinatorSummary.Builder> builders = new Long2ObjectHashMap<>();

        public Builder(TableId tableId)
        {
            this.tableId = tableId;
        }

        public CoordinatorSummary.Builder builderForLog(CoordinatorLogId logId)
        {
            CoordinatorSummary.Builder builder = builders.get(logId.asLong());
            if (builder == null)
            {
                builder = new CoordinatorSummary.Builder(logId);
                builders.put(logId.asLong(), builder);
            }

            return builder;
        }

        public MutationSummary build()
        {
            List<CoordinatorSummary> summaries = new ArrayList<>(builders.size());
            for (CoordinatorSummary.Builder builder : builders.values())
                if (!builder.isEmpty())
                    summaries.add(builder.build());

            summaries.sort(CoordinatorSummary.idComparator);
            return new MutationSummary(tableId, summaries);
        }
    }

    private final TableId tableId;
    private final List<CoordinatorSummary> summaries;
    private transient final Long2ObjectHashMap<CoordinatorSummary> coordinatorSummaryMap = new Long2ObjectHashMap<>();

    private MutationSummary(TableId tableId, List<CoordinatorSummary> summaries)
    {
        long lastId = 0;
        for (int i=0, mi=summaries.size(); i<mi; i++)
        {
            CoordinatorSummary summary = summaries.get(i);
            long thisId = summary.logId().asLong();
            if (i > 0 && thisId <= lastId)
                throw new IllegalArgumentException("duplicated or unsorted log id found");

            coordinatorSummaryMap.put(thisId, summary);
            lastId = thisId;
        }

        this.tableId = tableId;
        this.summaries = summaries;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        MutationSummary summary = (MutationSummary) o;
        return tableId.equals(summary.tableId) && summaries.equals(summary.summaries);
    }

    @Override
    public int hashCode()
    {
        return tableId.hashCode() + 31 * summaries.hashCode();
    }

    @Override
    public String toString()
    {
        return "MutationSummary{tableId=" + tableId + ", summaries=" + summaries + '}';
    }

    public TableId tableId()
    {
        return tableId;
    }

    public byte[] digest()
    {
        Digest digest = Digest.forReadResponse();
        digest.updateWithLong(tableId.asUUID().getMostSignificantBits());
        digest.updateWithLong(tableId.asUUID().getLeastSignificantBits());
        digest.updateWithInt(summaries.size());

        for (CoordinatorSummary summary : summaries)
            summary.digest(digest);

        return digest.digest();
    }

    public boolean contains(ShortMutationId id)
    {
        CoordinatorSummary summary = coordinatorSummaryMap.get(id.logId());
        return summary != null && summary.contains(id.offset());
    }

    public int unreconciledIds()
    {
        int count = 0;
        for (CoordinatorSummary summary : summaries)
            count += summary.unreconciled.offsetCount();
        return count;
    }

    public int reconciledIds()
    {
        int count = 0;
        for (CoordinatorSummary summary : summaries)
            count += summary.reconciled.offsetCount();
        return count;
    }

    public int size()
    {
        return summaries.size();
    }

    @VisibleForTesting
    public boolean isEmpty()
    {
        return size() == 0;
    }

    public CoordinatorSummary get(int i)
    {
        return summaries.get(i);
    }

    public CoordinatorSummary get(CoordinatorLogId logId)
    {
        return coordinatorSummaryMap.get(logId.asLong());
    }

    /**
     * Finds all elements that are contained by {@code left} and not contained by {@code right}
     */
    public static void difference(MutationSummary left, MutationSummary right, Collection<ShortMutationId> into)
    {
        int i = 0, j = 0, lsize = left.size(), rsize = right.size();

        while (i < lsize && j < rsize)
        {
            CoordinatorSummary l = left.get(i);
            CoordinatorSummary r = right.get(j);

            int cmp = CoordinatorSummary.idComparator.compare(l, r);

            if (cmp == 0)
            {
                CoordinatorSummary.difference(l, r, into);
                ++i;
                ++j;
            }
            else if (cmp < 0)
            {
                l.reconciled.collectIds(into);
                l.unreconciled.collectIds(into);
                ++i;
            }
            else
            {
                ++j;
            }
        }

        while (i < lsize)
        {
            CoordinatorSummary l = left.get(i);
            l.reconciled.collectIds(into);
            l.unreconciled.collectIds(into);
            ++i;
        }
    }

    /**
     * Finds offsets in {@code source.unreconciled} that are absent from {@code target},
     * returning them as a {@link Log2OffsetsMap}.
     */
    private static void unreconciledDifference(MutationSummary source, MutationSummary target, Consumer<Offsets.Immutable> into)
    {
        int i = 0, j = 0, ssize = source.size(), tsize = target.size();

        while (i < ssize && j < tsize)
        {
            CoordinatorSummary s = source.get(i);
            CoordinatorSummary t = target.get(j);

            int cmp = CoordinatorSummary.idComparator.compare(s, t);

            if (cmp == 0)
            {
                Offsets.Immutable diff = CoordinatorSummary.unreconciledDifference(s, t);
                if (diff != null && !diff.isEmpty())
                    into.accept(diff);
                ++i;
                ++j;
            }
            else if (cmp < 0)
            {
                if (!s.unreconciled.isEmpty())
                    into.accept(s.unreconciled);
                ++i;
            }
            else
            {
                ++j;
            }
        }

        while (i < ssize)
        {
            CoordinatorSummary s = source.get(i);
            if (!s.unreconciled.isEmpty())
                into.accept(s.unreconciled);
            ++i;
        }
    }

    /**
     * Finds offsets in {@code source.unreconciled} that are absent from {@code target}.
     */
    public static void unreconciledDifference(MutationSummary source, MutationSummary target, Collection<ShortMutationId> into)
    {
        unreconciledDifference(source, target, offsets -> offsets.collectIds(into));
    }

    /**
     * Finds offsets in {@code source.unreconciled} that are absent from {@code target}.
     */
    public static void unreconciledDifference(MutationSummary source, MutationSummary target, Log2OffsetsMap.Mutable into)
    {
        unreconciledDifference(source, target, into::add);
    }

    /**
     * Returns true if {@code source} contains any unreconciled offset absent from {@code target}.
     */
    public static boolean hasMissingUnreconciled(MutationSummary source, MutationSummary target)
    {
        if (source == target)
            return false;

        int i = 0, j = 0, ssize = source.size(), tsize = target.size();

        while (i < ssize && j < tsize)
        {
            CoordinatorSummary s = source.get(i);
            CoordinatorSummary t = target.get(j);

            int cmp = CoordinatorSummary.idComparator.compare(s, t);

            if (cmp == 0)
            {
                if (!s.unreconciled.isEmpty() && CoordinatorSummary.hasMissingUnreconciled(s, t))
                    return true;
                ++i;
                ++j;
            }
            else if (cmp < 0)
            {
                if (!s.unreconciled.isEmpty())
                    return true;
                ++i;
            }
            else
            {
                ++j;
            }
        }

        while (i < ssize)
        {
            if (!source.get(i).unreconciled.isEmpty())
                return true;
            ++i;
        }

        return false;
    }

    public void forEachUnreconciled(Consumer<Offsets> action)
    {
        for (int i = 0; i < summaries.size(); i++)
        {
            Offsets offsets = summaries.get(i).unreconciled;
            if (!offsets.isEmpty())
                action.accept(offsets);
        }
    }

    public static final UnversionedSerializer<MutationSummary> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(MutationSummary summary, DataOutputPlus out) throws IOException
        {
            summary.tableId.serializeCompact(out);
            CollectionSerializers.serializeList(summary.summaries, out, CoordinatorSummary.serializer);
        }

        @Override
        public MutationSummary deserialize(DataInputPlus in) throws IOException
        {
            TableId tableId = TableId.deserializeCompact(in);
            List<CoordinatorSummary> summaries = CollectionSerializers.deserializeList(in, CoordinatorSummary.serializer);
            return new MutationSummary(tableId, summaries);
        }

        @Override
        public long serializedSize(MutationSummary summary)
        {
            long size = summary.tableId.serializedCompactSize();
            size += CollectionSerializers.serializedListSize(summary.summaries, CoordinatorSummary.serializer);
            return size;
        }
    };
}
