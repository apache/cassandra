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

package org.apache.cassandra.harry.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.gen.BijectionCache;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.ImmutableUniqueList;

public class BytesPartitionState
{
    private final Factory factory;
    public final Clustering<ByteBuffer> key;
    private final Token token;
    private final PartitionState state;
    private final Map<Symbol, MultiCell> staticMultiCell;
    @Nullable
    private final Map<Long, Map<Symbol, MultiCell>> rowMultiCell;

    private BytesPartitionState(Factory factory, Clustering<ByteBuffer> key)
    {
        this.factory = factory;
        this.key = key;
        this.token = factory.metadata.partitioner.getToken(key.serializeAsPartitionKey());
        this.state = factory.partitionState(key);
        staticMultiCell = buildMultiCellState(factory.staticColumns);
        rowMultiCell = factory.regularColumns.stream().anyMatch(s -> s.type().isMultiCell())
                       ? new HashMap<>()
                       : null;
    }

    protected void validate()
    {
        var staticRow = staticRow();
        for (var e : staticMultiCell.entrySet())
        {
            Symbol symbol = e.getKey();
            var expected = e.getValue().state();
            var actual = staticRow.get(symbol);
            if (!Objects.equals(expected, actual))
                throw new AssertionError("Unexpected value for " + ref() + "#" + symbol
                                         + ";\nexpected=" + symbol.type().asCQL3Type().toCQLLiteral(expected)
                                         + "\n,actual=" + symbol.type().asCQL3Type().toCQLLiteral(actual));
        }
    }

    private static Map<Symbol, MultiCell> buildMultiCellState(Iterable<Symbol> columns)
    {
        ImmutableMap.Builder<Symbol, MultiCell> builder = ImmutableMap.builder();
        for (var col : columns)
        {
            if (!col.type().isMultiCell()) continue;
            builder.put(col, createMultiCell(col.type()));
        }
        return builder.build();
    }

    public void deleteRow(Clustering<ByteBuffer> clustering, long ts)
    {
        long cd = factory.clusteringCache.deflateOrUndefined(clustering);
        if (MagicConstants.UNSET_DESCR == cd)
            return;
        deleteRow(cd, ts);
    }

    private void deleteRow(long cd, long ts)
    {
        state.delete(cd, ts);
        if (rowMultiCell != null && rowMultiCell.containsKey(cd))
            rowMultiCell.remove(cd);
    }

    public void deleteColumns(Clustering<ByteBuffer> clustering, long ts, Set<Symbol> columns)
    {
        long cd = factory.clusteringCache.deflateOrUndefined(clustering);
        if (cd != MagicConstants.UNSET_DESCR)
        {
            BitSet regularColumns = bitset(columns, true);
            if (!regularColumns.allUnset())
                state.deleteRegularColumns(ts, cd, regularColumns);
            if (rowMultiCell != null && rowMultiCell.containsKey(cd))
            {
                var multiCells = rowMultiCell.get(cd);
                for (var c : Sets.intersection(columns, multiCells.keySet()))
                {
                    var value = multiCells.get(c).delete(ts);
                    state.writeRegular(cd, toDescriptor(factory.regularColumns, Collections.singletonMap(c, value)), MagicConstants.NO_TIMESTAMP, false);
                }
            }
        }
        deleteStaticColumns(ts, columns);
    }

    public void deleteStaticColumns(long ts, Set<Symbol> columns)
    {
        BitSet staticColumns = bitset(columns, false);
        if (!staticColumns.allUnset())
            state.deleteStaticColumns(ts, staticColumns);
        for (var c : Sets.intersection(columns, staticMultiCell.keySet()))
        {
            var value = staticMultiCell.get(c).delete(ts);
            state.writeStatic(toDescriptor(factory.staticColumns, Collections.singletonMap(c, value)), MagicConstants.NO_TIMESTAMP);
        }
    }

    private BitSet bitset(Set<Symbol> columns, boolean regular)
    {
        ImmutableUniqueList<Symbol> positions = regular ? factory.regularColumns : factory.staticColumns;
        BitSet bitSet = new BitSet.BitSet64Bit(positions.size());
        for (int i = 0; i < positions.size(); i++)
        {
            Symbol column = positions.get(i);
            if (!column.type().isMultiCell() && columns.contains(column))
                bitSet.set(i);
        }
        return bitSet;
    }

    public Ref ref()
    {
        return new Ref(factory, key, token);
    }

    public PrimaryKey partitionRowRef()
    {
        return new PrimaryKey(ref(), null);
    }

    private interface MultiCell
    {
        ByteBuffer state();
        ByteBuffer append(long ts, ByteBuffer buffer);

        ByteBuffer remove(long ts, ByteBuffer value);

        ByteBuffer delete(long ts);

        default ByteBuffer override(long ts, ByteBuffer buffer)
        {
            var previous = delete(ts - 1);
            return buffer == null ? previous : append(ts, buffer);
        }
    }

    private static abstract class AbstractMultiCell implements MultiCell
    {
        protected long highestTombstone = MagicConstants.NO_TIMESTAMP;

        protected boolean isShadowed(long ts)
        {
            return MagicConstants.NO_TIMESTAMP != ts && hasTombstone() && highestTombstone > ts;
        }

        protected boolean hasTombstone()
        {
            return highestTombstone != MagicConstants.NO_TIMESTAMP;
        }
    }

    private static class ListState extends AbstractMultiCell
    {
        private final ListType<?> type;
        private final List<Cell> cells = new ArrayList<>();

        private ListState(ListType<?> type)
        {
            this.type = type;
        }

        @Override
        public ByteBuffer state()
        {
            if (cells.isEmpty()) return null;
            return type.getSerializer().pack(cells.stream().map(c -> c.value).collect(Collectors.toList()));
        }

        @Override
        public ByteBuffer append(long ts, ByteBuffer buffer)
        {
            if (isShadowed(ts))
                return state();
            var values = type.compose(buffer);
            for (var v : values)
                cells.add(new Cell(ts, type.getElementsType().decomposeUntyped(v)));
            return state();
        }

        @Override
        public ByteBuffer remove(long ts, ByteBuffer value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer delete(long ts)
        {
            if (isShadowed(ts))
                return state();
            highestTombstone = ts;
            cells.removeIf(c -> c.ts <= ts);
            return state();
        }

        private final class Cell
        {
            final long ts;
            final ByteBuffer value;

            private Cell(long ts, ByteBuffer value)
            {
                this.ts = ts;
                this.value = value;
            }

            @Override
            public String toString()
            {
                return ts + " " + type.getElementsType().asCQL3Type().toCQLLiteral(value);
            }
        }
    }

    private static class SetState extends AbstractMultiCell
    {
        private final SetType<?> type;
        private final TreeMap<ByteBuffer, Long> cells;

        private SetState(SetType<?> type)
        {
            this.type = type;
            this.cells = new TreeMap<>(type.getElementsType());
        }

        @Override
        public ByteBuffer state()
        {
            if (cells.isEmpty()) return null;
            return type.getSerializer().pack(cells.keySet().stream().collect(Collectors.toList()));
        }

        @Override
        public ByteBuffer append(long ts, ByteBuffer buffer)
        {
            if (isShadowed(ts))
                return state();
            var values = type.compose(buffer);
            for (var v : values)
            {
                // writing will take the highest timestamp
                ByteBuffer bb = type.getElementsType().decomposeUntyped(v);
                if (ts != MagicConstants.NO_TIMESTAMP && cells.containsKey(bb))
                {
                    if (ts > cells.get(bb))
                        cells.put(bb, ts);
                }
                else
                {
                    cells.put(bb, ts);
                }
            }
            return state();
        }

        @Override
        public ByteBuffer remove(long ts, ByteBuffer value)
        {
            if (isShadowed(ts))
                return state();
            var values = type.compose(value);
            for (var v : values)
            {
                // writing will take the highest timestamp
                ByteBuffer bb = type.getElementsType().decomposeUntyped(v);
                if (ts != MagicConstants.NO_TIMESTAMP && cells.containsKey(bb))
                {
                    if (ts > cells.get(bb))
                        cells.remove(bb);
                }
            }
            return state();
        }

        @Override
        public ByteBuffer delete(long ts)
        {
            if (isShadowed(ts))
                return state();
            highestTombstone = ts;
            List<ByteBuffer> toDelete = new ArrayList<>();
            for (var e : cells.entrySet())
            {
                if (e.getValue() <= ts)
                    toDelete.add(e.getKey());
            }
            toDelete.forEach(cells::remove);
            return state();
        }
    }

    private static class MapState extends AbstractMultiCell
    {
        private final MapType<?, ?> type;
        private final TreeMap<ByteBuffer, Value> cells;

        private MapState(MapType<?, ?> type)
        {
            this.type = type;
            this.cells = new TreeMap<>(type.getKeysType());
        }

        @Override
        public ByteBuffer state()
        {
            if (cells.isEmpty()) return null;
            List<ByteBuffer> buffers = new ArrayList<>(cells.size());
            for (var e : cells.entrySet())
            {
                buffers.add(e.getKey());
                buffers.add(e.getValue().value);
            }
            return type.getSerializer().pack(buffers);
        }

        @Override
        public ByteBuffer append(long ts, ByteBuffer buffer)
        {
            if (isShadowed(ts))
                return state();
            var values = type.compose(buffer);
            for (var e : values.entrySet())
            {
                // writing will take the highest timestamp
                ByteBuffer key = type.getKeysType().decomposeUntyped(e.getKey());
                ByteBuffer value = type.getValuesType().decomposeUntyped(e.getValue());
                if (ts != MagicConstants.NO_TIMESTAMP && cells.containsKey(key))
                {
                    Value state = cells.get(key);
                    if (ts == state.ts)
                    {
                        if (ByteBufferUtil.compareUnsigned(value, state.value) > 0)
                            cells.put(key, new Value(ts, value));
                    }
                    else if (ts > state.ts)
                        cells.put(key, new Value(ts, value));
                }
                else
                {
                    cells.put(key, new Value(ts, value));
                }
            }
            return state();
        }

        @Override
        public ByteBuffer remove(long ts, ByteBuffer value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer delete(long ts)
        {
            if (isShadowed(ts))
                return state();
            highestTombstone = ts;
            List<ByteBuffer> toDelete = new ArrayList<>();
            for (var e : cells.entrySet())
            {
                if (e.getValue().ts <= ts)
                    toDelete.add(e.getKey());
            }
            toDelete.forEach(cells::remove);
            return state();
        }

        private static class Value
        {
            private final long ts;
            private final ByteBuffer value;

            private Value(long ts, ByteBuffer value)
            {
                this.ts = ts;
                this.value = value;
            }
        }
    }

    private static class UserTypeState extends AbstractMultiCell
    {
        private final UserType type;
        private final FieldState[] fields;

        private UserTypeState(UserType type)
        {
            this.type = type;
            fields = new FieldState[type.size()];
            for (int i = 0; i < fields.length; i++)
                fields[i] = new FieldState(type.fieldType(i));
        }

        @Override
        public ByteBuffer state()
        {
            List<ByteBuffer> values = Stream.of(fields).map(f -> f.value).collect(Collectors.toList());
            return values.stream().allMatch(b -> b == null) ? null : type.pack(values);
        }

        @Override
        public ByteBuffer append(long ts, ByteBuffer buffer)
        {
            if (isShadowed(ts))
                return state();
            var values = type.unpack(buffer);
            for (int i = 0; i < values.size(); i++)
            {
                var v = values.get(i);
                if (v == null)
                    continue; // this isn't a tombstone like other types, this is a UNSET logically
                fields[i].maybeApply(ts, v);
            }
            return state();
        }

        @Override
        public ByteBuffer remove(long ts, ByteBuffer value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer delete(long ts)
        {
            if (isShadowed(ts))
                return state();
            highestTombstone = ts;
            for (var field : fields)
                field.delete(ts);
            return state();
        }

        private static class FieldState
        {
            private final AbstractType<?> type;
            private @Nullable ByteBuffer value;
            private long ts = MagicConstants.NO_TIMESTAMP;

            private FieldState(AbstractType<?> type)
            {
                this.type = type;
            }

            private void maybeApply(long ts, ByteBuffer value)
            {
                if (ts != MagicConstants.NO_TIMESTAMP
                    && ts < this.ts)
                    return;
                type.validate(value);
                if (this.ts == ts)
                {
                    // tombstone always wins
                    if (this.value == null) return;
                    else if (value == null)
                    {
                        this.value = null;
                        return;
                    }
                    // cell resolution
                    this.value = ByteBufferUtil.compareUnsigned(this.value, value) >= 0
                                 ? this.value
                                 : value;
                }
                else
                {
                    this.ts = ts;
                    this.value = value;
                }
            }

            private void delete(long ts)
            {
                if (this.ts > ts) return;
                this.ts = ts;
                this.value = null;
            }

            @Override
            public String toString()
            {
                return ts + " " + (value == null ? "null" : !value.hasRemaining() ? "<empty>" : type.asCQL3Type().toCQLLiteral(value));
            }
        }
    }

    private static MultiCell createMultiCell(AbstractType<?> type)
    {
        if (type.getClass() == ListType.class)
            return new ListState((ListType<?>) type);
        if (type.getClass() == SetType.class)
            return new SetState((SetType<?>) type);
        if (type.getClass() == MapType.class)
            return new MapState((MapType<?, ?>) type);
        if (type.getClass() == UserType.class)
            return new UserTypeState((UserType) type);
        throw new UnsupportedOperationException(type.getClass().toString());
    }

    public static class Update
    {
        public static final Update SKIP = new Update(Kind.SKIP, null);

        public enum Kind
        {SKIP, OVERRIDE, APPEND, REMOVE}

        public final Kind kind;
        public final @Nullable ByteBuffer value;

        private Update(Kind kind, @Nullable ByteBuffer value)
        {
            this.kind = kind;
            this.value = value;
        }

        public static Update override(@Nullable ByteBuffer value)
        {
            return new Update(Kind.OVERRIDE, value);
        }

        public static Update append(ByteBuffer value)
        {
            return new Update(Kind.APPEND, value);
        }

        public static Update remove(ByteBuffer value)
        {
            return new Update(Kind.REMOVE, value);
        }

        @Override
        public String toString()
        {
            return kind.name() + "(" + value + ")";
        }
    }

    private static class Writes
    {
        final Map<Symbol, ByteBuffer> regular, multicell;

        private Writes(Map<Symbol, ByteBuffer> regular, Map<Symbol, ByteBuffer> multicell)
        {
            this.regular = regular;
            this.multicell = multicell;
        }
    }

    private Writes buildWrites(Map<Symbol, MultiCell> multiCells, long ts, Map<Symbol, Update> values)
    {
        Map<Symbol, ByteBuffer> regular = new HashMap<>();
        Map<Symbol, ByteBuffer> multicell = new HashMap<>();
        for (var e : values.entrySet())
        {
            Update update = e.getValue();
            ByteBuffer value;
            if (multiCells.containsKey(e.getKey()))
            {
                MultiCell multiCell = multiCells.get(e.getKey());
                switch (update.kind)
                {
                    case SKIP:      continue;
                    case OVERRIDE:  value = multiCell.override(ts, update.value); break;
                    case APPEND:    value = multiCell.append(ts, update.value); break;
                    case REMOVE:    value = multiCell.remove(ts, update.value); break;
                    default:        throw new UnsupportedOperationException(update.kind.name());
                }
                multicell.put(e.getKey(), value);
            }
            else
            {
                switch (update.kind)
                {
                    case SKIP:      continue;
                    case OVERRIDE:  value = update.value; break;
                    default:        throw new UnsupportedOperationException(update.kind.name());
                }
                regular.put(e.getKey(), value);
            }
        }
        return new Writes(regular, multicell);
    }

    public void setStaticColumns(long ts, Map<Symbol, Update> values)
    {
        if (factory.staticColumns.isEmpty() || values.isEmpty())
            throw new IllegalStateException("Attempt to write to static columns; but they do not exist");

        var writes = buildWrites(staticMultiCell, ts, values);
        state.writeStatic(toDescriptor(factory.staticColumns, writes.regular), ts);
        state.writeStatic(toDescriptor(factory.staticColumns, writes.multicell), MagicConstants.NO_TIMESTAMP);
        validate();
    }

    public void setColumns(Clustering<ByteBuffer> clustering, long ts, Map<Symbol, Update> values, boolean writePrimaryKeyLiveness)
    {
        long cd = factory.clusteringCache.deflate(clustering);
        Map<Symbol, MultiCell> multiCells = rowMultiCell == null
                                            ? Map.of()
                                            : rowMultiCell.computeIfAbsent(cd, i -> buildMultiCellState(factory.regularColumns));
        var writes = buildWrites(multiCells, ts, values);
        state.writeRegular(cd, toDescriptor(factory.regularColumns, writes.regular), ts, writePrimaryKeyLiveness);
        state.writeRegular(cd, toDescriptor(factory.regularColumns, writes.multicell), MagicConstants.NO_TIMESTAMP, writePrimaryKeyLiveness);

        // UDT's have the ability to "update" that triggers a delete; this allows creating an "empty" row.
        // When an empty row exists without liveness info, then purge the row
        var row = state.rows.get(cd);
        if (row.isEmpty() && !row.hasPrimaryKeyLivenessInfo)
            deleteRow(cd, ts);
    }

    private long[] toDescriptor(ImmutableUniqueList<Symbol> positions, Map<Symbol, ByteBuffer> values)
    {
        long[] vds = new long[positions.size()];
        for (int i = 0; i < positions.size(); i++)
        {
            Symbol column = positions.get(i);
            if (values.containsKey(column))
            {
                ByteBuffer value = values.get(column);
                // user type is the only multi cell type that allows <empty> so this check should be fine; can expand if we find more cases
                if (value == null || !value.hasRemaining() && (column.type().isUDT() && column.type().isMultiCell()))
                {
                    vds[i] = MagicConstants.NIL_DESCR;
                    continue;
                }
                vds[i] = factory.valueCache.deflate(new Value(column.type(), value));
            }
            else
            {
                vds[i] = MagicConstants.UNSET_DESCR;
            }
        }
        return vds;
    }

    private ByteBuffer[] fromDescriptor(ImmutableUniqueList<Symbol> positions, long[] values)
    {
        if (positions.size() != values.length)
            throw new IllegalArgumentException(String.format("Attempted to extract values but expected columns didn't match;  expected %s, but given %d values", positions, values.length));
        ByteBuffer[] bbs = new ByteBuffer[values.length];
        for (int i = 0; i < bbs.length; i++)
        {
            long vd = values[i];
            if (vd == MagicConstants.NIL_DESCR)
            {
                bbs[i] = null;
            }
            else
            {
                var value = factory.valueCache.inflate(vd);
                Symbol column = positions.get(i);
                if (!value.type.equals(column.type()))
                    throw new IllegalStateException(String.format("Given value descriptor %d that maps to the wrong type; expected %s, given %s", vd, column.type().asCQL3Type(), value.type.asCQL3Type()));
                bbs[i] = value.value;
            }
        }
        return bbs;
    }

    public int size()
    {
        return state.rows().size();
    }

    public boolean isEmpty()
    {
        return state.rows().isEmpty();
    }

    public boolean staticOnly()
    {
        return isEmpty() && !factory.staticColumns.isEmpty() && !staticRow().isEmpty();
    }

    @Nullable
    public Row get(Clustering<ByteBuffer> clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return staticRow();
        long cd = factory.clusteringCache.deflateOrUndefined(clustering);
        if (cd == MagicConstants.UNSET_DESCR)
            return null;
        PartitionState.RowState rowState = state.rows().get(cd);
        if (rowState == null)
            return null;
        return toRow(rowState);
    }

    @Nullable
    public ByteBuffer get(Clustering<ByteBuffer> clustering, Symbol column)
    {
        Row row = get(clustering);
        return row == null ? null : row.get(column);
    }

    public long timestamp(Clustering<ByteBuffer> clustering, Symbol column)
    {
        Row row = get(clustering);
        return row == null ? MagicConstants.NO_TIMESTAMP : row.timestamp(column);
    }

    private Row toRow(PartitionState.RowState rowState)
    {
        Clustering<ByteBuffer> clustering;
        ByteBuffer[] values;
        if (PartitionState.STATIC_CLUSTERING == rowState.cd)
        {
            clustering = Clustering.STATIC_CLUSTERING;
            values = fromDescriptor(factory.staticColumns, rowState.vds);
        }
        else
        {
            clustering = factory.clusteringCache.inflate(rowState.cd);
            values = fromDescriptor(factory.regularColumns, rowState.vds);
        }
        return new Row(clustering, values, rowState.lts);
    }

    public Collection<Row> rows()
    {
        return state.rows().values().stream().map(this::toRow).collect(Collectors.toList());
    }

    public NavigableSet<Clustering<ByteBuffer>> clusteringKeys()
    {
        NavigableSet<Clustering<ByteBuffer>> navigableSet = new TreeSet<>(factory.clusteringComparator);
        state.rows().keySet().stream().map(factory.clusteringCache::inflate).forEach(navigableSet::add);
        return navigableSet;
    }

    public Row staticRow()
    {
        return toRow(state.staticRow());
    }

    public boolean shouldDelete()
    {
        return state.shouldDelete();
    }

    static List<String> asCQL(List<Symbol> columns, ByteBuffer[] row)
    {
        List<String> cql = new ArrayList<>(row.length);
        for (int i = 0; i < row.length; i++)
            cql.add(columns.get(i).type().toCQLString(row[i]));
        return cql;
    }

    private static void appendValues(StringBuilder sb, List<Symbol> columns, Clustering<ByteBuffer> key)
    {
        if (columns.isEmpty())
        {
            sb.append(key == Clustering.STATIC_CLUSTERING ? "STATIC" : "EMPTY");
            return;
        }
        List<String> names = columns.stream().map(Symbol::toCQL).collect(Collectors.toList());
        List<String> values = asCQL(columns, key.getBufferArray());
        if (names.size() > 1)
            sb.append('(');
        for (int i = 0; i < names.size(); i++)
            sb.append(names.get(i)).append('=').append(values.get(i)).append(", ");
        sb.setLength(sb.length() - 2); // ", " = 2 chars
        if (names.size() > 1)
            sb.append(')');
    }

    public class PrimaryKey implements Comparable<PrimaryKey>
    {
        public final BytesPartitionState.Ref partition;
        @Nullable
        public final Clustering<ByteBuffer> clustering;

        public PrimaryKey(BytesPartitionState.Ref partition, @Nullable Clustering<ByteBuffer> clustering)
        {
            this.partition = partition;
            this.clustering = clustering;
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            int rc = partition.compareTo(o.partition);
            if (rc != 0) return rc;
            if (clustering == null) return rc; // if the partition matches, and clustering is null (partition doesn't have rows) then it would be a bug if o.clustering was non-null
            rc = factory.clusteringComparator.compare(clustering, o.clustering);
            return rc;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKey that = (PrimaryKey) o;
            return partition.equals(that.partition) && Objects.equals(clustering, that.clustering);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partition, clustering);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("(partition=");
            sb.append(partition);
            sb.append(", clustering=");
            appendValues(sb, factory.clusteringColumns, clustering);
            sb.append(')');
            return sb.toString();
        }
    }

    public static class Ref implements Comparable<Ref>
    {
        private final Factory factory;
        @Nullable
        public final Clustering<ByteBuffer> key;
        public final Token token;
        // when true (null, token) > (key, token).  When false (null, token) < (key, token)
        private final boolean nullKeyGtMatchingToken;

        private Ref(Factory factory, Clustering<ByteBuffer> key, Token token)
        {
            this.factory = factory;
            this.key = key;
            this.token = token;
            this.nullKeyGtMatchingToken = true;
        }

        private Ref(Factory factory, Token token, boolean nullKeyGtMatchingToken)
        {
            this.factory = factory;
            this.key = null;
            this.token = token;
            this.nullKeyGtMatchingToken = nullKeyGtMatchingToken;
        }

        @Override
        public int compareTo(Ref o)
        {
            int rc = token.compareTo(o.token);
            if (rc != 0) return rc;
            // when key is null, this is a token match lookup...
            if (key == null)
                return nullKeyGtMatchingToken ? 1 : -1;
            if (o.key == null)
                return nullKeyGtMatchingToken ? -1 : 1;
            for (int i = 0; i < key.size(); i++)
            {
                ByteBuffer self = key.bufferAt(i);
                ByteBuffer other = o.key.bufferAt(i);
                //TODO (correctness): what is the tie breaker?
                rc = FastByteOperations.compareUnsigned(self, other);
                if (rc != 0) return rc;
            }
            return 0;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Ref ref = (Ref) o;
            return Objects.equals(key, ref.key) && token.equals(ref.token);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, token);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            if (key == null)
            {
                sb.append("null");
            }
            else
            {
                appendValues(sb, factory.partitionColumns, key);
            }
            sb.append(", token=").append(token);
            sb.append(')');
            return sb.toString();
        }
    }

    public class Row
    {
        public final Clustering<ByteBuffer> clustering;
        private final ImmutableUniqueList<Symbol> columnNames;
        private final ByteBuffer[] columns;
        private final long[] lts;

        private Row(Clustering<ByteBuffer> clustering, ByteBuffer[] columns, long[] lts)
        {
            this.clustering = clustering;
            this.columnNames = clustering == Clustering.STATIC_CLUSTERING ? factory.staticColumns : factory.regularColumns;
            this.columns = columns;
            this.lts = lts;
        }

        public ByteBuffer get(Symbol col)
        {
            return columns[columnNames.indexOf(col)];
        }

        public ByteBuffer get(int offset)
        {
            return columns[offset];
        }

        public long timestamp(Symbol col)
        {
            return lts[columnNames.indexOf(col)];
        }

        public long timestamp(int offset)
        {
            return lts[offset];
        }

        public PrimaryKey ref()
        {
            return new PrimaryKey(BytesPartitionState.this.ref(), clustering);
        }

        public boolean isEmpty()
        {
            return Stream.of(columns).allMatch(b -> b == null );
        }
    }

    public static class Factory
    {
        public final TableMetadata metadata;
        public final ImmutableUniqueList<Symbol> partitionColumns;
        public final ImmutableUniqueList<Symbol> clusteringColumns;
        public final ImmutableUniqueList<Symbol> primaryColumns;
        public final ImmutableUniqueList<Symbol> staticColumns;
        public final ImmutableUniqueList<Symbol> regularColumns;
        public final ImmutableUniqueList<Symbol> selectionOrder, partitionAndStaticColumns, regularAndStaticColumns;
        public final ClusteringComparator clusteringComparator;


        // translation layer for harry interop
        private final BijectionCache<Clustering<ByteBuffer>> partitionCache = new BijectionCache<>(Reject.instance.as());
        private final BijectionCache<Clustering<ByteBuffer>> clusteringCache;
        private final BijectionCache<Value> valueCache = new BijectionCache<>((l, r) -> {
            if (!l.type.equals(r.type))
                throw new IllegalArgumentException("Unable to compare different types: " + l.type.asCQL3Type() + " != " + r.type.asCQL3Type());
            // Cells resolve based off unsigned byte order and not type order
            return ByteBufferUtil.compareUnsigned(l.value, r.value);
        });
        private final ValueGenerators<Clustering<ByteBuffer>, Clustering<ByteBuffer>> valueGenerators;

        public Factory(TableMetadata metadata)
        {
            this.metadata = metadata;
            ImmutableUniqueList.Builder<Symbol> symbolListBuilder = ImmutableUniqueList.builder();
            for (ColumnMetadata pk : metadata.partitionKeyColumns())
                symbolListBuilder.add(Symbol.from(pk));
            partitionColumns = symbolListBuilder.buildAndClear();
            for (ColumnMetadata pk : metadata.clusteringColumns())
                symbolListBuilder.add(Symbol.from(pk));
            clusteringColumns = symbolListBuilder.buildAndClear();
            if (clusteringColumns.isEmpty()) primaryColumns = partitionColumns;
            else
            {
                symbolListBuilder.addAll(partitionColumns);
                symbolListBuilder.addAll(clusteringColumns);
                primaryColumns = symbolListBuilder.buildAndClear();
            }
            for (ColumnMetadata pk : metadata.staticColumns())
                symbolListBuilder.add(Symbol.from(pk));
            staticColumns = symbolListBuilder.buildAndClear();
            if (staticColumns.isEmpty()) partitionAndStaticColumns = partitionColumns;
            else
            {
                symbolListBuilder.addAll(partitionColumns);
                symbolListBuilder.addAll(staticColumns);
                partitionAndStaticColumns = symbolListBuilder.buildAndClear();
            }
            for (ColumnMetadata pk : metadata.regularColumns())
                symbolListBuilder.add(Symbol.from(pk));
            regularColumns = symbolListBuilder.buildAndClear();
            metadata.allColumnsInSelectOrder().forEachRemaining(cm -> symbolListBuilder.add(Symbol.from(cm)));
            selectionOrder = symbolListBuilder.buildAndClear();
            metadata.regularAndStaticColumns().forEach(cm -> symbolListBuilder.add(Symbol.from(cm)));
            regularAndStaticColumns = symbolListBuilder.buildAndClear();

            clusteringComparator = new ClusteringComparator(clusteringColumns.stream().map(Symbol::rawType).collect(Collectors.toList()));

            List<Comparator<Object>> pkComparators = new ArrayList<>(partitionColumns.size());
            for (var p : partitionColumns)
                pkComparators.add(compareBytes(p.type()));
            List<Comparator<Object>> ckComparators = new ArrayList<>(clusteringColumns.size());
            for (var c : clusteringColumns)
                ckComparators.add(compareBytes(c.rawType()));
            List<Bijections.Bijection<?>> regularColumnGens = new ArrayList<>(regularColumns.size());
            List<Comparator<Object>> regularComparators = new ArrayList<>(regularColumns.size());
            for (var r : regularColumns)
            {
                regularColumnGens.add(valueCache);
                regularComparators.add(compareValue(r.type()));
            }
            List<Bijections.Bijection<?>> staticColumnGens = new ArrayList<>(staticColumns.size());
            List<Comparator<Object>> staticComparators = new ArrayList<>(staticColumns.size());
            for (var s : staticColumns)
            {
                staticColumnGens.add(valueCache);
                staticComparators.add(compareValue(s.type()));
            }

            clusteringCache = new BijectionCache<>(clusteringComparator);

            ValueGenerators.Accessor<Clustering<ByteBuffer>> clusteringAccessor = (offset, clustering) -> clustering.bufferAt(offset);
            valueGenerators = new ValueGenerators<>(partitionCache, clusteringCache, clusteringAccessor,
                                                    regularColumnGens, staticColumnGens,
                                                    pkComparators, ckComparators,
                                                    regularComparators, staticComparators);
        }

        private Comparator<Object> compareValue(AbstractType<?> type)
        {
            return (a, b) -> {
                Value av = (Value) a;
                Value bv = (Value) b;
                if (!av.type.equals(type))
                    throw new IllegalArgumentException(String.format("Attempted to compare values of the wrong type; expected %s, actual %s", type.asCQL3Type(), av.type.asCQL3Type()));
                if (!bv.type.equals(type))
                    throw new IllegalArgumentException(String.format("Attempted to compare values of the wrong type; expected %s, actual %s", type.asCQL3Type(), bv.type.asCQL3Type()));
                return type.compare(av.value, bv.value);
            };
        }

        private Comparator<Object> compareBytes(AbstractType<?> type)
        {
            return (a, b) -> type.compare((ByteBuffer) a, (ByteBuffer) b);
        }

        public BytesPartitionState create(Clustering<ByteBuffer> key)
        {
            return new BytesPartitionState(this, key);
        }

        public BytesPartitionState.Ref createRef(Clustering<ByteBuffer> key)
        {
            Token token = metadata.partitioner.getToken(key.serializeAsPartitionKey());
            return new Ref(this, key, token);
        }

        /**
         * Define a ref where the {@link Ref#key} is {@code null}, and the ordering of this ref is that (null, token) is either before (key, token) or after; depending on {@code nullKeyGtMatchingToken}
         *
         * @param token for the ref
         * @param nullKeyGtMatchingToken when true (null, token) > (key, token).  When false (null, token) < (key, token)
         */
        public BytesPartitionState.Ref createRef(Token token, boolean nullKeyGtMatchingToken)
        {
            return new BytesPartitionState.Ref(this, token, nullKeyGtMatchingToken);
        }

        private PartitionState partitionState(Clustering<ByteBuffer> key)
        {
            return new PartitionState(partitionCache.deflate(key), valueGenerators);
        }

        public void clear()
        {
            valueCache.clear();
            clusteringCache.clear();
            partitionCache.clear();
        }
    }

    private static class Value
    {
        final AbstractType<?> type;
        final ByteBuffer value;

        private Value(AbstractType<?> type, ByteBuffer value)
        {
            this.type = Objects.requireNonNull(type);
            this.value = Objects.requireNonNull(value);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Value value1 = (Value) o;
            return type.equals(value1.type) && value.equals(value1.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, value);
        }

        @Override
        public String toString()
        {
            return type.asCQL3Type().toCQLLiteral(value);
        }
    }

    private enum Reject implements Comparator<Object>
    {
        instance;

        <T> Comparator<T> as()
        {
            return (Comparator<T>) this;
        }

        @Override
        public int compare(Object o1, Object o2)
        {
            throw new UnsupportedOperationException();
        }
    }
}
