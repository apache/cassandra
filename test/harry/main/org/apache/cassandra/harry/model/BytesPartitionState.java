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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.gen.BijectionCache;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.ImmutableUniqueList;

public class BytesPartitionState
{
    private final Factory factory;
    public final Clustering<ByteBuffer> key;
    private final Token token;
    private final PartitionState state;

    private BytesPartitionState(Factory factory, Clustering<ByteBuffer> key)
    {
        this.factory = factory;
        this.key = key;
        this.token = factory.metadata.partitioner.getToken(key.serializeAsPartitionKey());
        this.state = factory.partitionState(key);
    }

    public void deleteRow(Clustering<ByteBuffer> clustering)
    {
        long cd = factory.clusteringCache.deflateOrUndefined(clustering);
        if (MagicConstants.UNSET_DESCR == cd)
            return;
        state.delete(cd, MagicConstants.NO_TIMESTAMP);
    }

    public void deleteColumns(Clustering<ByteBuffer> clustering, Set<Symbol> columns)
    {
        long cd = factory.clusteringCache.deflateOrUndefined(clustering);
        if (cd != MagicConstants.UNSET_DESCR)
        {
            BitSet regularColumns = bitset(columns, true);
            if (!regularColumns.allUnset())
                state.deleteRegularColumns(MagicConstants.NO_TIMESTAMP, cd, regularColumns);
        }
        deleteStaticColumns(columns);
    }

    public void deleteStaticColumns(Set<Symbol> columns)
    {
        BitSet staticColumns = bitset(columns, false);
        if (!staticColumns.allUnset())
            state.deleteStaticColumns(MagicConstants.NO_TIMESTAMP, staticColumns);
    }

    private BitSet bitset(Set<Symbol> columns, boolean regular)
    {
        ImmutableUniqueList<Symbol> positions = regular ? factory.regularColumns : factory.staticColumns;
        BitSet bitSet = new BitSet.BitSet64Bit(positions.size());
        for (int i = 0; i < positions.size(); i++)
        {
            Symbol column = positions.get(i);
            if (columns.contains(column))
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

    public void setStaticColumns(Map<Symbol, ByteBuffer> values)
    {
        if (factory.staticColumns.isEmpty() || values.isEmpty())
            throw new IllegalStateException("Attempt to write to static columns; but they do not exist");
        long[] sds = toDescriptor(factory.staticColumns, values);
        state.writeStatic(sds, MagicConstants.NO_TIMESTAMP);
    }

    public void setColumns(Clustering<ByteBuffer> clustering, Map<Symbol, ByteBuffer> values, boolean writePrimaryKeyLiveness)
    {
        long cd = factory.clusteringCache.deflate(clustering);
        long[] vds = toDescriptor(factory.regularColumns, values);
        state.writeRegular(cd, vds, MagicConstants.NO_TIMESTAMP, writePrimaryKeyLiveness);
    }

    private long[] toDescriptor(ImmutableUniqueList<Symbol> positions, Map<Symbol, ByteBuffer> values)
    {
        long[] vds = new long[positions.size()];
        for (int i = 0; i < positions.size(); i++)
        {
            Symbol column = positions.get(i);
            if (values.containsKey(column))
            {
                long vd = factory.valueCache.deflate(new Value(column.type(), values.get(column)));
                vds[i] = vd;
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
        long cd = factory.clusteringCache.deflateOrUndefined(clustering);
        if (cd == MagicConstants.UNSET_DESCR)
            return null;
        PartitionState.RowState rowState = state.rows().get(cd);
        if (rowState == null)
            return null;
        return toRow(rowState);
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
        return new Row(clustering, values);
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

        private Row(Clustering<ByteBuffer> clustering, ByteBuffer[] columns)
        {
            this.clustering = clustering;
            this.columnNames = clustering == Clustering.STATIC_CLUSTERING ? factory.staticColumns : factory.regularColumns;
            this.columns = columns;
        }

        public ByteBuffer get(Symbol col)
        {
            return columns[columnNames.indexOf(col)];
        }

        public ByteBuffer get(int offset)
        {
            return columns[offset];
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
        public final ImmutableUniqueList<Symbol> staticColumns;
        public final ImmutableUniqueList<Symbol> regularColumns;
        public final ImmutableUniqueList<Symbol> selectionOrder;
        public final ClusteringComparator clusteringComparator;


        // translation layer for harry interop
        private final BijectionCache<Clustering<ByteBuffer>> partitionCache = new BijectionCache<>(Reject.instance.as());
        private final BijectionCache<Clustering<ByteBuffer>> clusteringCache;
        private final BijectionCache<Value> valueCache = new BijectionCache<>(Reject.instance.as());
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
            for (ColumnMetadata pk : metadata.staticColumns())
                symbolListBuilder.add(Symbol.from(pk));
            staticColumns = symbolListBuilder.buildAndClear();
            for (ColumnMetadata pk : metadata.regularColumns())
                symbolListBuilder.add(Symbol.from(pk));
            regularColumns = symbolListBuilder.buildAndClear();
            metadata.allColumnsInSelectOrder().forEachRemaining(cm -> symbolListBuilder.add(Symbol.from(cm)));
            selectionOrder = symbolListBuilder.buildAndClear();

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
    }

    private static class Value
    {
        final AbstractType<?> type;
        final ByteBuffer value;

        private Value(AbstractType<?> type, ByteBuffer value)
        {
            this.type = type;
            this.value = value;
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
