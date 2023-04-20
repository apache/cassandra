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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeRemoval;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;

/**
 * An immutable and sorted list of (non-PK) columns for a given table.
 * <p>
 * Note that in practice, it will either store only static columns, or only regular ones. When
 * we need both type of columns, we use a {@link RegularAndStaticColumns} object.
 */
public class Columns extends AbstractCollection<ColumnMetadata> implements Collection<ColumnMetadata>
{
    public static final Serializer serializer = new Serializer();
    public static final Columns NONE = new Columns(BTree.empty(), 0);
    static final long EMPTY_SIZE = ObjectSizes.measure(NONE);

    public static final ColumnMetadata FIRST_COMPLEX_STATIC =
        new ColumnMetadata("",
                           "",
                           ColumnIdentifier.getInterned(ByteBufferUtil.EMPTY_BYTE_BUFFER, UTF8Type.instance),
                           SetType.getInstance(UTF8Type.instance, true),
                           ColumnMetadata.NO_POSITION,
                           ColumnMetadata.Kind.STATIC,
                           null);

    public static final ColumnMetadata FIRST_COMPLEX_REGULAR =
        new ColumnMetadata("",
                           "",
                           ColumnIdentifier.getInterned(ByteBufferUtil.EMPTY_BYTE_BUFFER, UTF8Type.instance),
                           SetType.getInstance(UTF8Type.instance, true),
                           ColumnMetadata.NO_POSITION,
                           ColumnMetadata.Kind.REGULAR,
                           null);

    private final Object[] columns;
    private final int complexIdx; // Index of the first complex column

    private Columns(Object[] columns, int complexIdx)
    {
        assert complexIdx <= BTree.size(columns);
        this.columns = columns;
        this.complexIdx = complexIdx;
    }

    private Columns(Object[] columns)
    {
        this(columns, findFirstComplexIdx(columns));
    }

    /**
     * Creates a {@code Columns} holding only the one column provided.
     *
     * @param c the column for which to create a {@code Columns} object.
     *
     * @return the newly created {@code Columns} containing only {@code c}.
     */
    public static Columns of(ColumnMetadata c)
    {
        return new Columns(BTree.singleton(c), c.isComplex() ? 0 : 1);
    }

   /**
    * Returns a new {@code Columns} object holing the same columns as the provided Row.
    *
    * @param row the row from which to create the new {@code Columns}.
    * @return the newly created {@code Columns} containing the columns from {@code row}.
    */
   public static Columns from(Row row)
   {
       try (BTree.FastBuilder<ColumnMetadata> builder = BTree.fastBuilder())
       {
           for (ColumnData cd : row)
               builder.add(cd.column());
           Object[] tree = builder.build();
           return new Columns(tree, findFirstComplexIdx(tree));
       }
   }

   public static Columns from(BTree.Builder<ColumnMetadata> builder)
   {
       Object[] tree = builder.build();
       return new Columns(tree, findFirstComplexIdx(tree));
   }

    /**
    * Returns a new {@code Columns} object holding the same columns than the provided set.
     * This method assumes nothing about the order of {@code s}.
     *
     * @param s the set from which to create the new {@code Columns}.
     * @return the newly created {@code Columns} containing the columns from {@code s}.
     */
    public static Columns from(Collection<ColumnMetadata> s)
    {
        Object[] tree = BTree.<ColumnMetadata>builder(Comparator.naturalOrder()).addAll(s).build();
        return new Columns(tree, findFirstComplexIdx(tree));
    }

    private static int findFirstComplexIdx(Object[] tree)
    {
        if (BTree.isEmpty(tree))
            return 0;

        int size = BTree.size(tree);
        ColumnMetadata last = BTree.findByIndex(tree, size - 1);
        return last.isSimple()
             ? size
             : BTree.ceilIndex(tree, Comparator.naturalOrder(), last.isStatic() ? FIRST_COMPLEX_STATIC : FIRST_COMPLEX_REGULAR);
    }

    /**
     * Whether this columns is empty.
     *
     * @return whether this columns is empty.
     */
    public boolean isEmpty()
    {
        return BTree.isEmpty(columns);
    }

    /**
     * The number of simple columns in this object.
     *
     * @return the number of simple columns in this object.
     */
    public int simpleColumnCount()
    {
        return complexIdx;
    }

    /**
     * The number of complex columns (non-frozen collections, udts, ...) in this object.
     *
     * @return the number of complex columns in this object.
     */
    public int complexColumnCount()
    {
        return BTree.size(columns) - complexIdx;
    }

    /**
     * The total number of columns in this object.
     *
     * @return the total number of columns in this object.
     */
    public int size()
    {
        return BTree.size(columns);
    }

    /**
     * Whether this objects contains simple columns.
     *
     * @return whether this objects contains simple columns.
     */
    public boolean hasSimple()
    {
        return complexIdx > 0;
    }

    /**
     * Whether this objects contains complex columns.
     *
     * @return whether this objects contains complex columns.
     */
    public boolean hasComplex()
    {
        return complexIdx < BTree.size(columns);
    }

    /**
     * Returns the ith simple column of this object.
     *
     * @param i the index for the simple column to fectch. This must
     * satisfy {@code 0 <= i < simpleColumnCount()}.
     *
     * @return the {@code i}th simple column in this object.
     */
    public ColumnMetadata getSimple(int i)
    {
        return BTree.findByIndex(columns, i);
    }

    /**
     * Returns the ith complex column of this object.
     *
     * @param i the index for the complex column to fectch. This must
     * satisfy {@code 0 <= i < complexColumnCount()}.
     *
     * @return the {@code i}th complex column in this object.
     */
    public ColumnMetadata getComplex(int i)
    {
        return BTree.findByIndex(columns, complexIdx + i);
    }

    /**
     * The index of the provided simple column in this object (if it contains
     * the provided column).
     *
     * @param c the simple column for which to return the index of.
     *
     * @return the index for simple column {@code c} if it is contains in this
     * object
     */
    public int simpleIdx(ColumnMetadata c)
    {
        return BTree.findIndex(columns, Comparator.naturalOrder(), c);
    }

    /**
     * The index of the provided complex column in this object (if it contains
     * the provided column).
     *
     * @param c the complex column for which to return the index of.
     *
     * @return the index for complex column {@code c} if it is contains in this
     * object
     */
    public int complexIdx(ColumnMetadata c)
    {
        return BTree.findIndex(columns, Comparator.naturalOrder(), c) - complexIdx;
    }

    /**
     * Whether the provided column is contained by this object.
     *
     * @param c the column to check presence of.
     *
     * @return whether {@code c} is contained by this object.
     */
    public boolean contains(ColumnMetadata c)
    {
        return BTree.findIndex(columns, Comparator.naturalOrder(), c) >= 0;
    }

    /**
     * Returns the result of merging this {@code Columns} object with the
     * provided one.
     *
     * @param other the other {@code Columns} to merge this object with.
     *
     * @return the result of merging/taking the union of {@code this} and
     * {@code other}. The returned object may be one of the operand and that
     * operand is a subset of the other operand.
     */
    public Columns mergeTo(Columns other)
    {
        if (this == other || other == NONE)
            return this;
        if (this == NONE)
            return other;

        Object[] tree = BTree.update(this.columns, other.columns, Comparator.naturalOrder());
        if (tree == this.columns)
            return this;
        if (tree == other.columns)
            return other;

        return new Columns(tree, findFirstComplexIdx(tree));
    }

    /**
     * Whether this object is a superset of the provided other {@code Columns object}.
     *
     * @param other the other object to test for inclusion in this object.
     *
     * @return whether all the columns of {@code other} are contained by this object.
     */
    public boolean containsAll(Collection<?> other)
    {
        if (other == this)
            return true;
        if (other.size() > this.size())
            return false;

        BTreeSearchIterator<ColumnMetadata, ColumnMetadata> iter = BTree.slice(columns, Comparator.naturalOrder(), BTree.Dir.ASC);
        for (Object def : other)
            if (iter.next((ColumnMetadata) def) == null)
                return false;
        return true;
    }

    /**
     * Iterator over the simple columns of this object.
     *
     * @return an iterator over the simple columns of this object.
     */
    public Iterator<ColumnMetadata> simpleColumns()
    {
        return BTree.iterator(columns, 0, complexIdx - 1, BTree.Dir.ASC);
    }

    /**
     * Iterator over the complex columns of this object.
     *
     * @return an iterator over the complex columns of this object.
     */
    public Iterator<ColumnMetadata> complexColumns()
    {
        return BTree.iterator(columns, complexIdx, BTree.size(columns) - 1, BTree.Dir.ASC);
    }

    /**
     * Iterator over all the columns of this object.
     *
     * @return an iterator over all the columns of this object.
     */
    public BTreeSearchIterator<ColumnMetadata, ColumnMetadata> iterator()
    {
        return BTree.<ColumnMetadata, ColumnMetadata>slice(columns, Comparator.naturalOrder(), BTree.Dir.ASC);
    }

    /**
     * An iterator that returns the columns of this object in "select" order (that
     * is in global alphabetical order, where the "normal" iterator returns simple
     * columns first and the complex second).
     *
     * @return an iterator returning columns in alphabetical order.
     */
    public Iterator<ColumnMetadata> selectOrderIterator()
    {
        // In wildcard selection, we want to return all columns in alphabetical order,
        // irregarding of whether they are complex or not
        return Iterators.<ColumnMetadata>
                         mergeSorted(ImmutableList.of(simpleColumns(), complexColumns()),
                                     (s, c) ->
                                     {
                                         assert !s.kind.isPrimaryKeyKind();
                                         return s.name.bytes.compareTo(c.name.bytes);
                                     });
    }

    /**
     * Returns the equivalent of those columns but with the provided column removed.
     *
     * @param column the column to remove.
     *
     * @return newly allocated columns containing all the columns of {@code this} expect
     * for {@code column}.
     */
    public Columns without(ColumnMetadata column)
    {
        if (!contains(column))
            return this;

        Object[] newColumns = BTreeRemoval.<ColumnMetadata>remove(columns, Comparator.naturalOrder(), column);
        return new Columns(newColumns);
    }

    /**
     * Returns a predicate to test whether columns are included in this {@code Columns} object,
     * assuming that tes tested columns are passed to the predicate in sorted order.
     *
     * @return a predicate to test the inclusion of sorted columns in this object.
     */
    public Predicate<ColumnMetadata> inOrderInclusionTester()
    {
        SearchIterator<ColumnMetadata, ColumnMetadata> iter = BTree.slice(columns, Comparator.naturalOrder(), BTree.Dir.ASC);
        return column -> iter.next(column) != null;
    }

    public void digest(Digest digest)
    {
        for (ColumnMetadata c : this)
            digest.update(c.name.bytes);
    }

    /**
     * Apply a function to each column definition in forwards or reversed order.
     * @param function
     */
    public void apply(Consumer<ColumnMetadata> function)
    {
        BTree.apply(columns, function);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this)
            return true;
        if (!(other instanceof Columns))
            return false;

        Columns that = (Columns)other;
        return this.complexIdx == that.complexIdx && BTree.equals(this.columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(complexIdx, BTree.hashCode(columns));
    }

    public long unsharedHeapSize()
    {
        if(this == NONE)
            return 0;

        return EMPTY_SIZE + BTree.sizeOfStructureOnHeap(columns);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (ColumnMetadata def : this)
        {
            if (first) first = false; else sb.append(" ");
            sb.append(def.name);
        }
        return sb.append("]").toString();
    }

    public static class Serializer
    {
        public void serialize(Columns columns, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(columns.size());
            for (ColumnMetadata column : columns)
                ByteBufferUtil.writeWithVIntLength(column.name.bytes, out);
        }

        public long serializedSize(Columns columns)
        {
            long size = TypeSizes.sizeofUnsignedVInt(columns.size());
            for (ColumnMetadata column : columns)
                size += ByteBufferUtil.serializedSizeWithVIntLength(column.name.bytes);
            return size;
        }

        public Columns deserialize(DataInputPlus in, TableMetadata metadata) throws IOException
        {
            int length = in.readUnsignedVInt32();
            try (BTree.FastBuilder<ColumnMetadata> builder = BTree.fastBuilder())
            {
                for (int i = 0; i < length; i++)
                {
                    ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
                    ColumnMetadata column = metadata.getColumn(name);
                    if (column == null)
                    {
                        // If we don't find the definition, it could be we have data for a dropped column, and we shouldn't
                        // fail deserialization because of that. So we grab a "fake" ColumnMetadata that ensure proper
                        // deserialization. The column will be ignore later on anyway.
                        column = metadata.getDroppedColumn(name);

                        if (column == null)
                            throw new RuntimeException("Unknown column " + UTF8Type.instance.getString(name) + " during deserialization");
                    }
                    builder.add(column);
                }
                return new Columns(builder.build());
            }
        }

        /**
         * If both ends have a pre-shared superset of the columns we are serializing, we can send them much
         * more efficiently. Both ends must provide the identically same set of columns.
         */
        public void serializeSubset(Collection<ColumnMetadata> columns, Columns superset, DataOutputPlus out) throws IOException
        {
            /**
             * We weight this towards small sets, and sets where the majority of items are present, since
             * we expect this to mostly be used for serializing result sets.
             *
             * For supersets with fewer than 64 columns, we encode a bitmap of *missing* columns,
             * which equates to a zero (single byte) when all columns are present, and otherwise
             * a positive integer that can typically be vint encoded efficiently.
             *
             * If we have 64 or more columns, we cannot neatly perform a bitmap encoding, so we just switch
             * to a vint encoded set of deltas, either adding or subtracting (whichever is most efficient).
             * We indicate this switch by sending our bitmap with every bit set, i.e. -1L
             */
            int columnCount = columns.size();
            int supersetCount = superset.size();
            if (columnCount == supersetCount)
            {
                out.writeUnsignedVInt32(0);
            }
            else if (supersetCount < 64)
            {
                out.writeUnsignedVInt(encodeBitmap(columns, superset, supersetCount));
            }
            else
            {
                serializeLargeSubset(columns, columnCount, superset, supersetCount, out);
            }
        }

        public long serializedSubsetSize(Collection<ColumnMetadata> columns, Columns superset)
        {
            int columnCount = columns.size();
            int supersetCount = superset.size();
            if (columnCount == supersetCount)
            {
                return TypeSizes.sizeofUnsignedVInt(0);
            }
            else if (supersetCount < 64)
            {
                return TypeSizes.sizeofUnsignedVInt(encodeBitmap(columns, superset, supersetCount));
            }
            else
            {
                return serializeLargeSubsetSize(columns, columnCount, superset, supersetCount);
            }
        }

        public Columns deserializeSubset(Columns superset, DataInputPlus in) throws IOException
        {
            long encoded = in.readUnsignedVInt();
            if (encoded == 0L)
            {
                return superset;
            }
            else if (superset.size() >= 64)
            {
                return deserializeLargeSubset(in, superset, (int) encoded);
            }
            else
            {
                try (BTree.FastBuilder<ColumnMetadata> builder = BTree.fastBuilder())
                {
                    int firstComplexIdx = 0;
                    for (ColumnMetadata column : superset)
                    {
                        if ((encoded & 1) == 0)
                        {
                            builder.add(column);
                            if (column.isSimple())
                                ++firstComplexIdx;
                        }
                        encoded >>>= 1;
                    }
                    if (encoded != 0)
                        throw new IOException("Invalid Columns subset bytes; too many bits set:" + Long.toBinaryString(encoded));
                    return new Columns(builder.build(), firstComplexIdx);
                }
            }
        }

        // encodes a 1 bit for every *missing* column, on the assumption presence is more common,
        // and because this is consistent with encoding 0 to represent all present
        private static long encodeBitmap(Collection<ColumnMetadata> columns, Columns superset, int supersetCount)
        {
            long bitmap = 0L;
            BTreeSearchIterator<ColumnMetadata, ColumnMetadata> iter = superset.iterator();
            // the index we would encounter next if all columns are present
            int expectIndex = 0;
            for (ColumnMetadata column : columns)
            {
                if (iter.next(column) == null)
                    throw new IllegalStateException(columns + " is not a subset of " + superset);

                int currentIndex = iter.indexOfCurrent();
                int count = currentIndex - expectIndex;
                // (1L << count) - 1 gives us count bits set at the bottom of the register
                // so << expectIndex moves these bits to start at expectIndex, which is where our missing portion
                // begins (assuming count > 0; if not, we're adding 0 bits, so it's a no-op)
                bitmap |= ((1L << count) - 1) << expectIndex;
                expectIndex = currentIndex + 1;
            }
            int count = supersetCount - expectIndex;
            bitmap |= ((1L << count) - 1) << expectIndex;
            return bitmap;
        }

        @DontInline
        private void serializeLargeSubset(Collection<ColumnMetadata> columns, int columnCount, Columns superset, int supersetCount, DataOutputPlus out) throws IOException
        {
            // write flag indicating we're in lengthy mode
            out.writeUnsignedVInt32(supersetCount - columnCount);
            BTreeSearchIterator<ColumnMetadata, ColumnMetadata> iter = superset.iterator();
            if (columnCount < supersetCount / 2)
            {
                // write present columns
                for (ColumnMetadata column : columns)
                {
                    if (iter.next(column) == null)
                        throw new IllegalStateException();
                    out.writeUnsignedVInt32(iter.indexOfCurrent());
                }
            }
            else
            {
                // write missing columns
                int prev = -1;
                for (ColumnMetadata column : columns)
                {
                    if (iter.next(column) == null)
                        throw new IllegalStateException();
                    int cur = iter.indexOfCurrent();
                    while (++prev != cur)
                        out.writeUnsignedVInt32(prev);
                }
                while (++prev != supersetCount)
                    out.writeUnsignedVInt32(prev);
            }
        }

        @DontInline
        private Columns deserializeLargeSubset(DataInputPlus in, Columns superset, int delta) throws IOException
        {
            int supersetCount = superset.size();
            int columnCount = supersetCount - delta;

            try (BTree.FastBuilder<ColumnMetadata> builder = BTree.fastBuilder())
            {
                if (columnCount < supersetCount / 2)
                {
                    for (int i = 0 ; i < columnCount ; i++)
                    {
                        int idx = in.readUnsignedVInt32();
                        builder.add(BTree.findByIndex(superset.columns, idx));
                    }
                }
                else
                {
                    Iterator<ColumnMetadata> iter = superset.iterator();
                    int idx = 0;
                    int skipped = 0;
                    while (true)
                    {
                        int nextMissingIndex = skipped < delta ? in.readUnsignedVInt32() : supersetCount;
                        while (idx < nextMissingIndex)
                        {
                            ColumnMetadata def = iter.next();
                            builder.add(def);
                            idx++;
                        }
                        if (idx == supersetCount)
                            break;
                        iter.next();
                        idx++;
                        skipped++;
                    }
                }
                return new Columns(builder.build());
            }
        }

        @DontInline
        private int serializeLargeSubsetSize(Collection<ColumnMetadata> columns, int columnCount, Columns superset, int supersetCount)
        {
            // write flag indicating we're in lengthy mode
            int size = TypeSizes.sizeofUnsignedVInt(supersetCount - columnCount);
            BTreeSearchIterator<ColumnMetadata, ColumnMetadata> iter = superset.iterator();
            if (columnCount < supersetCount / 2)
            {
                // write present columns
                for (ColumnMetadata column : columns)
                {
                    if (iter.next(column) == null)
                        throw new IllegalStateException();
                    size += TypeSizes.sizeofUnsignedVInt(iter.indexOfCurrent());
                }
            }
            else
            {
                // write missing columns
                int prev = -1;
                for (ColumnMetadata column : columns)
                {
                    if (iter.next(column) == null)
                        throw new IllegalStateException();
                    int cur = iter.indexOfCurrent();
                    while (++prev != cur)
                        size += TypeSizes.sizeofUnsignedVInt(prev);
                }
                while (++prev != supersetCount)
                    size += TypeSizes.sizeofUnsignedVInt(prev);
            }
            return size;
        }

    }
}
