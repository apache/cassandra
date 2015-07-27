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
import java.util.*;
import java.util.function.Predicate;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;

/**
 * An immutable and sorted list of (non-PK) columns for a given table.
 * <p>
 * Note that in practice, it will either store only static columns, or only regular ones. When
 * we need both type of columns, we use a {@link PartitionColumns} object.
 */
public class Columns implements Iterable<ColumnDefinition>
{
    public static final Serializer serializer = new Serializer();
    public static final Columns NONE = new Columns(BTree.empty(), 0);
    public static final ColumnDefinition FIRST_COMPLEX = new ColumnDefinition("", "", ColumnIdentifier.getInterned(ByteBufferUtil.EMPTY_BYTE_BUFFER, UTF8Type.instance),
                                                                              SetType.getInstance(UTF8Type.instance, true), null, ColumnDefinition.Kind.REGULAR);

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
    public static Columns of(ColumnDefinition c)
    {
        return new Columns(BTree.singleton(c), c.isComplex() ? 0 : 1);
    }

    /**
     * Returns a new {@code Columns} object holing the same columns than the provided set.
     *
     * @param s the set from which to create the new {@code Columns}.
     * @return the newly created {@code Columns} containing the columns from {@code s}.
     */
    public static Columns from(Set<ColumnDefinition> s)
    {
        Object[] tree = BTree.<ColumnDefinition>builder(Comparator.naturalOrder()).addAll(s).build();
        return new Columns(tree, findFirstComplexIdx(tree));
    }

    private static int findFirstComplexIdx(Object[] tree)
    {
        // have fast path for common no-complex case
        int size = BTree.size(tree);
        if (!BTree.isEmpty(tree) && BTree.<ColumnDefinition>findByIndex(tree, size - 1).isSimple())
            return size;
        return BTree.ceilIndex(tree, Comparator.naturalOrder(), FIRST_COMPLEX);
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
    public int columnCount()
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
    public ColumnDefinition getSimple(int i)
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
    public ColumnDefinition getComplex(int i)
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
    public int simpleIdx(ColumnDefinition c)
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
    public int complexIdx(ColumnDefinition c)
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
    public boolean contains(ColumnDefinition c)
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

        Object[] tree = BTree.<ColumnDefinition>merge(this.columns, other.columns, Comparator.naturalOrder());
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
    public boolean contains(Columns other)
    {
        if (other.columns.length > columns.length)
            return false;

        BTreeSearchIterator<ColumnDefinition, ColumnDefinition> iter = BTree.slice(columns, Comparator.naturalOrder(), BTree.Dir.ASC);
        for (ColumnDefinition def : BTree.<ColumnDefinition>iterable(other.columns))
            if (iter.next(def) == null)
                return false;
        return true;
    }

    /**
     * Iterator over the simple columns of this object.
     *
     * @return an iterator over the simple columns of this object.
     */
    public Iterator<ColumnDefinition> simpleColumns()
    {
        return BTree.iterator(columns, 0, complexIdx - 1, BTree.Dir.ASC);
    }

    /**
     * Iterator over the complex columns of this object.
     *
     * @return an iterator over the complex columns of this object.
     */
    public Iterator<ColumnDefinition> complexColumns()
    {
        return BTree.iterator(columns, complexIdx, BTree.size(columns) - 1, BTree.Dir.ASC);
    }

    /**
     * Iterator over all the columns of this object.
     *
     * @return an iterator over all the columns of this object.
     */
    public Iterator<ColumnDefinition> iterator()
    {
        return BTree.iterator(columns);
    }

    /**
     * An iterator that returns the columns of this object in "select" order (that
     * is in global alphabetical order, where the "normal" iterator returns simple
     * columns first and the complex second).
     *
     * @return an iterator returning columns in alphabetical order.
     */
    public Iterator<ColumnDefinition> selectOrderIterator()
    {
        // In wildcard selection, we want to return all columns in alphabetical order,
        // irregarding of whether they are complex or not
        return Iterators.<ColumnDefinition>mergeSorted(ImmutableList.of(simpleColumns(), complexColumns()),
                                     (s, c) -> s.name.compareTo(c.name));
    }

    /**
     * Returns the equivalent of those columns but with the provided column removed.
     *
     * @param column the column to remove.
     *
     * @return newly allocated columns containing all the columns of {@code this} expect
     * for {@code column}.
     */
    public Columns without(ColumnDefinition column)
    {
        if (!contains(column))
            return this;

        Object[] newColumns = BTree.<ColumnDefinition>transformAndFilter(columns, (c) -> c.equals(column) ? null : c);
        return new Columns(newColumns);
    }

    /**
     * Returns a predicate to test whether columns are included in this {@code Columns} object,
     * assuming that tes tested columns are passed to the predicate in sorted order.
     *
     * @return a predicate to test the inclusion of sorted columns in this object.
     */
    public Predicate<ColumnDefinition> inOrderInclusionTester()
    {
        SearchIterator<ColumnDefinition, ColumnDefinition> iter = BTree.slice(columns, Comparator.naturalOrder(), BTree.Dir.ASC);
        return column -> iter.next(column) != null;
    }

    public void digest(MessageDigest digest)
    {
        for (ColumnDefinition c : this)
            digest.update(c.name.bytes.duplicate());
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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (ColumnDefinition def : this)
        {
            if (first) first = false; else sb.append(" ");
            sb.append(def.name);
        }
        return sb.toString();
    }

    public static class Serializer
    {
        public void serialize(Columns columns, DataOutputPlus out) throws IOException
        {
            out.writeVInt(columns.columnCount());
            for (ColumnDefinition column : columns)
                ByteBufferUtil.writeWithVIntLength(column.name.bytes, out);
        }

        public long serializedSize(Columns columns)
        {
            long size = TypeSizes.sizeofVInt(columns.columnCount());
            for (ColumnDefinition column : columns)
                size += ByteBufferUtil.serializedSizeWithVIntLength(column.name.bytes);
            return size;
        }

        public Columns deserialize(DataInputPlus in, CFMetaData metadata) throws IOException
        {
            int length = (int)in.readVInt();
            BTree.Builder<ColumnDefinition> builder = BTree.builder(Comparator.naturalOrder());
            builder.auto(false);
            for (int i = 0; i < length; i++)
            {
                ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
                ColumnDefinition column = metadata.getColumnDefinition(name);
                if (column == null)
                {
                    // If we don't find the definition, it could be we have data for a dropped column, and we shouldn't
                    // fail deserialization because of that. So we grab a "fake" ColumnDefinition that ensure proper
                    // deserialization. The column will be ignore later on anyway.
                    column = metadata.getDroppedColumnDefinition(name);
                    if (column == null)
                        throw new RuntimeException("Unknown column " + UTF8Type.instance.getString(name) + " during deserialization");
                }
                builder.add(column);
            }
            return new Columns(builder.build());
        }
    }
}
