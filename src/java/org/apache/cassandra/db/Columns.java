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

import java.io.DataInput;
import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * An immutable and sorted list of (non-PK) columns for a given table.
 * <p>
 * Note that in practice, it will either store only static columns, or only regular ones. When
 * we need both type of columns, we use a {@link PartitionColumns} object.
 */
public class Columns implements Iterable<ColumnDefinition>
{
    public static final Serializer serializer = new Serializer();
    public static final Columns NONE = new Columns(new ColumnDefinition[0], 0);

    public final ColumnDefinition[] columns;
    public final int complexIdx; // Index of the first complex column

    private Columns(ColumnDefinition[] columns, int complexIdx)
    {
        assert complexIdx <= columns.length;
        this.columns = columns;
        this.complexIdx = complexIdx;
    }

    private Columns(ColumnDefinition[] columns)
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
        ColumnDefinition[] columns = new ColumnDefinition[]{ c };
        return new Columns(columns, c.isComplex() ? 0 : 1);
    }

    /**
     * Returns a new {@code Columns} object holing the same columns than the provided set.
     *
     * @param param s the set from which to create the new {@code Columns}.
     *
     * @return the newly created {@code Columns} containing the columns from {@code s}.
     */
    public static Columns from(Set<ColumnDefinition> s)
    {
        ColumnDefinition[] columns = s.toArray(new ColumnDefinition[s.size()]);
        Arrays.sort(columns);
        return new Columns(columns, findFirstComplexIdx(columns));
    }

    private static int findFirstComplexIdx(ColumnDefinition[] columns)
    {
        for (int i = 0; i < columns.length; i++)
            if (columns[i].isComplex())
                return i;
        return columns.length;
    }

    /**
     * Whether this columns is empty.
     *
     * @return whether this columns is empty.
     */
    public boolean isEmpty()
    {
        return columns.length == 0;
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
        return columns.length - complexIdx;
    }

    /**
     * The total number of columns in this object.
     *
     * @return the total number of columns in this object.
     */
    public int columnCount()
    {
        return columns.length;
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
        return complexIdx < columns.length;
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
        return columns[i];
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
        return columns[complexIdx + i];
    }

    /**
     * The index of the provided simple column in this object (if it contains
     * the provided column).
     *
     * @param c the simple column for which to return the index of.
     * @param from the index to start the search from.
     *
     * @return the index for simple column {@code c} if it is contains in this
     * object (starting from index {@code from}), {@code -1} otherwise.
     */
    public int simpleIdx(ColumnDefinition c, int from)
    {
        assert !c.isComplex();
        for (int i = from; i < complexIdx; i++)
            // We know we only use "interned" ColumnIdentifier so == is ok.
            if (columns[i].name == c.name)
                return i;
        return -1;
    }

    /**
     * The index of the provided complex column in this object (if it contains
     * the provided column).
     *
     * @param c the complex column for which to return the index of.
     * @param from the index to start the search from.
     *
     * @return the index for complex column {@code c} if it is contains in this
     * object (starting from index {@code from}), {@code -1} otherwise.
     */
    public int complexIdx(ColumnDefinition c, int from)
    {
        assert c.isComplex();
        for (int i = complexIdx + from; i < columns.length; i++)
            // We know we only use "interned" ColumnIdentifier so == is ok.
            if (columns[i].name == c.name)
                return i - complexIdx;
        return -1;
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
        return c.isComplex() ? complexIdx(c, 0) >= 0 : simpleIdx(c, 0) >= 0;
    }

    /**
     * Whether or not there is some counter columns within those columns.
     *
     * @return whether or not there is some counter columns within those columns.
     */
    public boolean hasCounters()
    {
        for (int i = 0; i < complexIdx; i++)
        {
            if (columns[i].type.isCounter())
                return true;
        }

        for (int i = complexIdx; i < columns.length; i++)
        {
            // We only support counter in maps because that's all we need for now (and we need it for the sake of thrift super columns of counter)
            if (columns[i].type instanceof MapType && (((MapType)columns[i].type).valueComparator().isCounter()))
                return true;
        }

        return false;
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

        int i = 0, j = 0;
        int size = 0;
        while (i < columns.length && j < other.columns.length)
        {
            ++size;
            int cmp = columns[i].compareTo(other.columns[j]);
            if (cmp == 0)
            {
                ++i;
                ++j;
            }
            else if (cmp < 0)
            {
                ++i;
            }
            else
            {
                ++j;
            }
        }

        // If every element was always counted on both array, we have the same
        // arrays for the first min elements
        if (i == size && j == size)
        {
            // We've exited because of either c1 or c2 (or both). The array that
            // made us stop is thus a subset of the 2nd one, return that array.
            return i == columns.length ? other : this;
        }

        size += i == columns.length ? other.columns.length - j : columns.length - i;
        ColumnDefinition[] result = new ColumnDefinition[size];
        i = 0;
        j = 0;
        for (int k = 0; k < size; k++)
        {
            int cmp = i >= columns.length ? 1
                    : (j >= other.columns.length ? -1 : columns[i].compareTo(other.columns[j]));
            if (cmp == 0)
            {
                result[k] = columns[i];
                ++i;
                ++j;
            }
            else if (cmp < 0)
            {
                result[k] = columns[i++];
            }
            else
            {
                result[k] = other.columns[j++];
            }
        }
        return new Columns(result, findFirstComplexIdx(result));
    }

    /**
     * Whether this object is a subset of the provided other {@code Columns object}.
     *
     * @param other the othere object to test for inclusion in this object.
     *
     * @return whether all the columns of {@code other} are contained by this object.
     */
    public boolean contains(Columns other)
    {
        if (other.columns.length > columns.length)
            return false;

        int j = 0;
        int cmp = 0;
        for (ColumnDefinition def : other.columns)
        {
            while (j < columns.length && (cmp = columns[j].compareTo(def)) < 0)
                j++;

            if (j >= columns.length || cmp > 0)
                return false;

            // cmp == 0, we've found the definition. Ce can bump j once more since
            // we know we won't need to compare that element again
            j++;
        }
        return true;
    }

    /**
     * Iterator over the simple columns of this object.
     *
     * @return an iterator over the simple columns of this object.
     */
    public Iterator<ColumnDefinition> simpleColumns()
    {
        return new ColumnIterator(0, complexIdx);
    }

    /**
     * Iterator over the complex columns of this object.
     *
     * @return an iterator over the complex columns of this object.
     */
    public Iterator<ColumnDefinition> complexColumns()
    {
        return new ColumnIterator(complexIdx, columns.length);
    }

    /**
     * Iterator over all the columns of this object.
     *
     * @return an iterator over all the columns of this object.
     */
    public Iterator<ColumnDefinition> iterator()
    {
        return Iterators.forArray(columns);
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
        return new AbstractIterator<ColumnDefinition>()
        {
            private int regular;
            private int complex = complexIdx;

            protected ColumnDefinition computeNext()
            {
                if (complex >= columns.length)
                    return regular >= complexIdx ? endOfData() : columns[regular++];
                if (regular >= complexIdx)
                    return columns[complex++];

                return ByteBufferUtil.compareUnsigned(columns[regular].name.bytes, columns[complex].name.bytes) < 0
                     ? columns[regular++]
                     : columns[complex++];
            }
        };
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
        int idx = column.isComplex() ? complexIdx(column, 0) : simpleIdx(column, 0);
        if (idx < 0)
            return this;

        int realIdx = column.isComplex() ? complexIdx + idx : idx;

        ColumnDefinition[] newColumns = new ColumnDefinition[columns.length - 1];
        System.arraycopy(columns, 0, newColumns, 0, realIdx);
        System.arraycopy(columns, realIdx + 1, newColumns, realIdx, newColumns.length - realIdx);
        return new Columns(newColumns);
    }

    public void digest(MessageDigest digest)
    {
        for (ColumnDefinition c : this)
            digest.update(c.name.bytes.duplicate());
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Columns))
            return false;

        Columns that = (Columns)other;
        return this.complexIdx == that.complexIdx && Arrays.equals(this.columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(complexIdx, Arrays.hashCode(columns));
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

    private class ColumnIterator extends AbstractIterator<ColumnDefinition>
    {
        private final int to;
        private int idx;

        private ColumnIterator(int from, int to)
        {
            this.idx = from;
            this.to = to;
        }

        protected ColumnDefinition computeNext()
        {
            if (idx >= to)
                return endOfData();
            return columns[idx++];
        }
    }

    public static class Serializer
    {
        public void serialize(Columns columns, DataOutputPlus out) throws IOException
        {
            out.writeShort(columns.columnCount());
            for (ColumnDefinition column : columns)
                ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
        }

        public long serializedSize(Columns columns, TypeSizes sizes)
        {
            long size = sizes.sizeof((short)columns.columnCount());
            for (ColumnDefinition column : columns)
                size += sizes.sizeofWithShortLength(column.name.bytes);
            return size;
        }

        public Columns deserialize(DataInput in, CFMetaData metadata) throws IOException
        {
            int length = in.readUnsignedShort();
            ColumnDefinition[] columns = new ColumnDefinition[length];
            for (int i = 0; i < length; i++)
            {
                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
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
                columns[i] = column;
            }
            return new Columns(columns);
        }
    }
}
