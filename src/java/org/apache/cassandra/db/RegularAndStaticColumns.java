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

import java.util.*;

import com.google.common.collect.Iterators;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;

import static java.util.Comparator.naturalOrder;

/**
 * Columns (or a subset of the columns) that a partition contains.
 * This mainly groups both static and regular columns for convenience.
 */
public class RegularAndStaticColumns implements Iterable<ColumnMetadata>
{
    public static RegularAndStaticColumns NONE = new RegularAndStaticColumns(Columns.NONE, Columns.NONE);
    static final long EMPTY_SIZE = ObjectSizes.measure(NONE);

    public final Columns statics;
    public final Columns regulars;

    public RegularAndStaticColumns(Columns statics, Columns regulars)
    {
        assert statics != null && regulars != null;
        this.statics = statics;
        this.regulars = regulars;
    }

    public static RegularAndStaticColumns of(ColumnMetadata column)
    {
        return new RegularAndStaticColumns(column.isStatic() ? Columns.of(column) : Columns.NONE,
                                           column.isStatic() ? Columns.NONE : Columns.of(column));
    }

    public RegularAndStaticColumns without(ColumnMetadata column)
    {
        return new RegularAndStaticColumns(column.isStatic() ? statics.without(column) : statics,
                                           column.isStatic() ? regulars : regulars.without(column));
    }

    public RegularAndStaticColumns mergeTo(RegularAndStaticColumns that)
    {
        if (this == that)
            return this;
        Columns statics = this.statics.mergeTo(that.statics);
        Columns regulars = this.regulars.mergeTo(that.regulars);
        if (statics == this.statics && regulars == this.regulars)
            return this;
        if (statics == that.statics && regulars == that.regulars)
            return that;
        return new RegularAndStaticColumns(statics, regulars);
    }

    public boolean isEmpty()
    {
        return statics.isEmpty() && regulars.isEmpty();
    }

    public Columns columns(boolean isStatic)
    {
        return isStatic ? statics : regulars;
    }

    public boolean contains(ColumnMetadata column)
    {
        return column.isStatic() ? statics.contains(column) : regulars.contains(column);
    }

    public boolean includes(RegularAndStaticColumns columns)
    {
        return statics.containsAll(columns.statics) && regulars.containsAll(columns.regulars);
    }

    public Iterator<ColumnMetadata> iterator()
    {
        return Iterators.concat(statics.iterator(), regulars.iterator());
    }

    public Iterator<ColumnMetadata> selectOrderIterator()
    {
        return Iterators.concat(statics.selectOrderIterator(), regulars.selectOrderIterator());
    }

    /** * Returns the total number of static and regular columns. */
    public int size()
    {
        return regulars.size() + statics.size();
    }

    public long unsharedHeapSize()
    {
        if(this == NONE)
            return 0;

        return EMPTY_SIZE + regulars.unsharedHeapSize() + statics.unsharedHeapSize();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(statics).append(" | ").append(regulars).append(']');
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof RegularAndStaticColumns))
            return false;

        RegularAndStaticColumns that = (RegularAndStaticColumns)other;
        return this.statics.equals(that.statics)
            && this.regulars.equals(that.regulars);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statics, regulars);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        // Note that we do want to use sorted sets because we want the column definitions to be compared
        // through compareTo, not equals. The former basically check it's the same column name, while the latter
        // check it's the same object, including the same type.
        private BTree.Builder<ColumnMetadata> regularColumns;
        private BTree.Builder<ColumnMetadata> staticColumns;

        public Builder add(ColumnMetadata c)
        {
            if (c.isStatic())
            {
                if (staticColumns == null)
                    staticColumns = BTree.builder(naturalOrder());
                staticColumns.add(c);
            }
            else
            {
                assert c.isRegular();
                if (regularColumns == null)
                    regularColumns = BTree.builder(naturalOrder());
                regularColumns.add(c);
            }
            return this;
        }

        public Builder addAll(Iterable<ColumnMetadata> columns)
        {
            for (ColumnMetadata c : columns)
                add(c);
            return this;
        }

        public Builder addAll(RegularAndStaticColumns columns)
        {
            if (regularColumns == null && !columns.regulars.isEmpty())
                regularColumns = BTree.builder(naturalOrder());

            for (ColumnMetadata c : columns.regulars)
                regularColumns.add(c);

            if (staticColumns == null && !columns.statics.isEmpty())
                staticColumns = BTree.builder(naturalOrder());

            for (ColumnMetadata c : columns.statics)
                staticColumns.add(c);

            return this;
        }

        public RegularAndStaticColumns build()
        {
            return new RegularAndStaticColumns(staticColumns  == null ? Columns.NONE : Columns.from(staticColumns),
                                               regularColumns == null ? Columns.NONE : Columns.from(regularColumns));
        }
    }
}
