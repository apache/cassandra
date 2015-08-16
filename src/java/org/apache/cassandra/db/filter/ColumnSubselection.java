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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Handles the selection of a subpart of a column.
 * <p>
 * This only make sense for complex column. For those, this allow for instance
 * to select only a slice of a map.
 */
public abstract class ColumnSubselection implements Comparable<ColumnSubselection>
{
    public static final Serializer serializer = new Serializer();

    private enum Kind { SLICE, ELEMENT }

    protected final ColumnDefinition column;

    protected ColumnSubselection(ColumnDefinition column)
    {
        this.column = column;
    }

    public static ColumnSubselection slice(ColumnDefinition column, CellPath from, CellPath to)
    {
        assert column.isComplex() && column.type instanceof CollectionType;
        assert from.size() <= 1 && to.size() <= 1;
        return new Slice(column, from, to);
    }

    public static ColumnSubselection element(ColumnDefinition column, CellPath elt)
    {
        assert column.isComplex() && column.type instanceof CollectionType;
        assert elt.size() == 1;
        return new Element(column, elt);
    }

    public ColumnDefinition column()
    {
        return column;
    }

    protected abstract Kind kind();

    protected abstract CellPath comparisonPath();

    public int compareTo(ColumnSubselection other)
    {
        assert other.column().name.equals(column().name);
        return column().cellPathComparator().compare(comparisonPath(), other.comparisonPath());
    }

    /**
     * Given a path, return -1 if the path is before anything selected by this subselection, 0 if it is selected by this
     * subselection and 1 if the path is after anything selected by this subselection.
     */
    public abstract int compareInclusionOf(CellPath path);

    private static class Slice extends ColumnSubselection
    {
        private final CellPath from;
        private final CellPath to;

        private Slice(ColumnDefinition column, CellPath from, CellPath to)
        {
            super(column);
            this.from = from;
            this.to = to;
        }

        protected Kind kind()
        {
            return Kind.SLICE;
        }

        public CellPath comparisonPath()
        {
            return from;
        }

        public int compareInclusionOf(CellPath path)
        {
            Comparator<CellPath> cmp = column.cellPathComparator();
            if (cmp.compare(path, from) < 0)
                return -1;
            else if (cmp.compare(to, path) < 0)
                return 1;
            else
                return 0;
        }

        @Override
        public String toString()
        {
            // This assert we're dealing with a collection since that's the only thing it's used for so far.
            AbstractType<?> type = ((CollectionType<?>)column().type).nameComparator();
            return String.format("[%s:%s]", from == CellPath.BOTTOM ? "" : type.getString(from.get(0)), to == CellPath.TOP ? "" : type.getString(to.get(0)));
        }
    }

    private static class Element extends ColumnSubselection
    {
        private final CellPath element;

        private Element(ColumnDefinition column, CellPath elt)
        {
            super(column);
            this.element = elt;
        }

        protected Kind kind()
        {
            return Kind.ELEMENT;
        }

        public CellPath comparisonPath()
        {
            return element;
        }

        public int compareInclusionOf(CellPath path)
        {
            return column.cellPathComparator().compare(path, element);
        }

        @Override
        public String toString()
        {
            // This assert we're dealing with a collection since that's the only thing it's used for so far.
            AbstractType<?> type = ((CollectionType<?>)column().type).nameComparator();
            return String.format("[%s]", type.getString(element.get(0)));
        }
    }

    public static class Serializer
    {
        public void serialize(ColumnSubselection subSel, DataOutputPlus out, int version) throws IOException
        {
            ColumnDefinition column = subSel.column();
            ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
            out.writeByte(subSel.kind().ordinal());
            switch (subSel.kind())
            {
                case SLICE:
                    Slice slice = (Slice)subSel;
                    column.cellPathSerializer().serialize(slice.from, out);
                    column.cellPathSerializer().serialize(slice.to, out);
                    break;
                case ELEMENT:
                    Element eltSelection = (Element)subSel;
                    column.cellPathSerializer().serialize(eltSelection.element, out);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        public ColumnSubselection deserialize(DataInputPlus in, int version, CFMetaData metadata) throws IOException
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

            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case SLICE:
                    CellPath from = column.cellPathSerializer().deserialize(in);
                    CellPath to = column.cellPathSerializer().deserialize(in);
                    return new Slice(column, from, to);
                case ELEMENT:
                    CellPath elt = column.cellPathSerializer().deserialize(in);
                    return new Element(column, elt);
            }
            throw new AssertionError();
        }

        public long serializedSize(ColumnSubselection subSel, int version)
        {
            long size = 0;

            ColumnDefinition column = subSel.column();
            size += TypeSizes.sizeofWithShortLength(column.name.bytes);
            size += 1; // kind
            switch (subSel.kind())
            {
                case SLICE:
                    Slice slice = (Slice)subSel;
                    size += column.cellPathSerializer().serializedSize(slice.from);
                    size += column.cellPathSerializer().serializedSize(slice.to);
                    break;
                case ELEMENT:
                    Element element = (Element)subSel;
                    size += column.cellPathSerializer().serializedSize(element.element);
                    break;
            }
            return size;
        }
    }
}
