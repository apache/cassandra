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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 *
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
public abstract class AbstractType<T> implements Comparator<ByteBuffer>
{
    public final Comparator<IndexInfo> indexComparator;
    public final Comparator<IndexInfo> indexReverseComparator;
    public final Comparator<Column> columnComparator;
    public final Comparator<Column> columnReverseComparator;
    public final Comparator<OnDiskAtom> onDiskAtomComparator;
    public final Comparator<ByteBuffer> reverseComparator;

    protected AbstractType()
    {
        indexComparator = new Comparator<IndexInfo>()
        {
            public int compare(IndexInfo o1, IndexInfo o2)
            {
                return AbstractType.this.compare(o1.lastName, o2.lastName);
            }
        };
        indexReverseComparator = new Comparator<IndexInfo>()
        {
            public int compare(IndexInfo o1, IndexInfo o2)
            {
                return AbstractType.this.compare(o1.firstName, o2.firstName);
            }
        };
        columnComparator = new Comparator<Column>()
        {
            public int compare(Column c1, Column c2)
            {
                return AbstractType.this.compare(c1.name(), c2.name());
            }
        };
        columnReverseComparator = new Comparator<Column>()
        {
            public int compare(Column c1, Column c2)
            {
                return AbstractType.this.compare(c2.name(), c1.name());
            }
        };
        onDiskAtomComparator = new Comparator<OnDiskAtom>()
        {
            public int compare(OnDiskAtom c1, OnDiskAtom c2)
            {
                int comp = AbstractType.this.compare(c1.name(), c2.name());
                if (comp != 0)
                    return comp;

                if (c1 instanceof RangeTombstone)
                {
                    if (c2 instanceof RangeTombstone)
                    {
                        RangeTombstone t1 = (RangeTombstone)c1;
                        RangeTombstone t2 = (RangeTombstone)c2;
                        int comp2 = AbstractType.this.compare(t1.max, t2.max);
                        if (comp2 == 0)
                            return t1.data.compareTo(t2.data);
                        else
                            return comp2;
                    }
                    else
                    {
                        return -1;
                    }
                }
                else if (c2 instanceof RangeTombstone)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        };
        reverseComparator = new Comparator<ByteBuffer>()
        {
            public int compare(ByteBuffer o1, ByteBuffer o2)
            {
                if (o1.remaining() == 0)
                {
                    return o2.remaining() == 0 ? 0 : -1;
                }
                if (o2.remaining() == 0)
                {
                    return 1;
                }

                return AbstractType.this.compare(o2, o1);
            }
        };
    }

    public T compose(ByteBuffer bytes)
    {
        return getSerializer().deserialize(bytes);
    }

    public ByteBuffer decompose(T value)
    {
        return getSerializer().serialize(value);
    }

    /** get a string representation of the bytes suitable for log messages */
    public String getString(ByteBuffer bytes)
    {
        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(bytes);

        T value = serializer.deserialize(bytes);
        return value == null ? "null" : serializer.toString(value);
    }

    /** get a byte representation of the given string. */
    public abstract ByteBuffer fromString(String source) throws MarshalException;

    /** for compatibility with TimeUUID in CQL2. See TimeUUIDType (that overrides it). */
    public ByteBuffer fromStringCQL2(String source) throws MarshalException
    {
        return fromString(source);
    }

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        getSerializer().validate(bytes);
    }

    /* Most of our internal type should override that. */
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }

    public abstract TypeSerializer<T> getSerializer();

    /** @deprecated use reverseComparator field instead */
    public Comparator<ByteBuffer> getReverseComparator()
    {
        return reverseComparator;
    }

    /* convenience method */
    public String getString(Collection<ByteBuffer> names)
    {
        StringBuilder builder = new StringBuilder();
        for (ByteBuffer name : names)
        {
            builder.append(getString(name)).append(",");
        }
        return builder.toString();
    }

    /* convenience method */
    public String getColumnsString(Iterable<Column> columns)
    {
        StringBuilder builder = new StringBuilder();
        for (Column column : columns)
        {
            builder.append(column.getString(this)).append(",");
        }
        return builder.toString();
    }

    public boolean isCommutative()
    {
        return false;
    }

    public static AbstractType<?> parseDefaultParameters(AbstractType<?> baseType, TypeParser parser) throws SyntaxException
    {
        Map<String, String> parameters = parser.getKeyValueParameters();
        String reversed = parameters.get("reversed");
        if (reversed != null && (reversed.isEmpty() || reversed.equals("true")))
        {
            return ReversedType.getInstance(baseType);
        }
        else
        {
            return baseType;
        }
    }

    /**
     * Returns true if this comparator is compatible with the provided
     * previous comparator, that is if previous can safely be replaced by this.
     * A comparator cn should be compatible with a previous one cp if forall columns c1 and c2,
     * if   cn.validate(c1) and cn.validate(c2) and cn.compare(c1, c2) == v,
     * then cp.validate(c1) and cp.validate(c2) and cp.compare(c1, c2) == v.
     *
     * Note that a type should be compatible with at least itself and when in
     * doubt, keep the default behavior of not being compatible with any other comparator!
     */
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        return this == previous;
    }

    /**
     * An alternative comparison function used by CollectionsType in conjunction with CompositeType.
     *
     * This comparator is only called to compare components of a CompositeType. It gets the value of the
     * previous component as argument (or null if it's the first component of the composite).
     *
     * Unless you're doing something very similar to CollectionsType, you shouldn't override this.
     */
    public int compareCollectionMembers(ByteBuffer v1, ByteBuffer v2, ByteBuffer collectionName)
    {
        return compare(v1, v2);
    }

    /**
     * An alternative validation function used by CollectionsType in conjunction with CompositeType.
     *
     * This is similar to the compare function above.
     */
    public void validateCollectionMember(ByteBuffer bytes, ByteBuffer collectionName) throws MarshalException
    {
        validate(bytes);
    }

    public boolean isCollection()
    {
        return false;
    }

    /**
     * The number of subcomponents this type has.
     * This is always 1, i.e. the type has only itself as "subcomponents", except for CompositeType.
     */
    public int componentsCount()
    {
        return 1;
    }

    /**
     * Return a list of the "subcomponents" this type has.
     * This always return a singleton list with the type itself except for CompositeType.
     */
    public List<AbstractType<?>> getComponents()
    {
        return Collections.<AbstractType<?>>singletonList(this);
    }

    /**
     * This must be overriden by subclasses if necessary so that for any
     * AbstractType, this == TypeParser.parse(toString()).
     *
     * Note that for backwards compatibility this includes the full classname.
     * For CQL purposes the short name is fine.
     */
    @Override
    public String toString()
    {
        return getClass().getName();
    }

    protected boolean intersects(ByteBuffer minColName, ByteBuffer maxColName, ByteBuffer sliceStart, ByteBuffer sliceEnd)
    {
        return (sliceStart.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER) || compare(maxColName, sliceStart) >= 0)
               && (sliceEnd.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER) || compare(sliceEnd, minColName) >= 0);
    }

    public boolean intersects(List<ByteBuffer> minColumnNames, List<ByteBuffer> maxColumnNames, SliceQueryFilter filter)
    {
        assert minColumnNames.size() == 1;

        for (ColumnSlice slice : filter.slices)
        {
            ByteBuffer start = filter.isReversed() ? slice.finish : slice.start;
            ByteBuffer finish = filter.isReversed() ? slice.start : slice.finish;

            if (intersects(minColumnNames.get(0), maxColumnNames.get(0), start, finish))
                return true;
        }
        return false;
    }
}
