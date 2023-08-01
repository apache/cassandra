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
package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;
import com.google.common.collect.Range;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.selection.SimpleSelector.SimpleSelectorFactory;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Selector class handling element (c[x]) and slice (c[x..y]) selections over collections.
 */
abstract class ElementsSelector extends Selector
{
    /**
     * An empty collection is composed of an int size of zero.
     */
    private static final ByteBuffer EMPTY_FROZEN_COLLECTION = ByteBufferUtil.bytes(0);

    protected final Selector selected;
    protected final CollectionType<?> type;

    protected ElementsSelector(Kind kind,Selector selected)
    {
        super(kind);
        this.selected = selected;
        this.type = getCollectionType(selected);
    }

    private static boolean isUnset(ByteBuffer bb)
    {
        return bb == ByteBufferUtil.UNSET_BYTE_BUFFER;
    }

    // For sets and maps, return the type corresponding to the element of a selection (that is, x in c[x]).
    private static AbstractType<?> keyType(CollectionType<?> type)
    {
        return type.nameComparator();
    }

    // For sets and maps, return the type corresponding to the result of a selection (that is, c[x] in c[x]).
    public static AbstractType<?> valueType(CollectionType<?> type)
    {
        return type instanceof MapType ? type.valueComparator() : type.nameComparator();
    }

    private static CollectionType<?> getCollectionType(Selector selected)
    {
        AbstractType<?> type = selected.getType();
        if (type instanceof ReversedType)
            type = ((ReversedType<?>) type).baseType;

        assert type instanceof MapType || type instanceof SetType : "this shouldn't have passed validation in Selectable";

        return (CollectionType<?>) type;
    }

    private static abstract class AbstractFactory extends Factory
    {
        protected final String name;
        protected final Selector.Factory factory;
        protected final CollectionType<?> type;

        protected AbstractFactory(String name, Selector.Factory factory, CollectionType<?> type)
        {
            this.name = name;
            this.factory = factory;
            this.type = type;
        }

        protected String getColumnName()
        {
            return name;
        }

        protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
        {
            factory.addColumnMapping(mapping, resultsColumn);
        }

        public boolean isAggregateSelectorFactory()
        {
            return factory.isAggregateSelectorFactory();
        }
    }

    /**
     * Creates a {@code Selector.Factory} for the selection of an element of a collection.
     *
     * @param name a string representing the selection the factory is for. Something like "c[x]".
     * @param factory the {@code Selector.Factory} corresponding to the collection on which an element
     * is selected.
     * @param type the type of the collection.
     * @param key the element within the value represented by {@code factory} that is selected.
     * @return the created factory.
     */
    public static Factory newElementFactory(String name, Selector.Factory factory, CollectionType<?> type, final Term key)
    {
        return new AbstractFactory(name, factory, type)
        {
            protected AbstractType<?> getReturnType()
            {
                return valueType(type);
            }

            public Selector newInstance(QueryOptions options) throws InvalidRequestException
            {
                ByteBuffer keyValue = key.bindAndGet(options);
                if (keyValue == null)
                    throw new InvalidRequestException("Invalid null value for element selection on " + factory.getColumnName());
                if (keyValue == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    throw new InvalidRequestException("Invalid unset value for element selection on " + factory.getColumnName());
                return new ElementSelector(factory.newInstance(options), keyValue);
            }

            public boolean areAllFetchedColumnsKnown()
            {
                // If we known all the fetched columns, it means that we don't have to wait execution to create
                // the ColumnFilter (through addFetchedColumns below).
                // That's the case if either there is no particular subselection
                // to add, or if there is one but the selected key is terminal. In other words,
                // we known all the fetched columns if all the feched columns of the factory are known and either:
                //  1) the type is frozen (in which case there isn't subselection to do).
                //  2) the factory (the left-hand-side) isn't a simple column selection (here again, no
                //     subselection we can do).
                //  3) the element selected is terminal.
                return factory.areAllFetchedColumnsKnown()
                        && (!type.isMultiCell() || !factory.isSimpleSelectorFactory() || key.isTerminal());
            }

            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                if (!type.isMultiCell() || !factory.isSimpleSelectorFactory())
                {
                    factory.addFetchedColumns(builder);
                    return;
                }

                ColumnMetadata column = ((SimpleSelectorFactory) factory).getColumn();
                builder.select(column, CellPath.create(((Term.Terminal)key).get(ProtocolVersion.V3)));
            }
        };
    }

    /**
     * Creates a {@code Selector.Factory} for the selection of a slice of a collection.
     *
     * @param name a string representing the selection the factory is for. Something like "c[x..y]".
     * @param factory the {@code Selector.Factory} corresponding to the collection on which a slice
     * is selected.
     * @param type the type of the collection.
     * @param from the starting bound of the selected slice. This cannot be {@code null} but can be
     * {@code Constants.UNSET_VALUE} if the slice doesn't have a start.
     * @param to the ending bound of the selected slice. This cannot be {@code null} but can be
     * {@code Constants.UNSET_VALUE} if the slice doesn't have an end.
     * @return the created factory.
     */
    public static Factory newSliceFactory(String name, Selector.Factory factory, CollectionType<?> type, final Term from, final Term to)
    {
        return new AbstractFactory(name, factory, type)
        {
            protected AbstractType<?> getReturnType()
            {
                return type;
            }

            public Selector newInstance(QueryOptions options) throws InvalidRequestException
            {
                ByteBuffer fromValue = from.bindAndGet(options);
                ByteBuffer toValue = to.bindAndGet(options);
                // Note that we use UNSET values to represent no bound, so null is truly invalid
                if (fromValue == null || toValue == null)
                    throw new InvalidRequestException("Invalid null value for slice selection on " + factory.getColumnName());
                return new SliceSelector(factory.newInstance(options), from.bindAndGet(options), to.bindAndGet(options));
            }

            public boolean areAllFetchedColumnsKnown()
            {
                // If we known all the fetched columns, it means that we don't have to wait execution to create
                // the ColumnFilter (through addFetchedColumns below).
                // That's the case if either there is no particular subselection
                // to add, or if there is one but the selected bound are terminal. In other words,
                // we known all the fetched columns if all the feched columns of the factory are known and either:
                //  1) the type is frozen (in which case there isn't subselection to do).
                //  2) the factory (the left-hand-side) isn't a simple column selection (here again, no
                //     subselection we can do).
                //  3) the bound of the selected slice are terminal.
                return factory.areAllFetchedColumnsKnown()
                        && (!type.isMultiCell() || !factory.isSimpleSelectorFactory() || (from.isTerminal() && to.isTerminal()));
            }

            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                if (!type.isMultiCell() || !factory.isSimpleSelectorFactory())
                {
                    factory.addFetchedColumns(builder);
                    return;
                }

                ColumnMetadata column = ((SimpleSelectorFactory) factory).getColumn();
                ByteBuffer fromBB = ((Term.Terminal)from).get(ProtocolVersion.V3);
                ByteBuffer toBB = ((Term.Terminal)to).get(ProtocolVersion.V3);
                builder.slice(column, isUnset(fromBB) ? CellPath.BOTTOM : CellPath.create(fromBB), isUnset(toBB) ? CellPath.TOP  : CellPath.create(toBB));
            }
        };
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        ByteBuffer value = selected.getOutput(protocolVersion);
        return value == null ? null : extractSelection(value);
    }

    protected abstract ByteBuffer extractSelection(ByteBuffer collection);

    public void addInput(InputRow input)
    {
        selected.addInput(input);
    }

    protected Range<Integer> getIndexRange(ByteBuffer output, ByteBuffer fromKey, ByteBuffer toKey)
    {
        return type.getSerializer().getIndexesRangeFromSerialized(output, fromKey, toKey, keyType(type));
    }

    public void reset()
    {
        selected.reset();
    }

    @Override
    public boolean isTerminal()
    {
        return selected.isTerminal();
    }

    static class ElementSelector extends ElementsSelector
    {
        protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
        {
            protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
            {
                Selector selected = Selector.serializer.deserialize(in, version, metadata);
                ByteBuffer key = ByteBufferUtil.readWithVIntLength(in);

                return new ElementSelector(selected, key);
            }
        };

        private final ByteBuffer key;

        private ElementSelector(Selector selected, ByteBuffer key)
        {
            super(Kind.ELEMENT_SELECTOR, selected);
            this.key = key;
        }

        public void addFetchedColumns(ColumnFilter.Builder builder)
        {
            if (type.isMultiCell() && selected instanceof SimpleSelector)
            {
                ColumnMetadata column = ((SimpleSelector)selected).column;
                builder.select(column, CellPath.create(key));
            }
            else
            {
                selected.addFetchedColumns(builder);
            }
        }

        protected ByteBuffer extractSelection(ByteBuffer collection)
        {
            return type.getSerializer().getSerializedValue(collection, key, keyType(type));
        }

        protected int getElementIndex(ProtocolVersion protocolVersion, ByteBuffer key)
        {
            ByteBuffer output = selected.getOutput(protocolVersion);
            return output == null ? -1 : type.getSerializer().getIndexFromSerialized(output, key, keyType(type));
        }

        @Override
        protected ColumnTimestamps getWritetimes(ProtocolVersion protocolVersion)
        {
            return getElementTimestamps(protocolVersion, selected.getWritetimes(protocolVersion));
        }

        @Override
        protected ColumnTimestamps getTTLs(ProtocolVersion protocolVersion)
        {
            return getElementTimestamps(protocolVersion, selected.getTTLs(protocolVersion));
        }

        private ColumnTimestamps getElementTimestamps(ProtocolVersion protocolVersion,
                                                      ColumnTimestamps timestamps)
        {
            int index = getElementIndex(protocolVersion, key);
            return index == -1 ? ColumnTimestamps.NO_TIMESTAMP : timestamps.get(index);
        }

        public AbstractType<?> getType()
        {
            return valueType(type);
        }

        @Override
        public String toString()
        {
            return String.format("%s[%s]", selected, keyType(type).getString(key));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof ElementSelector))
                return false;

            ElementSelector s = (ElementSelector) o;

            return Objects.equal(selected, s.selected)
                && Objects.equal(key, s.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(selected, key);
        }

        @Override
        protected int serializedSize(int version)
        {
            return TypeSizes.sizeofWithVIntLength(key) + serializer.serializedSize(selected, version);
        }

        @Override
        protected void serialize(DataOutputPlus out, int version) throws IOException
        {
            serializer.serialize(selected, out, version);
            ByteBufferUtil.serializedSizeWithVIntLength(key);
        }
    }

    static class SliceSelector extends ElementsSelector
    {
        protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
        {
            protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
            {
                Selector selected = Selector.serializer.deserialize(in, version, metadata);

                boolean isFromUnset = in.readBoolean();
                ByteBuffer from = isFromUnset ? ByteBufferUtil.UNSET_BYTE_BUFFER : ByteBufferUtil.readWithVIntLength(in);

                boolean isToUnset = in.readBoolean();
                ByteBuffer to = isToUnset ? ByteBufferUtil.UNSET_BYTE_BUFFER : ByteBufferUtil.readWithVIntLength(in);

                return new SliceSelector(selected, from, to);
            }
        };

        // Note that neither from nor to can be null, but they can both be ByteBufferUtil.UNSET_BYTE_BUFFER to represent no particular bound
        private final ByteBuffer from;
        private final ByteBuffer to;

        private SliceSelector(Selector selected, ByteBuffer from, ByteBuffer to)
        {
            super(Kind.SLICE_SELECTOR, selected);
            assert from != null && to != null : "We can have unset buffers, but not nulls";
            this.from = from;
            this.to = to;
        }

        public void addFetchedColumns(ColumnFilter.Builder builder)
        {
            if (type.isMultiCell() && selected instanceof SimpleSelector)
            {
                ColumnMetadata column = ((SimpleSelector)selected).column;
                builder.slice(column, isUnset(from) ? CellPath.BOTTOM : CellPath.create(from), isUnset(to) ? CellPath.TOP  : CellPath.create(to));
            }
            else
            {
                selected.addFetchedColumns(builder);
            }
        }

        protected ByteBuffer extractSelection(ByteBuffer collection)
        {
            return type.getSerializer().getSliceFromSerialized(collection, from, to, type.nameComparator(), type.isFrozenCollection());
        }

        @Override
        protected ColumnTimestamps getWritetimes(ProtocolVersion protocolVersion)
        {
            return getTimestampsSlice(protocolVersion, selected.getWritetimes(protocolVersion));
        }

        @Override
        protected ColumnTimestamps getTTLs(ProtocolVersion protocolVersion)
        {
            return getTimestampsSlice(protocolVersion, selected.getTTLs(protocolVersion));
        }

        protected ColumnTimestamps getTimestampsSlice(ProtocolVersion protocolVersion, ColumnTimestamps timestamps)
        {
            ByteBuffer output = selected.getOutput(protocolVersion);
            return (output == null || isCollectionEmpty(output))
                   ? ColumnTimestamps.NO_TIMESTAMP
                   : timestamps.slice(getIndexRange(output, from, to) );
        }

        /**
         * Checks if the collection is empty. Only frozen collection can be empty.
         * @param output the serialized collection
         * @return {@code true} if the collection is empty {@code false} otherwise.
         */
        private boolean isCollectionEmpty(ByteBuffer output)
        {
            return EMPTY_FROZEN_COLLECTION.equals(output);
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            boolean fromUnset = isUnset(from);
            boolean toUnset = isUnset(to);
            return fromUnset && toUnset
                 ? selected.toString()
                 : String.format("%s[%s..%s]", selected, fromUnset ? "" : keyType(type).getString(from), toUnset ? "" : keyType(type).getString(to));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof SliceSelector))
                return false;

            SliceSelector s = (SliceSelector) o;

            return Objects.equal(selected, s.selected)
                && Objects.equal(from, s.from)
                && Objects.equal(to, s.to);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(selected, from, to);
        }

        @Override
        protected int serializedSize(int version)
        {
            int size = serializer.serializedSize(selected, version) + 2;

            if (!isUnset(from))
                size += TypeSizes.sizeofWithVIntLength(from);

            if (!isUnset(to))
                size += TypeSizes.sizeofWithVIntLength(to);

            return size;
        }

        @Override
        protected void serialize(DataOutputPlus out, int version) throws IOException
        {
            serializer.serialize(selected, out, version);

            boolean isFromUnset = isUnset(from);
            out.writeBoolean(isFromUnset);
            if (!isFromUnset)
                ByteBufferUtil.serializedSizeWithVIntLength(from);

            boolean isToUnset = isUnset(to);
            out.writeBoolean(isToUnset);
            if (!isToUnset)
                ByteBufferUtil.serializedSizeWithVIntLength(to);
        }
    }
}
