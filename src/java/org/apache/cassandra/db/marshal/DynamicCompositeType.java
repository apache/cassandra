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
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.Iterables.any;

/*
 * The encoding of a DynamicCompositeType column name should be:
 *   <component><component><component> ...
 * where <component> is:
 *   <comparator part><value><'end-of-component' byte>
 * where:
 *   - <comparator part>: either the comparator full name, or a declared
 *     aliases. This is at least 2 bytes (those 2 bytes are called header in
 *     the following). If the first bit of the header is 1, then this
 *     comparator part is an alias, otherwise it's a comparator full name:
 *       - aliases: the actual alias is the 2nd byte of header taken as a
 *         character. The whole <comparator part> is thus 2 byte long.
 *       - comparator full name: the header is the length of the remaining
 *         part. The remaining part is the UTF-8 encoded comparator class
 *         name.
 *   - <value>: the component value bytes preceded by 2 byte containing the
 *     size of value (see CompositeType).
 *   - 'end-of-component' byte is defined as in CompositeType
 */
public class DynamicCompositeType extends AbstractCompositeType
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicCompositeType.class);

    public static class Serializer extends BytesSerializer
    {
        // aliases are held to make sure the serializer is unique for each collection of types, this is to make sure it's
        // safe to cache in all cases
        private final Map<Byte, AbstractType<?>> aliases;

        public Serializer(Map<Byte, AbstractType<?>> aliases)
        {
            this.aliases = aliases;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Serializer that = (Serializer) o;
            return aliases.equals(that.aliases);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(aliases);
        }
    }

    @VisibleForTesting
    public final Map<Byte, AbstractType<?>> aliases;
    private final Serializer serializer;

    // interning instances
    private static final ConcurrentHashMap<Map<Byte, AbstractType<?>>, DynamicCompositeType> instances = new ConcurrentHashMap<>();

    public static DynamicCompositeType getInstance(TypeParser parser)
    {
        return getInstance(parser.getAliasParameters());
    }

    public static DynamicCompositeType getInstance(Map<Byte, AbstractType<?>> aliases)
    {
        DynamicCompositeType dct = instances.get(aliases);
        return null == dct
             ? instances.computeIfAbsent(aliases, DynamicCompositeType::new)
             : dct;
    }

    private DynamicCompositeType(Map<Byte, AbstractType<?>> aliases)
    {
        this.aliases = ImmutableMap.copyOf(aliases);
        this.serializer = new Serializer(aliases);
    }

    @Override
    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return serializer;
    }

    protected <V> boolean readIsStatic(V value, ValueAccessor<V> accessor)
    {
        // We don't have the static nothing for DCT
        return false;
    }

    protected int startingOffset(boolean isStatic)
    {
        return 0;
    }

    protected <V> int getComparatorSize(int i, V value, ValueAccessor<V> accessor, int offset)
    {
        int header = accessor.getShort(value, offset);
        if ((header & 0x8000) == 0)
        {
            return 2 + header;
        }
        else
        {
            return 2;
        }
    }

    private <V> AbstractType<?> getComparator(V value, ValueAccessor<V> accessor, int offset)
    {
        try
        {
            int header = accessor.getShort(value, offset);
            if ((header & 0x8000) == 0)
            {

                String name = accessor.toString(accessor.slice(value, offset + 2, header));
                return TypeParser.parse(name);
            }
            else
            {
                return aliases.get((byte)(header & 0xFF));
            }
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected <V> AbstractType<?> getComparator(int i, V value, ValueAccessor<V> accessor, int offset)
    {
        return getComparator(value, accessor, offset);
    }

    protected <VL, VR> AbstractType<?> getComparator(int i, VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, int offsetL, int offsetR)
    {
        AbstractType<?> comp1 = getComparator(left, accessorL, offsetL);
        AbstractType<?> comp2 = getComparator(right, accessorR, offsetR);
        AbstractType<?> rawComp = comp1;

        /*
         * If both types are ReversedType(Type), we need to compare on the wrapped type (which may differ between the two types) to avoid
         * incompatible comparisons being made.
         */
        if ((comp1 instanceof ReversedType) && (comp2 instanceof ReversedType))
        {
            comp1 = ((ReversedType<?>) comp1).baseType;
            comp2 = ((ReversedType<?>) comp2).baseType;
        }

        // Fast test if the comparator uses singleton instances
        if (comp1 != comp2)
        {
            /*
             * We compare component of different types by comparing the
             * comparator class names. We start with the simple classname
             * first because that will be faster in almost all cases, but
             * fallback on the full name if necessary
             */
            int cmp = comp1.getClass().getSimpleName().compareTo(comp2.getClass().getSimpleName());
            if (cmp != 0)
                return cmp < 0 ? FixedValueComparator.alwaysLesserThan : FixedValueComparator.alwaysGreaterThan;

            cmp = comp1.getClass().getName().compareTo(comp2.getClass().getName());
            if (cmp != 0)
                return cmp < 0 ? FixedValueComparator.alwaysLesserThan : FixedValueComparator.alwaysGreaterThan;

            // if cmp == 0, we're actually having the same type, but one that
            // did not have a singleton instance. It's ok (though inefficient).
        }
        // Use the raw comparator (prior to ReversedType unwrapping)
        return rawComp;
    }

    protected <V> AbstractType<?> getAndAppendComparator(int i, V value, ValueAccessor<V> accessor, StringBuilder sb, int offset)
    {
        try
        {
            int header = accessor.getShort(value, offset);
            if ((header & 0x8000) == 0)
            {
                String name = accessor.toString(accessor.slice(value, offset + 2, header));
                sb.append(name).append("@");
                return TypeParser.parse(name);
            }
            else
            {
                sb.append((char)(header & 0xFF)).append("@");
                return aliases.get((byte)(header & 0xFF));
            }
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected ParsedComparator parseComparator(int i, String part)
    {
        return new DynamicParsedComparator(part);
    }

    protected <V> AbstractType<?> validateComparator(int i, V input, ValueAccessor<V> accessor, int offset) throws MarshalException
    {
        AbstractType<?> comparator = null;
        if (accessor.sizeFromOffset(input, offset) < 2)
            throw new MarshalException("Not enough bytes to header of the comparator part of component " + i);
        int header = accessor.getShort(input, offset);
        offset += TypeSizes.SHORT_SIZE;
        if ((header & 0x8000) == 0)
        {
            if (accessor.sizeFromOffset(input, offset) < header)
                throw new MarshalException("Not enough bytes to read comparator name of component " + i);

            V value = accessor.slice(input, offset, header);
            String valueStr = null;
            try
            {
                valueStr = accessor.toString(value);
                comparator = TypeParser.parse(valueStr);
            }
            catch (CharacterCodingException ce)
            {
                // ByteBufferUtil.string failed.
                // Log it here and we'll further throw an exception below since comparator == null
                logger.error("Failed when decoding the byte buffer in ByteBufferUtil.string()", ce);
            }
            catch (Exception e)
            {
                // parse failed.
                // Log it here and we'll further throw an exception below since comparator == null
                logger.error("Failed to parse value string \"{}\" with exception:", valueStr, e);
            }
        }
        else
        {
            comparator = aliases.get((byte)(header & 0xFF));
        }

        if (comparator == null)
            throw new MarshalException("Cannot find comparator for component " + i);
        else
            return comparator;
    }

    public ByteBuffer decompose(Object... objects)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!(previous instanceof DynamicCompositeType))
            return false;

        // Adding new aliases is fine (but removing is not)
        // Note that modifying the type for an alias to a compatible type is
        // *not* fine since this would deal correctly with mixed aliased/not
        // aliased component.
        DynamicCompositeType cp = (DynamicCompositeType)previous;
        if (aliases.size() < cp.aliases.size())
            return false;

        for (Map.Entry<Byte, AbstractType<?>> entry : cp.aliases.entrySet())
        {
            AbstractType<?> tprev = entry.getValue();
            AbstractType<?> tnew = aliases.get(entry.getKey());
            if (tnew == null || tnew != tprev)
                return false;
        }
        return true;
    }

    @Override
    public <V> boolean referencesUserType(V name, ValueAccessor<V> accessor)
    {
        return any(aliases.values(), t -> t.referencesUserType(name, accessor));
    }

    @Override
    public DynamicCompositeType withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        instances.remove(aliases);

        return getInstance(Maps.transformValues(aliases, v -> v.withUpdatedUserType(udt)));
    }

    @Override
    public AbstractType<?> expandUserTypes()
    {
        return getInstance(Maps.transformValues(aliases, v -> v.expandUserTypes()));
    }

    private class DynamicParsedComparator implements ParsedComparator
    {
        final AbstractType<?> type;
        final boolean isAlias;
        final String comparatorName;
        final String remainingPart;

        DynamicParsedComparator(String part)
        {
            String[] splits = part.split("@");
            if (splits.length != 2)
                throw new IllegalArgumentException("Invalid component representation: " + part);

            comparatorName = splits[0];
            remainingPart = splits[1];

            try
            {
                AbstractType<?> t = null;
                if (comparatorName.length() == 1)
                {
                    // try for an alias
                    // Note: the char to byte cast is theorically bogus for unicode character. I take full
                    // responsibility if someone get hit by this (without making it on purpose)
                    t = aliases.get((byte)comparatorName.charAt(0));
                }
                isAlias = t != null;
                if (!isAlias)
                {
                    t = TypeParser.parse(comparatorName);
                }
                type = t;
            }
            catch (SyntaxException | ConfigurationException e)
            {
                throw new IllegalArgumentException(e);
            }
        }

        public AbstractType<?> getAbstractType()
        {
            return type;
        }

        public String getRemainingPart()
        {
            return remainingPart;
        }

        public int getComparatorSerializedSize()
        {
            return isAlias ? 2 : 2 + ByteBufferUtil.bytes(comparatorName).remaining();
        }

        public void serializeComparator(ByteBuffer bb)
        {
            int header = 0;
            if (isAlias)
                header = 0x8000 | (((byte)comparatorName.charAt(0)) & 0xFF);
            else
                header = comparatorName.length();
            ByteBufferUtil.writeShortLength(bb, header);

            if (!isAlias)
                bb.put(ByteBufferUtil.bytes(comparatorName));
        }
    }

    @Override
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyAliasesParameters(aliases);
    }

    /*
     * A comparator that always sorts it's first argument before the second
     * one.
     */
    @VisibleForTesting
    public static class FixedValueComparator extends AbstractType<Void>
    {
        public static final FixedValueComparator alwaysLesserThan = new FixedValueComparator(-1);
        public static final FixedValueComparator alwaysGreaterThan = new FixedValueComparator(1);

        private final int cmp;

        public FixedValueComparator(int cmp)
        {
            super(ComparisonType.CUSTOM);
            this.cmp = cmp;
        }

        public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
        {
            return cmp;
        }

        @Override
        public <V> Void compose(V value, ValueAccessor<V> accessor)
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer decompose(Void value)
        {
            throw new UnsupportedOperationException();
        }

        public <V> String getString(V value, ValueAccessor<V> accessor)
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer fromString(String str)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Term fromJSONObject(Object parsed)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validate(ByteBuffer bytes)
        {
            throw new UnsupportedOperationException();
        }

        public TypeSerializer<Void> getSerializer()
        {
            throw new UnsupportedOperationException();
        }
    }
}
