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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

/*
 * The encoding of a CompositeType column name should be:
 *   <component><component><component> ...
 * where <component> is:
 *   <length of value><value><'end-of-component' byte>
 * where <length of value> is a 2 bytes unsigned short (but 0xFFFF is invalid, see
 * below) and the 'end-of-component' byte should always be 0 for actual column name.
 * However, it can set to 1 for query bounds. This allows to query for the
 * equivalent of 'give me the full super-column'. That is, if during a slice
 * query uses:
 *   start = <3><"foo".getBytes()><0>
 *   end   = <3><"foo".getBytes()><1>
 * then he will be sure to get *all* the columns whose first component is "foo".
 * If for a component, the 'end-of-component' is != 0, there should not be any
 * following component. The end-of-component can also be -1 to allow
 * non-inclusive query. For instance:
 *   start = <3><"foo".getBytes()><-1>
 * allows to query everything that is greater than <3><"foo".getBytes()>, but
 * not <3><"foo".getBytes()> itself.
 *
 * On top of that, CQL3 uses a specific prefix (0xFFFF) to encode "static columns"
 * (CASSANDRA-6561). This does mean the maximum size of the first component of a
 * composite is 65534, not 65535 (or we wouldn't be able to detect if the first 2
 * bytes is the static prefix or not).
 */
public class CompositeType extends AbstractCompositeType
{
    public static class Serializer extends BytesSerializer
    {
        // types are held to make sure the serializer is unique for each collection of types, this is to make sure it's
        // safe to cache in all cases
        public final List<AbstractType<?>> types;

        public Serializer(List<AbstractType<?>> types)
        {
            this.types = types;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Serializer that = (Serializer) o;
            return types.equals(that.types);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(types);
        }
    }

    private static final int STATIC_MARKER = 0xFFFF;

    public final List<AbstractType<?>> types;
    private final Serializer serializer;

    // interning instances
    private static final ConcurrentMap<List<AbstractType<?>>, CompositeType> instances = new ConcurrentHashMap<>();

    public static CompositeType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        return getInstance(parser.getTypeParameters());
    }

    public static CompositeType getInstance(Iterable<AbstractType<?>> types)
    {
        return getInstance(Lists.newArrayList(types));
    }

    public static CompositeType getInstance(AbstractType... types)
    {
        return getInstance(Arrays.asList(types));
    }

    protected static int startingOffsetInternal(boolean isStatic)
    {
        return isStatic ? 2 : 0;
    }

    protected int startingOffset(boolean isStatic)
    {
        return startingOffsetInternal(isStatic);
    }

    protected static <V> boolean readIsStaticInternal(V value, ValueAccessor<V> accessor)
    {
        if (accessor.size(value) < 2)
            return false;

        int header = accessor.getShort(value, 0);
        if ((header & 0xFFFF) != STATIC_MARKER)
            return false;

        return true;
    }

    protected <V> boolean readIsStatic(V value, ValueAccessor<V> accessor)
    {
        return readIsStaticInternal(value, accessor);
    }

    private static boolean readStatic(ByteBuffer bb)
    {
        if (bb.remaining() < 2)
            return false;

        int header = ByteBufferUtil.getShortLength(bb, bb.position());
        if ((header & 0xFFFF) != STATIC_MARKER)
            return false;

        ByteBufferUtil.readShortLength(bb); // Skip header
        return true;
    }

    public static CompositeType getInstance(List<AbstractType<?>> types)
    {
        assert types != null && !types.isEmpty();
        CompositeType t = instances.get(types);
        return null == t
             ? instances.computeIfAbsent(types, CompositeType::new)
             : t;
    }

    protected CompositeType(List<AbstractType<?>> types)
    {
        this.types = ImmutableList.copyOf(types);
        this.serializer = new Serializer(this.types);
    }

    @Override
    public List<AbstractType<?>> subTypes()
    {
        return types;
    }

    @Override
    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return serializer;
    }

    protected <V> AbstractType<?> getComparator(int i, V value, ValueAccessor<V> accessor, int offset)
    {
        try
        {
            return types.get(i);
        }
        catch (IndexOutOfBoundsException e)
        {
            // We shouldn't get there in general we shouldn't construct broken composites
            // but there is a few cases where if the schema has changed since we created/validated
            // the composite, this will be thrown (see #6262). Those cases are a user error but
            // throwing a more meaningful error message to make understanding such error easier. .
            throw new RuntimeException("Cannot get comparator " + i + " in " + this + ". "
                                     + "This might due to a mismatch between the schema and the data read", e);
        }
    }

    protected <VL, VR> AbstractType<?> getComparator(int i, VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, int offsetL, int offsetR)
    {
        return getComparator(i, left, accessorL, offsetL);
    }

    protected <V> AbstractType<?> getAndAppendComparator(int i, V value, ValueAccessor<V> accessor, StringBuilder sb, int offset)
    {
        return types.get(i);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, Version version)
    {
        if (data == null || accessor.isEmpty(data))
            return null;

        ByteSource[] srcs = new ByteSource[types.size() * 2 + 1];
        int length = accessor.size(data);

        // statics go first
        boolean isStatic = readIsStaticInternal(data, accessor);
        int offset = startingOffsetInternal(isStatic);
        srcs[0] = isStatic ? null : ByteSource.EMPTY;

        int i = 0;
        byte lastEoc = 0;
        while (offset < length)
        {
            // Only the end-of-component byte of the last component of this composite can be non-zero, so the
            // component before can't have a non-zero end-of-component byte.
            assert lastEoc == 0 : lastEoc;

            int componentLength = accessor.getUnsignedShort(data, offset);
            offset += 2;
            srcs[i * 2 + 1] = types.get(i).asComparableBytes(accessor, accessor.slice(data, offset, componentLength), version);
            offset += componentLength;
            lastEoc = accessor.getByte(data, offset);
            offset += 1;
            srcs[i * 2 + 2] = ByteSource.oneByte(lastEoc & 0xFF ^ 0x80); // end-of-component also takes part in comparison as signed byte
            ++i;
        }
        // A composite may be leaving some values unspecified. If this is the case, make sure we terminate early
        // so that translations created before an extra field was added match translations that have the field but don't
        // specify a value for it.
        if (i * 2 + 1 < srcs.length)
            srcs = Arrays.copyOfRange(srcs, 0, i * 2 + 1);

        return ByteSource.withTerminatorMaybeLegacy(version, ByteSource.END_OF_STREAM, srcs);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, Version version)
    {
        // For ByteComparable.Version.LEGACY the terminator byte is ByteSource.END_OF_STREAM. The latter means that it's
        // indistinguishable from the END_OF_STREAM byte that gets returned _after_ the terminator byte has already
        // been consumed, when the composite is part of a multi-component sequence. So if in such a scenario we consume
        // the ByteSource.END_OF_STREAM terminator here, this will result in actually consuming the multi-component
        // sequence separator after it and jumping directly into the bytes of the next component, when we try to
        // consume the (already consumed) separator.
        // Instead of trying to find a way around the situation, we can just take advantage of the fact that we don't
        // need to decode from Version.LEGACY, assume that we never do that, and assert it here.
        assert version != Version.LEGACY;

        if (comparableBytes == null)
            return accessor.empty();

        int separator = comparableBytes.next();
        boolean isStatic = ByteSourceInverse.nextComponentNull(separator);
        int i = 0;
        V[] buffers = accessor.createArray(types.size());
        byte lastEoc = 0;

        while ((separator = comparableBytes.next()) != ByteSource.TERMINATOR && i < types.size())
        {
            // Only the end-of-component byte of the last component of this composite can be non-zero, so the
            // component before can't have a non-zero end-of-component byte.
            assert lastEoc == 0 : lastEoc;

            // Get the next type and decode its payload.
            AbstractType<?> type = types.get(i);
            V decoded = type.fromComparableBytes(accessor,
                                                 ByteSourceInverse.nextComponentSource(comparableBytes, separator),
                                                 version);
            buffers[i++] = decoded;

            lastEoc = ByteSourceInverse.getSignedByte(ByteSourceInverse.nextComponentSource(comparableBytes));
        }
        return build(accessor, isStatic, Arrays.copyOf(buffers, i), lastEoc);
    }

    protected ParsedComparator parseComparator(int i, String part)
    {
        return new StaticParsedComparator(types.get(i), part);
    }

    protected <V> AbstractType<?> validateComparator(int i, V value, ValueAccessor<V> accessor, int offset) throws MarshalException
    {
        if (i >= types.size())
            throw new MarshalException("Too many bytes for comparator");
        return types.get(i);
    }

    protected <V> int getComparatorSize(int i, V value, ValueAccessor<V> accessor, int offset)
    {
        return 0;
    }

    public ByteBuffer decompose(Object... objects)
    {
        assert objects.length == types.size() : String.format("Expected length %d but given %d", types.size(), objects.length);

        ByteBuffer[] serialized = new ByteBuffer[objects.length];
        for (int i = 0; i < objects.length; i++)
        {
            ByteBuffer buffer = ((AbstractType) types.get(i)).decompose(objects[i]);
            serialized[i] = buffer;
        }
        return build(ByteBufferAccessor.instance, serialized);
    }
    // Overriding the one of AbstractCompositeType because we can do a tad better
    @Override
    public ByteBuffer[] split(ByteBuffer name)
    {
        // Assume all components, we'll trunk the array afterwards if need be, but
        // most names will be complete.
        ByteBuffer[] l = new ByteBuffer[types.size()];
        ByteBuffer bb = name.duplicate();
        readStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            l[i++] = ByteBufferUtil.readBytesWithShortLength(bb);
            bb.get(); // skip end-of-component
        }
        return i == l.length ? l : Arrays.copyOfRange(l, 0, i);
    }

    public static <V> List<V> splitName(V name, ValueAccessor<V> accessor)
    {
        List<V> l = new ArrayList<>();
        boolean isStatic = readIsStaticInternal(name, accessor);
        int offset = startingOffsetInternal(isStatic);
        while (!accessor.isEmptyFromOffset(name, offset))
        {
            V value = accessor.sliceWithShortLength(name, offset);
            offset += accessor.sizeWithShortLength(value);
            l.add(value);
            offset++; // skip end-of-component
        }
        return l;
    }

    // Extract component idx from bb. Return null if there is not enough component.
    public static ByteBuffer extractComponent(ByteBuffer bb, int idx)
    {
        bb = bb.duplicate();
        readStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            ByteBuffer c = ByteBufferUtil.readBytesWithShortLength(bb);
            if (i == idx)
                return c;

            bb.get(); // skip end-of-component
            ++i;
        }
        return null;
    }

    public static <V> boolean isStaticName(V value, ValueAccessor<V> accessor)
    {
        return accessor.size(value) >= 2 && (accessor.getUnsignedShort(value, 0) & 0xFFFF) == STATIC_MARKER;
    }

    @Override
    public List<AbstractType<?>> getComponents()
    {
        return types;
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!(previous instanceof CompositeType))
            return false;

        // Extending with new components is fine
        CompositeType cp = (CompositeType)previous;
        if (types.size() < cp.types.size())
            return false;

        for (int i = 0; i < cp.types.size(); i++)
        {
            AbstractType tprev = cp.types.get(i);
            AbstractType tnew = types.get(i);
            if (!tnew.isCompatibleWith(tprev))
                return false;
        }
        return true;
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        if (this == otherType)
            return true;

        if (!(otherType instanceof CompositeType))
            return false;

        // Extending with new components is fine
        CompositeType cp = (CompositeType) otherType;
        if (types.size() < cp.types.size())
            return false;

        for (int i = 0; i < cp.types.size(); i++)
        {
            AbstractType tprev = cp.types.get(i);
            AbstractType tnew = types.get(i);
            if (!tnew.isValueCompatibleWith(tprev))
                return false;
        }
        return true;
    }

    @Override
    public <V> boolean referencesUserType(V name, ValueAccessor<V> accessor)
    {
        return any(types, t -> t.referencesUserType(name, accessor));
    }

    @Override
    public CompositeType withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        instances.remove(types);

        return getInstance(transform(types, t -> t.withUpdatedUserType(udt)));
    }

    @Override
    public AbstractType<?> expandUserTypes()
    {
        return getInstance(transform(types, AbstractType::expandUserTypes));
    }

    private static class StaticParsedComparator implements ParsedComparator
    {
        final AbstractType<?> type;
        final String part;

        StaticParsedComparator(AbstractType<?> type, String part)
        {
            this.type = type;
            this.part = part;
        }

        public AbstractType<?> getAbstractType()
        {
            return type;
        }

        public String getRemainingPart()
        {
            return part;
        }

        public int getComparatorSerializedSize()
        {
            return 0;
        }

        public void serializeComparator(ByteBuffer bb) {}
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeType that = (CompositeType) o;
        return types.equals(that.types);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(types);
    }

    @Override
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyTypeParameters(types);
    }

    @SafeVarargs
    public static <V> V build(ValueAccessor<V> accessor, V... values)
    {
        return build(accessor, false, values);
    }

    @SafeVarargs
    public static <V> V build(ValueAccessor<V> accessor, boolean isStatic, V... values)
    {
        return build(accessor, isStatic, values, (byte) 0);
    }

    @VisibleForTesting
    public static <V> V build(ValueAccessor<V> accessor, boolean isStatic, V[] values, byte lastEoc)
    {
        int totalLength = isStatic ? 2 : 0;
        for (V v : values)
            totalLength += 2 + accessor.size(v) + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);

        if (isStatic)
            out.putShort((short)STATIC_MARKER);

        for (int i = 0; i < values.length; ++i)
        {
            V v = values[i];
            ByteBufferUtil.writeShortLength(out, accessor.size(v));
            accessor.write(v, out);
            out.put(i != values.length - 1 ? (byte) 0 : lastEoc);
        }
        out.flip();
        return accessor.valueOf(out);
    }
}
