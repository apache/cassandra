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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

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
    private static final int STATIC_MARKER = 0xFFFF;

    public final List<AbstractType<?>> types;

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

    protected boolean readIsStatic(ByteBuffer bb)
    {
        return readStatic(bb);
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

        CompositeType ct = instances.get(types);
        if (ct == null)
        {
            ct = new CompositeType(types);
            CompositeType previous = instances.putIfAbsent(types, ct);
            if (previous != null)
            {
                ct = previous;
            }
        }
        return ct;
    }

    protected CompositeType(List<AbstractType<?>> types)
    {
        this.types = ImmutableList.copyOf(types);
    }

    protected AbstractType<?> getComparator(int i, ByteBuffer bb)
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

    protected AbstractType<?> getComparator(int i, ByteBuffer bb1, ByteBuffer bb2)
    {
        return getComparator(i, bb1);
    }

    protected AbstractType<?> getAndAppendComparator(int i, ByteBuffer bb, StringBuilder sb)
    {
        return types.get(i);
    }

    protected ParsedComparator parseComparator(int i, String part)
    {
        return new StaticParsedComparator(types.get(i), part);
    }

    protected AbstractType<?> validateComparator(int i, ByteBuffer bb) throws MarshalException
    {
        if (i >= types.size())
            throw new MarshalException("Too many bytes for comparator");
        return types.get(i);
    }

    public ByteBuffer decompose(Object... objects)
    {
        assert objects.length == types.size();

        ByteBuffer[] serialized = new ByteBuffer[objects.length];
        for (int i = 0; i < objects.length; i++)
        {
            ByteBuffer buffer = ((AbstractType) types.get(i)).decompose(objects[i]);
            serialized[i] = buffer;
        }
        return build(serialized);
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

    public static List<ByteBuffer> splitName(ByteBuffer name)
    {
        List<ByteBuffer> l = new ArrayList<>();
        ByteBuffer bb = name.duplicate();
        readStatic(bb);
        while (bb.remaining() > 0)
        {
            l.add(ByteBufferUtil.readBytesWithShortLength(bb));
            bb.get(); // skip end-of-component
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

    public static boolean isStaticName(ByteBuffer bb)
    {
        return bb.remaining() >= 2 && (ByteBufferUtil.getShortLength(bb, bb.position()) & 0xFFFF) == STATIC_MARKER;
    }

    @Override
    public int componentsCount()
    {
        return types.size();
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
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyTypeParameters(types);
    }

    public static ByteBuffer build(ByteBuffer... buffers)
    {
        return build(false, buffers);
    }

    public static ByteBuffer build(boolean isStatic, ByteBuffer... buffers)
    {
        int totalLength = isStatic ? 2 : 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);

        if (isStatic)
            out.putShort((short)STATIC_MARKER);

        for (ByteBuffer bb : buffers)
        {
            ByteBufferUtil.writeShortLength(out, bb.remaining());
            int toCopy = bb.remaining();
            ByteBufferUtil.arrayCopy(bb, bb.position(), out, out.position(), toCopy);
            out.position(out.position() + toCopy);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }
}
