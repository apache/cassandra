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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
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
    public static final int STATIC_MARKER = 0xFFFF;

    public final List<AbstractType<?>> types;

    // interning instances
    private static final ConcurrentMap<List<AbstractType<?>>, CompositeType> instances = new ConcurrentHashMap<List<AbstractType<?>>, CompositeType>();

    public static CompositeType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        return getInstance(parser.getTypeParameters());
    }

    public static CompositeType getInstance(AbstractType... types)
    {
        return getInstance(Arrays.<AbstractType<?>>asList(types));
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
            // We shouldn't get there in general because 1) we shouldn't construct broken composites
            // from CQL and 2) broken composites coming from thrift should be rejected by validate.
            // There is a few cases however where, if the schema has changed since we created/validated
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

    public static byte lastEOC(ByteBuffer name)
    {
        return name.get(name.limit() - 1);
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

    // Extract CQL3 column name from the full column name.
    public ByteBuffer extractLastComponent(ByteBuffer bb)
    {
        int idx = types.get(types.size() - 1) instanceof ColumnToCollectionType ? types.size() - 2 : types.size() - 1;
        return extractComponent(bb, idx);
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

    public Builder builder()
    {
        return new Builder(this);
    }

    public Builder builder(boolean isStatic)
    {
        return new Builder(this, isStatic);
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

    public static class Builder
    {
        private final CompositeType composite;

        private final List<ByteBuffer> components;
        private final byte[] endOfComponents;
        private int serializedSize;
        private final boolean isStatic;

        public Builder(CompositeType composite)
        {
            this(composite, false);
        }

        public Builder(CompositeType composite, boolean isStatic)
        {
            this(composite, new ArrayList<>(composite.types.size()), new byte[composite.types.size()], isStatic);
        }

        private Builder(CompositeType composite, List<ByteBuffer> components, byte[] endOfComponents, boolean isStatic)
        {
            assert endOfComponents.length == composite.types.size();

            this.composite = composite;
            this.components = components;
            this.endOfComponents = endOfComponents;
            this.isStatic = isStatic;
            if (isStatic)
                serializedSize = 2;
        }

        private Builder(Builder b)
        {
            this(b.composite, new ArrayList<>(b.components), Arrays.copyOf(b.endOfComponents, b.endOfComponents.length), b.isStatic);
            this.serializedSize = b.serializedSize;
        }

        public Builder add(ByteBuffer bb)
        {
            if (components.size() >= composite.types.size())
                throw new IllegalStateException("Composite column is already fully constructed");

            components.add(bb);
            serializedSize += 3 + bb.remaining(); // 2 bytes lenght + 1 byte eoc
            return this;
        }

        public Builder add(ColumnIdentifier name)
        {
            return add(name.bytes);
        }

        public int componentCount()
        {
            return components.size();
        }

        public int remainingCount()
        {
            return composite.types.size() - components.size();
        }

        public ByteBuffer get(int i)
        {
            return components.get(i);
        }

        public ByteBuffer build()
        {
            try (DataOutputBuffer out = new DataOutputBufferFixed(serializedSize))
            {
                if (isStatic)
                    out.writeShort(STATIC_MARKER);

                for (int i = 0; i < components.size(); i++)
                {
                    ByteBufferUtil.writeWithShortLength(components.get(i), out);
                    out.write(endOfComponents[i]);
                }
                return ByteBuffer.wrap(out.getData(), 0, out.getLength());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer buildAsEndOfRange()
        {
            if (components.isEmpty())
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;

            ByteBuffer bb = build();
            bb.put(bb.remaining() - 1, (byte)1);
            return bb;
        }

        public ByteBuffer buildForRelation(Operator op)
        {
            /*
             * Given the rules for eoc (end-of-component, see AbstractCompositeType.compare()),
             * We can select:
             *   - = 'a' by using <'a'><0>
             *   - < 'a' by using <'a'><-1>
             *   - <= 'a' by using <'a'><1>
             *   - > 'a' by using <'a'><1>
             *   - >= 'a' by using <'a'><0>
             */
            int current = components.size() - 1;
            switch (op)
            {
                case LT:
                    endOfComponents[current] = (byte) -1;
                    break;
                case GT:
                case LTE:
                    endOfComponents[current] = (byte) 1;
                    break;
                default:
                    endOfComponents[current] = (byte) 0;
                    break;
            }
            return build();
        }

        public Builder copy()
        {
            return new Builder(this);
        }

        public ByteBuffer getComponent(int i)
        {
            if (i >= components.size())
                throw new IllegalArgumentException();

            return components.get(i);
        }
    }
}
