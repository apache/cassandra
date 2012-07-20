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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

/*
 * The encoding of a CompositeType column name should be:
 *   <component><component><component> ...
 * where <component> is:
 *   <length of value><value><'end-of-component' byte>
 * where <length of value> is a 2 bytes unsigned short the and the
 * 'end-of-component' byte should always be 0 for actual column name.
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
 */
public class CompositeType extends AbstractCompositeType
{
    public final List<AbstractType<?>> types;

    // interning instances
    private static final Map<List<AbstractType<?>>, CompositeType> instances = new HashMap<List<AbstractType<?>>, CompositeType>();

    public static CompositeType getInstance(TypeParser parser) throws ConfigurationException
    {
        return getInstance(parser.getTypeParameters());
    }

    public static synchronized CompositeType getInstance(List<AbstractType<?>> types)
    {
        assert types != null && !types.isEmpty();

        CompositeType ct = instances.get(types);
        if (ct == null)
        {
            ct = new CompositeType(types);
            instances.put(types, ct);
        }
        return ct;
    }

    private CompositeType(List<AbstractType<?>> types)
    {
        this.types = ImmutableList.copyOf(types);
    }

    protected AbstractType<?> getComparator(int i, ByteBuffer bb)
    {
        return types.get(i);
    }

    protected AbstractType<?> getComparator(int i, ByteBuffer bb1, ByteBuffer bb2)
    {
        return types.get(i);
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

        List<ByteBuffer> serialized = new ArrayList<ByteBuffer>(objects.length);
        int totalLength = 0;
        for (int i = 0; i < objects.length; i++)
        {
            ByteBuffer buffer = ((AbstractType) types.get(i)).decompose(objects[i]);
            serialized.add(buffer);
            totalLength += 2 + buffer.remaining() + 1;
        }
        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer bb : serialized)
        {
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
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

    public static class Builder implements ColumnNameBuilder
    {
        private final CompositeType composite;

        private final List<ByteBuffer> components;
        private final byte[] endOfComponents;
        private int serializedSize;

        public Builder(CompositeType composite)
        {
            this(composite, new ArrayList<ByteBuffer>(composite.types.size()), new byte[composite.types.size()]);
        }

        public Builder(CompositeType composite, List<ByteBuffer> components, byte[] endOfComponents)
        {
            assert endOfComponents.length == composite.types.size();

            this.composite = composite;
            this.components = components;
            this.endOfComponents = endOfComponents;
        }

        private Builder(Builder b)
        {
            this(b.composite, new ArrayList<ByteBuffer>(b.components), Arrays.copyOf(b.endOfComponents, b.endOfComponents.length));
            this.serializedSize = b.serializedSize;
        }

        public Builder add(Term t, Relation.Type op, List<ByteBuffer> variables) throws InvalidRequestException
        {
            if (components.size() >= composite.types.size())
                throw new IllegalStateException("Composite column is already fully constructed");

            int current = components.size();
            AbstractType currentType = composite.types.get(current);
            ByteBuffer buffer = t.getByteBuffer(currentType, variables);
            components.add(buffer);

            /*
             * Given the rules for eoc (end-of-component, see AbstractCompositeType.compare()),
             * We can select:
             *   - = 'a' by using <'a'><0>
             *   - < 'a' by using <'a'><-1>
             *   - <= 'a' by using <'a'><1>
             *   - > 'a' by using <'a'><1>
             *   - >= 'a' by using <'a'><0>
             */
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
            return this;
        }

        public Builder add(ByteBuffer bb)
        {
            int current = components.size();
            if (current >= composite.types.size())
                throw new IllegalStateException("Composite column is already fully constructed");

            components.add(bb);
            endOfComponents[current] = (byte) 0;
            return this;
        }

        public int componentCount()
        {
            return components.size();
        }

        public ByteBuffer build()
        {
            DataOutputBuffer out = new DataOutputBuffer(serializedSize);
            for (int i = 0; i < components.size(); i++)
            {
                ByteBufferUtil.writeWithShortLength(components.get(i), out);
                out.write(endOfComponents[i]);
            }
            return ByteBuffer.wrap(out.getData(), 0, out.getLength());
        }

        public ByteBuffer buildAsEndOfRange()
        {
            if (components.size() >= composite.types.size())
                throw new IllegalStateException("Composite column is already fully constructed");

            if (components.isEmpty())
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;

            ByteBuffer bb = build();
            bb.put(bb.remaining() - 1, (byte)1);
            return bb;
        }

        public Builder copy()
        {
            return new Builder(this);
        }
    }
}
