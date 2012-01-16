/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.marshal;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.thrift.InvalidRequestException;

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

    public static synchronized CompositeType getInstance(List<AbstractType<?>> types) throws ConfigurationException
    {
        if (types == null || types.isEmpty())
            throw new ConfigurationException("Nonsensical empty parameter list for CompositeType");

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
        this.types = types;
    }

    protected AbstractType<?> getNextComparator(int i, ByteBuffer bb)
    {
        return types.get(i);
    }

    protected AbstractType<?> getNextComparator(int i, ByteBuffer bb1, ByteBuffer bb2)
    {
        return types.get(i);
    }

    protected AbstractType<?> getAndAppendNextComparator(int i, ByteBuffer bb, StringBuilder sb)
    {
        return types.get(i);
    }

    protected ParsedComparator parseNextComparator(int i, String part)
    {
        return new StaticParsedComparator(types.get(i), part);
    }

    protected AbstractType<?> validateNextComparator(int i, ByteBuffer bb) throws MarshalException
    {
        if (i >= types.size())
            throw new MarshalException("Too many bytes for comparator");
        return types.get(i);
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
        private int current;

        private final FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
        private final DataOutput out = new DataOutputStream(baos);

        public Builder(CompositeType composite)
        {
            this.composite = composite;
        }

        private Builder(Builder b)
        {
            this(b.composite);
            this.current = b.current;
            try
            {
                out.write(b.baos.toByteArray());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public Builder add(Term t, Relation.Type op, List<ByteBuffer> variables) throws InvalidRequestException
        {
            if (current >= composite.types.size())
                throw new IllegalStateException("Composite column is already fully constructed");

            AbstractType currentType = composite.types.get(current++);
            ByteBuffer buffer = t.getByteBuffer(currentType, variables);
            try
            {
                ByteBufferUtil.writeWithShortLength(buffer, out);

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
                        out.write((byte) -1);
                        break;
                    case GT:
                    case LTE:
                        out.write((byte) 1);
                        break;
                    default:
                        out.write((byte) 0);
                        break;
                }
                return this;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public Builder add(ByteBuffer bb)
        {
            if (current >= composite.types.size())
                throw new IllegalStateException("Composite column is already fully constructed");

            try
            {
                ByteBufferUtil.writeWithShortLength(bb, out);
                out.write((byte) 0);
                return this;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public int componentCount()
        {
            return current;
        }

        public ByteBuffer build()
        {
            return ByteBuffer.wrap(baos.toByteArray());
        }

        public ByteBuffer buildAsEndOfRange()
        {
            if (current >= composite.types.size())
                throw new IllegalStateException("Composite column is already fully constructed");

            if (current == 0)
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
