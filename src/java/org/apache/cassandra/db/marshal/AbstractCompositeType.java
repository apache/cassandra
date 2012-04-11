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
import java.util.List;

/**
 * A class avoiding class duplication between CompositeType and
 * DynamicCompositeType.
 * Those two differs only in that for DynamicCompositeType, the comparators
 * are in the encoded column name at the front of each component.
 */
public abstract class AbstractCompositeType extends AbstractType<ByteBuffer>
{
    // changes bb position
    protected static int getShortLength(ByteBuffer bb)
    {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    // changes bb position
    protected static void putShortLength(ByteBuffer bb, int length)
    {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

    // changes bb position
    protected static ByteBuffer getBytes(ByteBuffer bb, int length)
    {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    // changes bb position
    protected static ByteBuffer getWithShortLength(ByteBuffer bb)
    {
        int length = getShortLength(bb);
        return getBytes(bb, length);
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (null == o1)
            return null == o2 ? 0 : -1;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();
        int i = 0;

        while (bb1.remaining() > 0 && bb2.remaining() > 0)
        {
            AbstractType<?> comparator = getComparator(i, bb1, bb2);

            ByteBuffer value1 = getWithShortLength(bb1);
            ByteBuffer value2 = getWithShortLength(bb2);

            int cmp = comparator.compare(value1, value2);
            if (cmp != 0)
                return cmp;

            byte b1 = bb1.get();
            byte b2 = bb2.get();
            if (b1 < 0)
            {
                if (b2 >= 0)
                    return -1;
            }
            else if (b1 > 0)
            {
                if (b2 <= 0)
                    return 1;
            }
            else
            {
                // b1 == 0
                if (b2 != 0)
                    return -b2;
            }
            ++i;
        }

        if (bb1.remaining() == 0)
            return bb2.remaining() == 0 ? 0 : -1;

        // bb1.remaining() > 0 && bb2.remaining() == 0
        return 1;
    }

    /**
     * Split a composite column names into it's components.
     */
    public ByteBuffer[] split(ByteBuffer name)
    {
        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        ByteBuffer bb = name.duplicate();
        int i = 0;
        while (bb.remaining() > 0)
        {
            getComparator(i++, bb);
            l.add(getWithShortLength(bb));
            bb.get(); // skip end-of-component
        }
        return l.toArray(new ByteBuffer[l.size()]);
    }

    public String getString(ByteBuffer bytes)
    {
        StringBuilder sb = new StringBuilder();
        ByteBuffer bb = bytes.duplicate();
        int i = 0;

        while (bb.remaining() > 0)
        {
            if (bb.remaining() != bytes.remaining())
                sb.append(":");

            AbstractType<?> comparator = getAndAppendComparator(i, bb, sb);
            ByteBuffer value = getWithShortLength(bb);

            sb.append(comparator.getString(value));

            byte b = bb.get();
            if (b != 0)
            {
                sb.append(":!");
                break;
            }
            ++i;
        }
        return sb.toString();
    }

    public static class CompositeComponent
    {
        public AbstractType comparator;
        public ByteBuffer   value;

        public CompositeComponent( AbstractType comparator, ByteBuffer value )
        {
            this.comparator = comparator;
            this.value      = value;
        }
    }

    public List<CompositeComponent> deconstruct( ByteBuffer bytes )
    {
        List<CompositeComponent> list = new ArrayList<CompositeComponent>();

        ByteBuffer bb = bytes.duplicate();
        int i = 0;

        while (bb.remaining() > 0)
        {
            AbstractType comparator = getComparator(i, bb);
            ByteBuffer value = getWithShortLength(bb);

            list.add( new CompositeComponent(comparator,value) );

            byte b = bb.get(); // Ignore; not relevant here
            ++i;
        }
        return list;
    }

    /*
     * FIXME: this would break if some of the component string representation
     * contains ':'. None of our current comparator do so, so this is probably
     * not an urgent matter, but this could break for custom comparator.
     * (DynamicCompositeType would break on '@' too)
     */
    public ByteBuffer fromString(String source)
    {
        String[] parts = source.split(":");
        List<ByteBuffer> components = new ArrayList<ByteBuffer>(parts.length);
        List<ParsedComparator> comparators = new ArrayList<ParsedComparator>(parts.length);
        int totalLength = 0, i = 0;
        boolean lastByteIsOne = false;

        for (String part : parts)
        {
            if (part.equals("!"))
            {
                lastByteIsOne = true;
                break;
            }

            ParsedComparator p = parseComparator(i, part);
            AbstractType<?> type = p.getAbstractType();
            part = p.getRemainingPart();

            ByteBuffer component = type.fromString(part);
            totalLength += p.getComparatorSerializedSize() + 2 + component.remaining() + 1;
            components.add(component);
            comparators.add(p);
            ++i;
        }

        ByteBuffer bb = ByteBuffer.allocate(totalLength);
        i = 0;
        for (ByteBuffer component : components)
        {
            comparators.get(i).serializeComparator(bb);
            putShortLength(bb, component.remaining());
            bb.put(component); // it's ok to consume component as we won't use it anymore
            bb.put((byte)0);
            ++i;
        }
        if (lastByteIsOne)
            bb.put(bb.limit() - 1, (byte)1);

        bb.rewind();
        return bb;
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        ByteBuffer bb = bytes.duplicate();

        int i = 0;
        while (bb.remaining() > 0)
        {
            AbstractType<?> comparator = validateComparator(i, bb);

            if (bb.remaining() < 2)
                throw new MarshalException("Not enough bytes to read value size of component " + i);
            int length = getShortLength(bb);

            if (bb.remaining() < length)
                throw new MarshalException("Not enough bytes to read value of component " + i);
            ByteBuffer value = getBytes(bb, length);

            comparator.validate(value);

            if (bb.remaining() == 0)
                throw new MarshalException("Not enough bytes to read the end-of-component byte of component" + i);
            byte b = bb.get();
            if (b != 0 && bb.remaining() != 0)
                throw new MarshalException("Invalid bytes remaining after an end-of-component at component" + i);
            ++i;
        }
    }

    public abstract ByteBuffer decompose(Object... objects);

    public ByteBuffer compose(ByteBuffer bytes)
    {
        return bytes;
    }

    public ByteBuffer decompose(ByteBuffer value)
    {
        return value;
    }

    /**
     * @return the comparator for the given component. static CompositeType will consult
     * @param i; DynamicCompositeType will read the type information from @param bb
     */
    abstract protected AbstractType<?> getComparator(int i, ByteBuffer bb);

    /**
     * Adds DynamicCompositeType type information from @param bb1 to @param bb2.
     * @param i is ignored.
     */
    abstract protected AbstractType<?> getComparator(int i, ByteBuffer bb1, ByteBuffer bb2);

    /**
     * Adds type information from @param bb to @param sb.  @param i is ignored.
     */
    abstract protected AbstractType<?> getAndAppendComparator(int i, ByteBuffer bb, StringBuilder sb);

    /**
     * Like getComparator, but validates that @param i does not exceed the defined range
     */
    abstract protected AbstractType<?> validateComparator(int i, ByteBuffer bb) throws MarshalException;

    /**
     * Used by fromString
     */
    abstract protected ParsedComparator parseComparator(int i, String part);

    protected static interface ParsedComparator
    {
        AbstractType<?> getAbstractType();
        String getRemainingPart();
        int getComparatorSerializedSize();
        void serializeComparator(ByteBuffer bb);
    }
}
