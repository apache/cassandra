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
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A class avoiding class duplication between CompositeType and
 * DynamicCompositeType.
 * Those two differs only in that for DynamicCompositeType, the comparators
 * are in the encoded column name at the front of each component.
 */
public abstract class AbstractCompositeType extends AbstractType<ByteBuffer>
{
    protected AbstractCompositeType()
    {
        super(ComparisonType.CUSTOM);
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        boolean isStatic1 = readIsStatic(left, accessorL);
        boolean isStatic2 = readIsStatic(right, accessorR);
        if (isStatic1 != isStatic2)
            return isStatic1 ? -1 : 1;

        int i = 0;

        VL previous = null;
        int offset1 = startingOffset(isStatic1);
        int offset2 = startingOffset(isStatic2);

        while (accessorL.sizeFromOffset(left, offset1) > 0 && accessorR.sizeFromOffset(right, offset1) > 0)
        {
            AbstractType<?> comparator = getComparator(i, left, accessorL, right, accessorR, offset1, offset2);
            offset1 += getComparatorSize(i, left, accessorL, offset1);
            offset2 += getComparatorSize(i, right, accessorR, offset2);

            VL value1 = accessorL.sliceWithShortLength(left, offset1);
            offset1 += accessorL.sizeWithShortLength(value1);
            VR value2 = accessorR.sliceWithShortLength(right, offset2);
            offset2 += accessorR.sizeWithShortLength(value2);

            int cmp = comparator.compareCollectionMembers(value1, accessorL, value2, accessorR, previous);
            if (cmp != 0)
                return cmp;

            previous = value1;

            byte b1 = accessorL.getByte(left, offset1++);
            byte b2 = accessorR.getByte(right, offset2++);
            if (b1 != b2)
                return b1 - b2;

            ++i;
        }

        if (accessorL.sizeFromOffset(left, offset1) == 0)
            return accessorR.sizeFromOffset(right, offset2) == 0 ? 0 : -1;

        // left.remaining() > 0 && right.remaining() == 0
        return 1;
    }

    // Check if the provided BB represents a static name and advance the
    // buffer to the real beginning if so.
    protected abstract <V> boolean readIsStatic(V value, ValueAccessor<V> handle);

    protected abstract int startingOffset(boolean isStatic);

    /**
     * Split a composite column names into it's components.
     */
    public ByteBuffer[] split(ByteBuffer bb)
    {
        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        boolean isStatic = readIsStatic(bb, ByteBufferAccessor.instance);
        int offset = startingOffset(isStatic);

        int i = 0;
        while (ByteBufferAccessor.instance.sizeFromOffset(bb, offset) > 0)
        {
            offset += getComparatorSize(i++, bb, ByteBufferAccessor.instance, offset);
            ByteBuffer value = ByteBufferAccessor.instance.sliceWithShortLength(bb, offset);
            offset += ByteBufferAccessor.instance.sizeWithShortLength(value);
            l.add(value);
            offset++; // skip end-of-component
        }
        return l.toArray(new ByteBuffer[l.size()]);
    }

    private static final String COLON = ":";
    private static final Pattern COLON_PAT = Pattern.compile(COLON);
    private static final String ESCAPED_COLON = "\\\\:";
    private static final Pattern ESCAPED_COLON_PAT = Pattern.compile(ESCAPED_COLON);


    /*
     * Escapes all occurences of the ':' character from the input, replacing them by "\:".
     * Furthermore, if the last character is '\' or '!', a '!' is appended.
     */
    public static String escape(String input)
    {
        if (input.isEmpty())
            return input;

        String res = COLON_PAT.matcher(input).replaceAll(ESCAPED_COLON);
        char last = res.charAt(res.length() - 1);
        return last == '\\' || last == '!' ? res + '!' : res;
    }

    /*
     * Reverses the effect of espace().
     * Replaces all occurences of "\:" by ":" and remove last character if it is '!'.
     */
    static String unescape(String input)
    {
        if (input.isEmpty())
            return input;

        String res = ESCAPED_COLON_PAT.matcher(input).replaceAll(COLON);
        char last = res.charAt(res.length() - 1);
        return last == '!' ? res.substring(0, res.length() - 1) : res;
    }

    /*
     * Split the input on character ':', unless the previous character is '\'.
     */
    static List<String> split(String input)
    {
        if (input.isEmpty())
            return Collections.<String>emptyList();

        List<String> res = new ArrayList<String>();
        int prev = 0;
        for (int i = 0; i < input.length(); i++)
        {
            if (input.charAt(i) != ':' || (i > 0 && input.charAt(i-1) == '\\'))
                continue;

            res.add(input.substring(prev, i));
            prev = i + 1;
        }
        res.add(input.substring(prev, input.length()));
        return res;
    }

    public <V> String getString(V input, ValueAccessor<V> handle)
    {
        StringBuilder sb = new StringBuilder();
        boolean isStatic  = readIsStatic(input, handle);
        int offset = startingOffset(isStatic);
        int startOffset = offset;

        int i = 0;
        while (handle.sizeFromOffset(input, offset) > 0)
        {
            if (offset != startOffset)
                sb.append(":");

            AbstractType<?> comparator = getAndAppendComparator(i, input, handle, sb, offset);
            offset += getComparatorSize(i, input, handle, offset);
            V value = handle.sliceWithShortLength(input, offset);
            offset += handle.sizeWithShortLength(value);

            sb.append(escape(comparator.getString(value, handle)));

            byte b = handle.getByte(input, offset++);
            if (b != 0)
            {
                sb.append(b < 0 ? ":_" : ":!");
                break;
            }
            ++i;
        }
        return sb.toString();
    }

    public ByteBuffer fromString(String source)
    {
        List<String> parts = split(source);
        List<ByteBuffer> components = new ArrayList<ByteBuffer>(parts.size());
        List<ParsedComparator> comparators = new ArrayList<ParsedComparator>(parts.size());
        int totalLength = 0, i = 0;
        boolean lastByteIsOne = false;
        boolean lastByteIsMinusOne = false;

        for (String part : parts)
        {
            if (part.equals("!"))
            {
                lastByteIsOne = true;
                break;
            }
            else if (part.equals("_"))
            {
                lastByteIsMinusOne = true;
                break;
            }

            ParsedComparator p = parseComparator(i, part);
            AbstractType<?> type = p.getAbstractType();
            part = p.getRemainingPart();

            ByteBuffer component = type.fromString(unescape(part));
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
            ByteBufferUtil.writeShortLength(bb, component.remaining());
            bb.put(component); // it's ok to consume component as we won't use it anymore
            bb.put((byte)0);
            ++i;
        }
        if (lastByteIsOne)
            bb.put(bb.limit() - 1, (byte)1);
        else if (lastByteIsMinusOne)
            bb.put(bb.limit() - 1, (byte)-1);

        bb.rewind();
        return bb;
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
    public void validate(ByteBuffer bb) throws MarshalException
    {
        validate(bb, ByteBufferAccessor.instance);
    }

    public  <V> void validate(V input, ValueAccessor<V> handle)
    {
        boolean isStatic = readIsStatic(input, handle);
        int offset = startingOffset(isStatic);

        int i = 0;
        V previous = null;
        while (handle.sizeFromOffset(input, offset) > 0)
        {
            AbstractType<?> comparator = validateComparator(i, input, handle, offset);
            offset += getComparatorSize(i, input, handle, offset);

            if (handle.sizeFromOffset(input, offset) < 2)
                throw new MarshalException("Not enough bytes to read value size of component " + i);
            int length = handle.getShort(input, offset);
            offset += 2;

            if (handle.sizeFromOffset(input, offset) < length)
                throw new MarshalException("Not enough bytes to read value of component " + i);
            V value = handle.slice(input, offset, length);
            offset += length;

            comparator.validateCollectionMember(value, previous, handle);

            if (handle.sizeFromOffset(input, offset) == 0)
                throw new MarshalException("Not enough bytes to read the end-of-component byte of component" + i);
            byte b = handle.getByte(input, offset++);
            if (b != 0 && handle.sizeFromOffset(input, offset) != 0)
                throw new MarshalException("Invalid bytes remaining after an end-of-component at component" + i);

            previous = value;
            ++i;
        }
    }

    public abstract <V> V decompose(ValueAccessor<V> accessor, Object... objects);

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return BytesSerializer.instance;
    }

    abstract protected <V> int getComparatorSize(int i, V value, ValueAccessor<V> handle, int offset);
    /**
     * @return the comparator for the given component. static CompositeType will consult
     * @param i DynamicCompositeType will read the type information from @param bb
     * @param value name of type definition
     */
    abstract protected <V> AbstractType<?> getComparator(int i, V value, ValueAccessor<V> handle, int offset);

    /**
     * Adds DynamicCompositeType type information from @param bb1 to @param bb2.
     * @param i is ignored.
     */
    abstract protected <VL, VR> AbstractType<?> getComparator(int i, VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, int offset1, int offset2);

    /**
     * Adds type information from @param bb to @param sb.  @param i is ignored.
     */
    abstract protected <V> AbstractType<?> getAndAppendComparator(int i, V value, ValueAccessor<V> handle, StringBuilder sb, int offset);

    /**
     * Like getComparator, but validates that @param i does not exceed the defined range
     */
    abstract protected <V> AbstractType<?> validateComparator(int i, V value, ValueAccessor<V> handle, int offset) throws MarshalException;

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
