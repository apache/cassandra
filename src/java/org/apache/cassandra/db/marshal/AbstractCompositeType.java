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

    @Override
    public boolean allowsEmpty()
    {
        return true;
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        boolean isStaticL = readIsStatic(left, accessorL);
        boolean isStaticR = readIsStatic(right, accessorR);
        if (isStaticL != isStaticR)
            return isStaticL ? -1 : 1;

        int i = 0;

        VL previous = null;
        int offsetL = startingOffset(isStaticL);
        int offsetR = startingOffset(isStaticR);

        while (!accessorL.isEmptyFromOffset(left, offsetL) && !accessorR.isEmptyFromOffset(right, offsetL))
        {
            AbstractType<?> comparator = getComparator(i, left, accessorL, right, accessorR, offsetL, offsetR);
            offsetL += getComparatorSize(i, left, accessorL, offsetL);
            offsetR += getComparatorSize(i, right, accessorR, offsetR);

            VL value1 = accessorL.sliceWithShortLength(left, offsetL);
            offsetL += accessorL.sizeWithShortLength(value1);
            VR value2 = accessorR.sliceWithShortLength(right, offsetR);
            offsetR += accessorR.sizeWithShortLength(value2);

            int cmp = comparator.compareCollectionMembers(value1, accessorL, value2, accessorR, previous);
            if (cmp != 0)
                return cmp;

            previous = value1;

            byte bL = accessorL.getByte(left, offsetL++);
            byte bR = accessorR.getByte(right, offsetR++);
            if (bL != bR)
                return bL - bR;

            ++i;
        }

        if (accessorL.isEmptyFromOffset(left, offsetL))
            return accessorR.sizeFromOffset(right, offsetR) == 0 ? 0 : -1;

        // left.remaining() > 0 && right.remaining() == 0
        return 1;
    }

    // Check if the provided BB represents a static name and advance the
    // buffer to the real beginning if so.
    protected abstract <V> boolean readIsStatic(V value, ValueAccessor<V> accessor);

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
        while (!ByteBufferAccessor.instance.isEmptyFromOffset(bb, offset))
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

    public <V> String getString(V input, ValueAccessor<V> accessor)
    {
        StringBuilder sb = new StringBuilder();
        boolean isStatic  = readIsStatic(input, accessor);
        int offset = startingOffset(isStatic);
        int startOffset = offset;

        int i = 0;
        while (!accessor.isEmptyFromOffset(input, offset))
        {
            if (offset != startOffset)
                sb.append(":");

            AbstractType<?> comparator = getAndAppendComparator(i, input, accessor, sb, offset);
            offset += getComparatorSize(i, input, accessor, offset);
            V value = accessor.sliceWithShortLength(input, offset);
            offset += accessor.sizeWithShortLength(value);

            sb.append(escape(comparator.getString(value, accessor)));

            byte b = accessor.getByte(input, offset++);
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
        List<ByteBuffer> components = new ArrayList<>(parts.size());
        List<ParsedComparator> comparators = new ArrayList<>(parts.size());
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
            type.validate(component);
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
            bb.put(component.duplicate()); // it's not ok to consume component as we did not create it (CASSANDRA-14752)
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
        // TODO: suport toJSONString (CASSANDRA-18177)
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate(ByteBuffer bb) throws MarshalException
    {
        validate(bb, ByteBufferAccessor.instance);
    }

    public  <V> void validate(V input, ValueAccessor<V> accessor)
    {
        boolean isStatic = readIsStatic(input, accessor);
        int offset = startingOffset(isStatic);

        int i = 0;
        V previous = null;
        while (!accessor.isEmptyFromOffset(input, offset))
        {
            AbstractType<?> comparator = validateComparator(i, input, accessor, offset);
            offset += getComparatorSize(i, input, accessor, offset);

            if (accessor.sizeFromOffset(input, offset) < 2)
                throw new MarshalException("Not enough bytes to read value size of component " + i);
            int length = accessor.getUnsignedShort(input, offset);
            offset += 2;

            if (accessor.sizeFromOffset(input, offset) < length)
                throw new MarshalException("Not enough bytes to read value of component " + i);
            V value = accessor.slice(input, offset, length);
            offset += length;

            comparator.validateCollectionMember(value, previous, accessor);

            if (accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Not enough bytes to read the end-of-component byte of component" + i);
            byte b = accessor.getByte(input, offset++);
            if (b != 0 && !accessor.isEmptyFromOffset(input, offset))
                throw new MarshalException("Invalid bytes remaining after an end-of-component at component" + i);

            previous = value;
            ++i;
        }
    }

    public abstract ByteBuffer decompose(Object... objects);

    abstract protected <V> int getComparatorSize(int i, V value, ValueAccessor<V> accessor, int offset);
    /**
     * @return the comparator for the given component. static CompositeType will consult
     * @param i DynamicCompositeType will read the type information from @param bb
     * @param value name of type definition
     */
    abstract protected <V> AbstractType<?> getComparator(int i, V value, ValueAccessor<V> accessor, int offset);

    /**
     * Adds DynamicCompositeType type information from @param bb1 to @param bb2.
     * @param i is ignored.
     */
    abstract protected <VL, VR> AbstractType<?> getComparator(int i, VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, int offsetL, int offsetR);

    /**
     * Adds type information from @param bb to @param sb.  @param i is ignored.
     */
    abstract protected <V> AbstractType<?> getAndAppendComparator(int i, V value, ValueAccessor<V> accessor, StringBuilder sb, int offset);

    /**
     * Like getComparator, but validates that @param i does not exceed the defined range
     */
    abstract protected <V> AbstractType<?> validateComparator(int i, V value, ValueAccessor<V> accessor, int offset) throws MarshalException;

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
