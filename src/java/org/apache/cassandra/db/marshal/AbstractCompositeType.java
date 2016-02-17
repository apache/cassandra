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

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        boolean isStatic1 = readIsStatic(bb1);
        boolean isStatic2 = readIsStatic(bb2);
        if (isStatic1 != isStatic2)
            return isStatic1 ? -1 : 1;

        int i = 0;

        ByteBuffer previous = null;

        while (bb1.remaining() > 0 && bb2.remaining() > 0)
        {
            AbstractType<?> comparator = getComparator(i, bb1, bb2);

            ByteBuffer value1 = ByteBufferUtil.readBytesWithShortLength(bb1);
            ByteBuffer value2 = ByteBufferUtil.readBytesWithShortLength(bb2);

            int cmp = comparator.compareCollectionMembers(value1, value2, previous);
            if (cmp != 0)
                return cmp;

            previous = value1;

            byte b1 = bb1.get();
            byte b2 = bb2.get();
            if (b1 != b2)
                return b1 - b2;

            ++i;
        }

        if (bb1.remaining() == 0)
            return bb2.remaining() == 0 ? 0 : -1;

        // bb1.remaining() > 0 && bb2.remaining() == 0
        return 1;
    }

    // Check if the provided BB represents a static name and advance the
    // buffer to the real beginning if so.
    protected abstract boolean readIsStatic(ByteBuffer bb);

    /**
     * Split a composite column names into it's components.
     */
    public ByteBuffer[] split(ByteBuffer name)
    {
        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        ByteBuffer bb = name.duplicate();
        readIsStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            getComparator(i++, bb);
            l.add(ByteBufferUtil.readBytesWithShortLength(bb));
            bb.get(); // skip end-of-component
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

    public String getString(ByteBuffer bytes)
    {
        StringBuilder sb = new StringBuilder();
        ByteBuffer bb = bytes.duplicate();
        readIsStatic(bb);

        int i = 0;
        while (bb.remaining() > 0)
        {
            if (bb.remaining() != bytes.remaining())
                sb.append(":");

            AbstractType<?> comparator = getAndAppendComparator(i, bb, sb);
            ByteBuffer value = ByteBufferUtil.readBytesWithShortLength(bb);

            sb.append(escape(comparator.getString(value)));

            byte b = bb.get();
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
    public String toJSONString(ByteBuffer buffer, int protocolVersion)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        ByteBuffer bb = bytes.duplicate();
        readIsStatic(bb);

        int i = 0;
        ByteBuffer previous = null;
        while (bb.remaining() > 0)
        {
            AbstractType<?> comparator = validateComparator(i, bb);

            if (bb.remaining() < 2)
                throw new MarshalException("Not enough bytes to read value size of component " + i);
            int length = ByteBufferUtil.readShortLength(bb);

            if (bb.remaining() < length)
                throw new MarshalException("Not enough bytes to read value of component " + i);
            ByteBuffer value = ByteBufferUtil.readBytes(bb, length);

            comparator.validateCollectionMember(value, previous);

            if (bb.remaining() == 0)
                throw new MarshalException("Not enough bytes to read the end-of-component byte of component" + i);
            byte b = bb.get();
            if (b != 0 && bb.remaining() != 0)
                throw new MarshalException("Invalid bytes remaining after an end-of-component at component" + i);

            previous = value;
            ++i;
        }
    }

    public abstract ByteBuffer decompose(Object... objects);

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return BytesSerializer.instance;
    }

    @Override
    public boolean referencesUserType(String name)
    {
        return getComponents().stream().anyMatch(f -> f.referencesUserType(name));
    }

    /**
     * @return the comparator for the given component. static CompositeType will consult
     * @param i DynamicCompositeType will read the type information from @param bb
     * @param bb name of type definition
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
