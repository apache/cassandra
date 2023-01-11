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

package org.apache.cassandra.serializers;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.google.common.collect.Range;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Common superclass for {@link SetSerializer} and {@link MapSerializer}, considering a set as a map without values.
 */
abstract class AbstractMapSerializer<T> extends CollectionSerializer<T>
{
    private final boolean hasValues;
    private final String name;

    protected AbstractMapSerializer(boolean hasValues)
    {
        this.hasValues = hasValues;
        name = hasValues ? "map" : "set";
    }

    @Override
    public ByteBuffer getSliceFromSerialized(ByteBuffer collection,
                                             ByteBuffer from,
                                             ByteBuffer to,
                                             AbstractType<?> comparator,
                                             boolean frozen)
    {
        if (from == ByteBufferUtil.UNSET_BYTE_BUFFER && to == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return collection;

        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ByteBufferAccessor.instance);
            input.position(input.position() + sizeOfCollectionSize());
            int startPos = input.position();
            int count = 0;
            boolean inSlice = from == ByteBufferUtil.UNSET_BYTE_BUFFER;

            for (int i = 0; i < n; i++)
            {
                int pos = input.position();
                ByteBuffer key = readValue(input, ByteBufferAccessor.instance, 0);
                input.position(input.position() + sizeOfValue(key, ByteBufferAccessor.instance));

                // If we haven't passed the start already, check if we have now
                if (!inSlice)
                {
                    int comparison = comparator.compareForCQL(from, key);
                    if (comparison <= 0)
                    {
                        // We're now within the slice
                        inSlice = true;
                        startPos = pos;
                    }
                    else
                    {
                        // We're before the slice, so we know we don't care about this element
                        skipMapValue(input);
                        continue;
                    }
                }

                // Now check if we're done
                int comparison = to == ByteBufferUtil.UNSET_BYTE_BUFFER ? -1 : comparator.compareForCQL(key, to);
                if (comparison > 0)
                {
                    // We're done and shouldn't include the key we just read
                    input.position(pos);
                    break;
                }

                // Otherwise, we'll include that element
                skipMapValue(input); // value
                ++count;

                // But if we know it was the last of the slice, we break early
                if (comparison == 0)
                    break;
            }

            if (count == 0 && !frozen)
                return null;

            return copyAsNewCollection(collection, count, startPos, input.position());
        }
        catch (BufferUnderflowException | IndexOutOfBoundsException e)
        {
            throw new MarshalException("Not enough bytes to read a " + name);
        }
    }

    @Override
    public int getIndexFromSerialized(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator)
    {
        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ByteBufferAccessor.instance);
            int offset = sizeOfCollectionSize();
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, ByteBufferAccessor.instance, offset);
                offset += sizeOfValue(kbb, ByteBufferAccessor.instance);
                int comparison = comparator.compareForCQL(kbb, key);

                if (comparison == 0)
                    return i;

                if (comparison > 0)
                    // since the set is in sorted order, we know we've gone too far and the element doesn't exist
                    return -1;

                // comparison < 0
                if (hasValues)
                    offset += skipValue(input, ByteBufferAccessor.instance, offset);
            }
            return -1;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a " + name);
        }
    }

    @Override
    public Range<Integer> getIndexesRangeFromSerialized(ByteBuffer collection,
                                                        ByteBuffer from,
                                                        ByteBuffer to,
                                                        AbstractType<?> comparator)
    {
        if (from == ByteBufferUtil.UNSET_BYTE_BUFFER && to == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return Range.closed(0, Integer.MAX_VALUE);

        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ByteBufferAccessor.instance);
            input.position(input.position() + sizeOfCollectionSize());
            int start = from == ByteBufferUtil.UNSET_BYTE_BUFFER ? 0 : -1;
            int end = to == ByteBufferUtil.UNSET_BYTE_BUFFER ? n : -1;

            for (int i = 0; i < n; i++)
            {
                if (start >= 0 && end >= 0)
                    break;
                else if (i > 0)
                    skipMapValue(input);

                ByteBuffer key = readValue(input, ByteBufferAccessor.instance, 0);
                input.position(input.position() + sizeOfValue(key, ByteBufferAccessor.instance));

                if (start < 0)
                {
                    int comparison = comparator.compareForCQL(from, key);
                    if (comparison <= 0)
                        start = i;
                    else
                        continue;
                }

                if (end < 0)
                {
                    int comparison = comparator.compareForCQL(key, to);
                    if (comparison > 0)
                        end = i;
                }
            }

            if (start < 0)
                return Range.closedOpen(0, 0);

            if (end < 0)
                return Range.closedOpen(start, n);

            return Range.closedOpen(start, end);
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a " + name);
        }
    }

    private void skipMapValue(ByteBuffer input)
    {
        if (hasValues)
            skipValue(input);
    }
}
