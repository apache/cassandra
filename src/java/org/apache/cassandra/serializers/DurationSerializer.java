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

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class DurationSerializer extends TypeSerializer<Duration>
{
    public static final DurationSerializer instance = new DurationSerializer();

    public ByteBuffer serialize(Duration duration)
    {
        if (duration == null)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long months = duration.getMonths();
        long days = duration.getDays();
        long nanoseconds = duration.getNanoseconds();

        int size = VIntCoding.computeVIntSize(months)
                   + VIntCoding.computeVIntSize(days)
                   + VIntCoding.computeVIntSize(nanoseconds);

        try (DataOutputBufferFixed output = new DataOutputBufferFixed(size))
        {
            output.writeVInt(months);
            output.writeVInt(days);
            output.writeVInt(nanoseconds);
            return output.buffer();
        }
        catch (IOException e)
        {
            // this should never happen with a DataOutputBufferFixed
            throw new AssertionError("Unexpected error", e);
        }
    }

    public <V> Duration deserialize(V value, ValueAccessor<V> accessor)
    {
        if (accessor.isEmpty(value))
            return null;

        try (DataInputBuffer in = new DataInputBuffer(accessor.toBuffer(value), true))  // TODO: make a value input buffer
        {
            int months = in.readVInt32();
            int days = in.readVInt32();
            long nanoseconds = in.readVInt();
            return Duration.newInstance(months, days, nanoseconds);
        }
        catch (IOException e)
        {
            // this should never happen with a DataInputBuffer
            throw new AssertionError("Unexpected error", e);
        }
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.size(value) < 3)
            throw new MarshalException(String.format("Expected at least 3 bytes for a duration (%d)", accessor.size(value)));

        try (DataInputBuffer in = new DataInputBuffer(accessor.toBuffer(value), true))  // FIXME: value input buffer
        {
            long monthsAsLong = in.readVInt();
            long daysAsLong = in.readVInt();
            long nanoseconds = in.readVInt();

            if (!canBeCastToInt(monthsAsLong))
                throw new MarshalException(String.format("The duration months must be a 32 bits integer but was: %d",
                                                         monthsAsLong));
            if (!canBeCastToInt(daysAsLong))
                throw new MarshalException(String.format("The duration days must be a 32 bits integer but was: %d",
                                                         daysAsLong));
            int months = (int) monthsAsLong;
            int days = (int) daysAsLong;

            if (!((months >= 0 && days >= 0 && nanoseconds >= 0) || (months <= 0 && days <=0 && nanoseconds <=0)))
                throw new MarshalException(String.format("The duration months, days and nanoseconds must be all of the same sign (%d, %d, %d)",
                                                         months, days, nanoseconds));
        }
        catch (IOException e)
        {
            // this should never happen with a DataInputBuffer
            throw new AssertionError("Unexpected error", e);
        }
    }

    /**
     * Checks that the specified {@code long} can be cast to an {@code int} without information lost.
     *
     * @param l the {@code long} to check
     * @return {@code true} if the specified {@code long} can be cast to an {@code int} without information lost,
     * {@code false} otherwise.
     */
    private boolean canBeCastToInt(long l)
    {
        return ((int) l) == l;
    }

    public String toString(Duration duration)
    {
        return duration == null ? "" : duration.toString();
    }

    public Class<Duration> getType()
    {
        return Duration.class;
    }
}
