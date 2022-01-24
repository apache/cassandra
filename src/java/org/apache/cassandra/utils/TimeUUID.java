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

package org.apache.cassandra.utils;

import java.util.UUID;

public class TimeUUID implements Comparable<TimeUUID>
{
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    public static final long UUID_EPOCH_UNIX_MILLIS = -12219292800000L;
    protected static final long TIMESTAMP_UUID_VERSION_IN_MSB = 0x1000L;
    protected static final long UUID_VERSION_BITS_IN_MSB = 0xf000L;

    /*
     * The min and max possible lsb for a UUID.
     * Note that his is not 0 and all 1's because Cassandra TimeUUIDType
     * compares the lsb parts as a signed byte array comparison. So the min
     * value is 8 times -128 and the max is 8 times +127.
     *
     * Note that we ignore the uuid variant (namely, MIN_CLOCK_SEQ_AND_NODE
     * have variant 2 as it should, but MAX_CLOCK_SEQ_AND_NODE have variant 0).
     * I don't think that has any practical consequence and is more robust in
     * case someone provides a UUID with a broken variant.
     */
    private static final long MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
    private static final long MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

    final long uuidTimestamp, lsb;

    public TimeUUID(long uuidTimestamp, long lsb)
    {
        // we don't validate that this is a true TIMEUUID to avoid problems with historical mixing of UUID with TimeUUID
        this.uuidTimestamp = uuidTimestamp;
        this.lsb = lsb;
    }

    public static TimeUUID atUnixMicrosWithLsb(long unixMicros, long uniqueLsb)
    {
        return new TimeUUID(unixMicrosToRawTimestamp(unixMicros), uniqueLsb);
    }

    public static UUID atUnixMicrosWithLsbAsUUID(long unixMicros, long uniqueLsb)
    {
        return new UUID(rawTimestampToMsb(unixMicrosToRawTimestamp(unixMicros)), uniqueLsb);
    }

    /**
     * Returns the smaller possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    public static TimeUUID minAtUnixMillis(long unixMillis)
    {
        return new TimeUUID(unixMillisToRawTimestamp(unixMillis, 0), MIN_CLOCK_SEQ_AND_NODE);
    }

    public static TimeUUID fromUuid(UUID uuid)
    {
        return fromBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static TimeUUID fromBytes(long msb, long lsb)
    {
        return new TimeUUID(msbToRawTimestamp(msb), lsb);
    }

    public UUID asUUID()
    {
        return new UUID(rawTimestampToMsb(uuidTimestamp), lsb);
    }

    /**
     * The UUID-format timestamp, i.e. 10x micros-resolution, as of UUIDGen.UUID_EPOCH_UNIX_MILLIS
     * The tenths of a microsecond are used to store a flag value.
     */
    public long uuidTimestamp()
    {
        return uuidTimestamp & 0x0FFFFFFFFFFFFFFFL;
    }

    public long msb()
    {
        return rawTimestampToMsb(uuidTimestamp);
    }

    public long lsb()
    {
        return lsb;
    }

    public static long unixMillisToRawTimestamp(long unixMillis, long tenthsOfAMicro)
    {
        return unixMillis * 10000 - (UUID_EPOCH_UNIX_MILLIS * 10000) + tenthsOfAMicro;
    }

    public static long unixMicrosToRawTimestamp(long unixMicros)
    {
        return unixMicros * 10 - (UUID_EPOCH_UNIX_MILLIS * 10000);
    }

    public static long msbToRawTimestamp(long msb)
    {
        assert (UUID_VERSION_BITS_IN_MSB & msb) == TIMESTAMP_UUID_VERSION_IN_MSB;
        msb &= ~TIMESTAMP_UUID_VERSION_IN_MSB;
        return (msb & 0xFFFFL) << 48
               | (msb & 0xFFFF0000L) << 16
               | (msb >>> 32);
    }

    public static long rawTimestampToMsb(long rawTimestamp)
    {
        return TIMESTAMP_UUID_VERSION_IN_MSB
               | (rawTimestamp >>> 48)
               | ((rawTimestamp & 0xFFFF00000000L) >>> 16)
               | (rawTimestamp << 32);
    }

    @Override
    public int hashCode()
    {
        return (int) ((uuidTimestamp ^ (uuidTimestamp >> 32) * 31) + (lsb ^ (lsb >> 32)));
    }

    @Override
    public boolean equals(Object that)
    {
        return (that instanceof UUID && equals((UUID) that))
               || (that instanceof TimeUUID && equals((TimeUUID) that));
    }

    public boolean equals(TimeUUID that)
    {
        return that != null && uuidTimestamp == that.uuidTimestamp && lsb == that.lsb;
    }

    public boolean equals(UUID that)
    {
        return that != null && uuidTimestamp == that.timestamp() && lsb == that.getLeastSignificantBits();
    }

    @Override
    public String toString()
    {
        return asUUID().toString();
    }

    @Override
    public int compareTo(TimeUUID that)
    {
        return this.uuidTimestamp != that.uuidTimestamp
               ? Long.compare(this.uuidTimestamp, that.uuidTimestamp)
               : Long.compare(this.lsb, that.lsb);
    }

    public static class Generator
    {
        public static TimeUUID nextTimeUUID()
        {
            return TimeUUID.fromUuid(UUIDGen.getTimeUUID());
        }

        public static TimeUUID atUnixMillis(long unixMillis)
        {
            return TimeUUID.fromUuid(UUIDGen.getTimeUUID(unixMillis));
        }
    }
}
