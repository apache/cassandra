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

import de.huxhorn.sulky.ulid.ULID;

public class TimeUUID implements Comparable<TimeUUID>
{
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    public static final long UUID_EPOCH_UNIX_MILLIS = -12219292800000L;
    protected static final long TIMESTAMP_UUID_VERSION_IN_MSB = 0x1000L;
    protected static final long UUID_VERSION_BITS_IN_MSB = 0xf000L;

    final long uuidTimestamp, lsb;

    public TimeUUID(long uuidTimestamp, long lsb)
    {
        // we don't validate that this is a true TIMEUUID to avoid problems with historical mixing of UUID with TimeUUID
        this.uuidTimestamp = uuidTimestamp;
        this.lsb = lsb;
    }

    public static TimeUUID fromUuid(UUID uuid)
    {
        return fromBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static TimeUUID fromBytes(long msb, long lsb)
    {
        return new TimeUUID(msbToRawTimestamp(msb), lsb);
    }

    public static TimeUUID approximateFromULID(ULID.Value ulid)
    {
        long rawTimestamp = unixMillisToRawTimestamp(ulid.timestamp(), (10_000L * (ulid.getMostSignificantBits() & 0xFFFF)) >> 16);
        return new TimeUUID(rawTimestamp, ulid.getLeastSignificantBits());
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
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof TimeUUID)) return false;
        TimeUUID timeUUID = (TimeUUID) o;
        return uuidTimestamp == timeUUID.uuidTimestamp && lsb == timeUUID.lsb;
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

    public long unixMillis()
    {
        return (uuidTimestamp / 10_000L) + UUID_EPOCH_UNIX_MILLIS;
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
