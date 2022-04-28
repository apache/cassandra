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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.TimeUUID;

/**
 * SSTable generation identifiers that can be stored across nodes in one directory/bucket
 * <p>
 * Uses the UUID v1 identifiers
 */
public final class UUIDBasedSSTableId implements SSTableId, Comparable<UUIDBasedSSTableId>
{
    public final static int STRING_LEN = 28;
    public final static int BYTES_LEN = 16;

    private final TimeUUID uuid;
    private final String repr;

    public UUIDBasedSSTableId(TimeUUID uuid)
    {
        this.uuid = uuid;
        this.repr = asString();
    }

    @Override
    public ByteBuffer asBytes()
    {
        return ByteBuffer.allocate(16)
                         .order(ByteOrder.BIG_ENDIAN)
                         .putLong(0, uuid.uuidTimestamp())
                         .putLong(Long.BYTES, uuid.lsb());
    }

    private String asString()
    {
        long ts = uuid.uuidTimestamp();
        long nanoPart = ts % 10_000_000;
        ts = ts / 10_000_000;
        long seconds = ts % 86_400;
        ts = ts / 86_400;

        return String.format("%4s_%4s_%5s%13s",
                             Long.toString(ts, 36),
                             Long.toString(seconds, 36),
                             Long.toString(nanoPart, 36),
                             Long.toUnsignedString(uuid.lsb(), 36)).replace(' ', '0');
    }

    @Override
    public String toString()
    {
        return repr;
    }

    @Override
    public int compareTo(UUIDBasedSSTableId o)
    {
        if (o == null)
            return 1;
        else if (o == this)
            return 0;

        return uuid.compareTo(o.uuid);
    }

    @Override
    public boolean equals(Object o)
    {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass())
            return false;
        UUIDBasedSSTableId that = (UUIDBasedSSTableId) o;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    public static class Builder implements SSTableId.Builder<UUIDBasedSSTableId>
    {
        public static final Builder instance = new Builder();
        private final static Pattern PATTERN = Pattern.compile("([0-9a-z]{4})_([0-9a-z]{4})_([0-9a-z]{5})([0-9a-z]{13})", Pattern.CASE_INSENSITIVE);

        /**
         * Creates a new UUID based identifiers generator.
         *
         * @param existingIdentifiers not used by UUID based generator
         */
        @Override
        public Supplier<UUIDBasedSSTableId> generator(Stream<SSTableId> existingIdentifiers)
        {
            return () -> new UUIDBasedSSTableId(TimeUUID.Generator.nextTimeUUID());
        }

        @Override
        public boolean isUniqueIdentifier(String str)
        {
            return str != null && str.length() == STRING_LEN && PATTERN.matcher(str).matches();
        }

        @Override
        public boolean isUniqueIdentifier(ByteBuffer bytes)
        {
            return bytes != null && bytes.remaining() == BYTES_LEN;
        }

        @Override
        public UUIDBasedSSTableId fromString(@Nonnull String s) throws IllegalArgumentException
        {
            Matcher m = PATTERN.matcher(s);
            if (!m.matches())
                throw new IllegalArgumentException("String '" + s + "' is not a valid UUID based sstable identifier");

            long dayPart = Long.parseLong(m.group(1), 36);
            long secondPart = Long.parseLong(m.group(2), 36);
            long nanoPart = Long.parseLong(m.group(3), 36);
            long ts = (dayPart * 86_400 + secondPart) * 10_000_000 + nanoPart;
            long randomPart = Long.parseUnsignedLong(m.group(4), 36);

            TimeUUID uuid = new TimeUUID(ts, randomPart);
            return new UUIDBasedSSTableId(uuid);
        }

        @Override
        public UUIDBasedSSTableId fromBytes(@Nonnull ByteBuffer bytes) throws IllegalArgumentException
        {
            Preconditions.checkArgument(bytes.remaining() == UUIDBasedSSTableId.BYTES_LEN, "Buffer does not have a valid number of bytes remaining. Expecting: %s but was: %s", UUIDBasedSSTableId.BYTES_LEN, bytes.remaining());
            bytes = bytes.order() == ByteOrder.BIG_ENDIAN ? bytes : bytes.duplicate().order(ByteOrder.BIG_ENDIAN);
            TimeUUID uuid = new TimeUUID(bytes.getLong(0), bytes.getLong(Long.BYTES));
            return new UUIDBasedSSTableId(uuid);
        }
    }
}
