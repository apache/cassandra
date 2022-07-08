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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import de.huxhorn.sulky.ulid.ULID;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

/**
 * SSTable generation identifiers that can be stored across nodes in one directory/bucket
 * Uses the ULID based identifiers
 */
public final class ULIDBasedSSTableId implements SSTableId, Comparable<ULIDBasedSSTableId>
{
    public static final int STRING_LEN = 26;
    public static final int BYTES_LEN = 16;

    final ULID.Value ulid;
    final TimeUUID approximateTimeUUID;

    public ULIDBasedSSTableId(ULID.Value ulid)
    {
        this.ulid = ulid;
        this.approximateTimeUUID = TimeUUID.approximateFromULID(ulid);
    }

    @Override
    public ByteBuffer asBytes()
    {
        return ByteBuffer.wrap(ulid.toBytes());
    }

    @Override
    public String toString()
    {
        return ulid.toString();
    }

    @Override
    public int compareTo(ULIDBasedSSTableId o)
    {
        if (o == null)
            return 1;
        else if (o == this)
            return 0;

        return ulid.compareTo(o.ulid);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ULIDBasedSSTableId that = (ULIDBasedSSTableId) o;
        return ulid.equals(that.ulid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ulid);
    }

    public static class Builder implements SSTableId.Builder<ULIDBasedSSTableId>
    {
        private static final Pattern PATTERN = Pattern.compile("[0-9a-z]{26}", Pattern.CASE_INSENSITIVE);

        public static final Builder instance = new Builder();

        private static final ULID ulid = new ULID();
        private static final AtomicReference<ULID.Value> prevRef = new AtomicReference<>();

        /**
         * Creates a new ULID based identifiers generator.
         *
         * @param existingIdentifiers not used by UUID based generator
         */
        @Override
        public Supplier<ULIDBasedSSTableId> generator(Stream<SSTableId> existingIdentifiers)
        {
            return () -> {
                ULID.Value prevVal;
                ULID.Value newVal = null;
                do
                {
                    prevVal = prevRef.get();
                    if (prevVal != null)
                    {
                        Optional<ULID.Value> newValOpt = ulid.nextStrictlyMonotonicValue(prevVal);
                        if (!newValOpt.isPresent())
                            continue;
                        newVal = newValOpt.get();
                    }
                    else
                    {
                        newVal = ulid.nextValue();
                    }
                } while (newVal != null && !prevRef.compareAndSet(prevVal, newVal));
                return new ULIDBasedSSTableId(newVal);
            };
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
        public ULIDBasedSSTableId fromString(@Nonnull String s) throws IllegalArgumentException
        {
            Matcher m = PATTERN.matcher(s);
            if (!m.matches())
                throw new IllegalArgumentException("String '" + s + "' is not a valid ULID based sstable identifier");

            return new ULIDBasedSSTableId(ULID.parseULID(s));
        }

        @Override
        public ULIDBasedSSTableId fromBytes(@Nonnull ByteBuffer bytes) throws IllegalArgumentException
        {
            Preconditions.checkArgument(bytes.remaining() == ULIDBasedSSTableId.BYTES_LEN,
                                        "Buffer does not have a valid number of bytes remaining. Expecting: %s but was: %s",
                                        ULIDBasedSSTableId.BYTES_LEN, bytes.remaining());

            return new ULIDBasedSSTableId(ULID.fromBytes(ByteBufferUtil.getArray(bytes)));
        }
    }
}
