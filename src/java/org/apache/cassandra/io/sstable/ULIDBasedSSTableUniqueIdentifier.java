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
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import de.huxhorn.sulky.ulid.ULID;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SSTable generation identifiers that can be stored across nodes in one directory/bucket
 * <p>
 * Uses the ULID identifiers which are lexicographically sortable by time: https://github.com/ulid/spec
 */
public final class ULIDBasedSSTableUniqueIdentifier implements SSTableUniqueIdentifier
{
    public final static int STRING_LEN = 26;
    public final static int BYTES_LEN = 16;

    private final ULID.Value ulid;

    ULIDBasedSSTableUniqueIdentifier(ULID.Value ulid)
    {
        this.ulid = ulid;
    }

    @Override
    public ByteBuffer asBytes()
    {
        return ByteBuffer.wrap(ulid.toBytes());
    }

    @Override
    public String asString()
    {
        return ulid.toString();
    }

    @Override
    public String toString()
    {
        return asString();
    }

    @Override
    public int compareTo(SSTableUniqueIdentifier o)
    {
        //Assumed sstables with a diff identifier are old
        return o instanceof ULIDBasedSSTableUniqueIdentifier ? ulid.compareTo(((ULIDBasedSSTableUniqueIdentifier) o).ulid) : +1;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ULIDBasedSSTableUniqueIdentifier that = (ULIDBasedSSTableUniqueIdentifier) o;
        return ulid.equals(that.ulid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ulid);
    }

    public static class Builder implements SSTableUniqueIdentifier.Builder<ULIDBasedSSTableUniqueIdentifier>
    {
        public static final Builder instance = new Builder();

        private final ULID ulid = new ULID(new Random(new SecureRandom().nextLong()));

        /**
         * Creates a new ULID based identifiers generator.
         *
         * @param existingIdentifiers not used by ULID based generator
         */
        @Override
        public Supplier<ULIDBasedSSTableUniqueIdentifier> generator(Stream<SSTableUniqueIdentifier> existingIdentifiers)
        {
            return () -> new ULIDBasedSSTableUniqueIdentifier(ulid.nextValue());
        }

        @Override
        public ULIDBasedSSTableUniqueIdentifier fromString(@Nonnull String s) throws IllegalArgumentException
        {
            return new ULIDBasedSSTableUniqueIdentifier(ULID.parseULID(s));
        }

        @Override
        public ULIDBasedSSTableUniqueIdentifier fromBytes(@Nonnull ByteBuffer byteBuffer) throws IllegalArgumentException
        {
            return new ULIDBasedSSTableUniqueIdentifier(ULID.fromBytes(ByteBufferUtil.getArray(byteBuffer)));
        }
    }
}
