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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * Generation identifier based on sequence of integers.
 * This has been the standard implementation in C* since inception.
 */
public class SequenceBasedSSTableId implements SSTableId, Comparable<SequenceBasedSSTableId>
{
    public final long generation;

    public SequenceBasedSSTableId(final long generation)
    {
        assert generation >= 0;

        this.generation = generation;
    }

    @Override
    public int compareTo(SequenceBasedSSTableId o)
    {
        if (o == null)
            return 1;
        else if (o == this)
            return 0;

        return Long.compare(this.generation, o.generation);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SequenceBasedSSTableId that = (SequenceBasedSSTableId) o;
        return generation == that.generation;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(generation);
    }

    @Override
    public ByteBuffer asBytes()
    {
        ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES);
        bytes.putLong(0, generation);
        return bytes;
    }

    @Override
    public String toString()
    {
        return String.valueOf(generation);
    }

    public static class Builder implements SSTableId.Builder<SequenceBasedSSTableId>
    {
        public final static Builder instance = new Builder();

        private final static Pattern PATTERN = Pattern.compile("\\d+");

        /**
         * Generates a sequential number to represent an sstables identifier. The first generated identifier will be
         * greater by one than the largest generation number found across the provided existing identifiers.
         */
        @Override
        public Supplier<SequenceBasedSSTableId> generator(Stream<SSTableId> existingIdentifiers)
        {
            long value = existingIdentifiers.filter(SequenceBasedSSTableId.class::isInstance)
                                            .map(SequenceBasedSSTableId.class::cast)
                                            .mapToLong(id -> id.generation)
                                            .max()
                                            .orElse(0);

            AtomicLong fileIndexGenerator = new AtomicLong(value);
            return () -> new SequenceBasedSSTableId(fileIndexGenerator.incrementAndGet());
        }

        @Override
        public boolean isUniqueIdentifier(String str)
        {
            return str != null && !str.isEmpty() && str.length() <= 10 && PATTERN.matcher(str).matches();
        }

        @Override
        public boolean isUniqueIdentifier(ByteBuffer bytes)
        {
            return bytes != null && bytes.remaining() == Long.BYTES && bytes.getLong(0) >= 0;
        }

        @Override
        public SequenceBasedSSTableId fromString(String token) throws IllegalArgumentException
        {
            return new SequenceBasedSSTableId(Long.parseLong(token));
        }

        @Override
        public SequenceBasedSSTableId fromBytes(ByteBuffer bytes) throws IllegalArgumentException
        {
            Preconditions.checkArgument(bytes.remaining() == Long.BYTES, "Buffer does not have a valid number of bytes remaining. Expecting: %s but was: %s", Long.BYTES, bytes.remaining());
            return new SequenceBasedSSTableId(bytes.getLong(0));
        }
    }
}
