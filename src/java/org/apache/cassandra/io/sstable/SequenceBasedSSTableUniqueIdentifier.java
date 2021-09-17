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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * Generation identifier based on sequence of integers.
 * This has been the standard implementation in C* since inception.
 */
public class SequenceBasedSSTableUniqueIdentifier implements SSTableUniqueIdentifier
{
    public final int generation;

    public SequenceBasedSSTableUniqueIdentifier(final int generation)
    {
        assert generation >= 0;

        this.generation = generation;
    }

    @Override
    public int compareTo(SSTableUniqueIdentifier o)
    {
        if (o instanceof SequenceBasedSSTableUniqueIdentifier)
            return Integer.compare(this.generation, ((SequenceBasedSSTableUniqueIdentifier) o).generation);

        return -1;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SequenceBasedSSTableUniqueIdentifier that = (SequenceBasedSSTableUniqueIdentifier) o;
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
        ByteBuffer bytes = ByteBuffer.allocate(Integer.BYTES);
        bytes.putInt(0, generation);
        return bytes;
    }

    @Override
    public String asString()
    {
        return String.valueOf(generation);
    }

    @Override
    public String toString()
    {
        return asString();
    }

    public static class Builder implements SSTableUniqueIdentifier.Builder<SequenceBasedSSTableUniqueIdentifier>
    {
        public final static Builder instance = new Builder();

        /**
         * Generates a sequential number to represent an sstables 'generation'. The first generated identifier will be
         * greater by one than the largest generation number found across the provided existing identifiers.
         */
        @Override
        public Supplier<SequenceBasedSSTableUniqueIdentifier> generator(Stream<SSTableUniqueIdentifier> existingIdentifiers)
        {
            int value = existingIdentifiers.filter(SequenceBasedSSTableUniqueIdentifier.class::isInstance)
                                           .map(SequenceBasedSSTableUniqueIdentifier.class::cast)
                                           .mapToInt(id -> id.generation)
                                           .max()
                                           .orElse(0);

            AtomicInteger fileIndexGenerator = new AtomicInteger(value);
            return () -> new SequenceBasedSSTableUniqueIdentifier(fileIndexGenerator.incrementAndGet());
        }

        @Override
        public SequenceBasedSSTableUniqueIdentifier fromString(String token) throws IllegalArgumentException
        {
            return new SequenceBasedSSTableUniqueIdentifier(Integer.parseInt(token));
        }

        @Override
        public SequenceBasedSSTableUniqueIdentifier fromBytes(ByteBuffer bytes) throws IllegalArgumentException
        {
            Preconditions.checkArgument(bytes.remaining() >= Integer.BYTES, "Buffer does not have enough data: %s", bytes.remaining());
            return new SequenceBasedSSTableUniqueIdentifier(bytes.getInt(0));
        }
    }
}
