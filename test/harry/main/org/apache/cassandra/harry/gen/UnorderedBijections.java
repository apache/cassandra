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

package org.apache.cassandra.harry.gen;

import java.nio.ByteBuffer;

import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.gen.rng.PureRng;
import org.apache.cassandra.harry.gen.rng.RngUtils;
import org.apache.cassandra.harry.stress.distribution.Distribution;

/**
 * Bijections where values are NOT sorted by index. Indices are assigned via a pseudo-random
 * permutation (PCG), so the mapping between index and descriptor is invertible but unordered:
 * iterating indices 0, 1, 2, ... produces values in an effectively random order.
 */
public class UnorderedBijections
{
    public static class UnorderedStringBijection extends StringBijection implements HistoryBuilder.IndexedBijection<String>, Generator<String>
    {
        private final long stream;
        private final PureRng rng;
        private final Distribution extraSizeDistribution;
        private final char[] allowedChars;

        public UnorderedStringBijection(long seed)
        {
            this(0xaabbccddeeffL, seed, null, null);
        }

        public UnorderedStringBijection(long stream, long seed)
        {
            this(stream, seed, null, null);
        }

        public UnorderedStringBijection(long stream, long seed, Distribution extraSizeDistribution, char[] allowedChars)
        {
            this.stream = stream;
            this.rng = new PureRng.PCGFast(seed);
            this.extraSizeDistribution = extraSizeDistribution;
            this.allowedChars = allowedChars;
        }

        public UnorderedStringBijection(int nibbleSize, int maxRandomBytes, long stream, long seed,
                                        Distribution extraSizeDistribution, char[] allowedChars)
        {
            super(nibbleSize, maxRandomBytes);
            this.stream = stream;
            this.rng = new PureRng.PCGFast(seed);
            this.extraSizeDistribution = extraSizeDistribution;
            this.allowedChars = allowedChars;
        }

        public UnorderedStringBijection(String[] nibbles, int nibbleSize, int maxRandomBytes, long stream, long seed,
                                        Distribution extraSizeDistribution, char[] allowedChars)
        {
            super(nibbles, nibbleSize, maxRandomBytes);
            this.stream = stream;
            this.rng = new PureRng.PCGFast(seed);
            this.extraSizeDistribution = extraSizeDistribution;
            this.allowedChars = allowedChars;
        }

        @Override
        public long idxFor(long descriptor)
        {
            return rng.sequenceNumber(descriptor, stream);
        }

        @Override
        public long descriptorAt(long idx)
        {
            return rng.randomNumber(idx, stream);
        }

        @Override
        public String generate(EntropySource rng)
        {
            return inflate(rng.next());
        }

        @Override
        protected void appendExtra(StringBuilder builder, long descriptor)
        {
            if (extraSizeDistribution == null || allowedChars == null)
                return;

            // Use a different seed derivation to avoid correlation with the suffix
            long rnd = RngUtils.next(RngUtils.next(RngUtils.next(descriptor)));
            int remaining = Math.toIntExact(extraSizeDistribution.next(rnd));

            while (remaining > 0)
            {
                rnd = RngUtils.next(rnd);
                builder.append(allowedChars[RngUtils.asInt(rnd, 0, allowedChars.length - 1)]);
                remaining--;
            }
        }
    }

    public static class UnorderedBytesBijection extends BytesBijection implements HistoryBuilder.IndexedBijection<ByteBuffer>, Generator<ByteBuffer>
    {
        private final long stream;
        private final PureRng rng;
        private final Distribution extraSizeDistribution;

        public UnorderedBytesBijection(long seed)
        {
            this(0xaabbccddeeffL, seed, null);
        }

        public UnorderedBytesBijection(long stream, long seed)
        {
            this(stream, seed, null);
        }

        public UnorderedBytesBijection(long stream, long seed, Distribution extraSizeDistribution)
        {
            this.stream = stream;
            this.rng = new PureRng.PCGFast(seed);
            this.extraSizeDistribution = extraSizeDistribution;
        }

        public UnorderedBytesBijection(int nibbleSize, int maxRandomBytes, long stream, long seed,
                                       Distribution extraSizeDistribution)
        {
            super(nibbleSize, maxRandomBytes);
            this.stream = stream;
            this.rng = new PureRng.PCGFast(seed);
            this.extraSizeDistribution = extraSizeDistribution;
        }

        public UnorderedBytesBijection(byte[][] nibbles, int nibbleSize, int maxRandomBytes, long stream, long seed,
                                       Distribution extraSizeDistribution)
        {
            super(nibbles, nibbleSize, maxRandomBytes);
            this.stream = stream;
            this.rng = new PureRng.PCGFast(seed);
            this.extraSizeDistribution = extraSizeDistribution;
        }

        @Override
        public long idxFor(long descriptor)
        {
            return rng.sequenceNumber(descriptor, stream);
        }

        @Override
        public long descriptorAt(long idx)
        {
            return rng.randomNumber(idx, stream);
        }

        @Override
        public ByteBuffer generate(EntropySource rng)
        {
            return inflate(rng.next());
        }

        @Override
        protected int extraLength(long descriptor)
        {
            if (extraSizeDistribution == null)
                return 0;

            // Use a different seed derivation to avoid correlation with the suffix
            long rnd = RngUtils.next(RngUtils.next(RngUtils.next(descriptor)));
            return Math.toIntExact(extraSizeDistribution.next(rnd));
        }
    }
}
