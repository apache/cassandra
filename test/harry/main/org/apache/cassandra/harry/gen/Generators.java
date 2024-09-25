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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import accord.utils.Invariants;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.TimeUUID;

public class Generators
{
    public static Generator<BitSet> bitSet(int size)
    {
        return rng -> {
            BitSet bitSet = BitSet.allUnset(size);
            for (int i = 0; i < size; i++)
                if (rng.nextBoolean())
                    bitSet.set(i);
            return bitSet;
        };
    }

    public static Generator<String> ascii(int minLength, int maxLength)
    {
        return new StringGenerator(minLength, maxLength, 0, 127);
    }

    public static Generator<String> utf8(int minLength, int maxLength)
    {
        return rng -> {
            int length = rng.nextInt(minLength, maxLength);
            int[] codePoints = new int[length];
            for (int i = 0; i < length; i++)
            {
                int next;
                // Exclude surrogate range, generate values before and after it
                if (rng.nextBoolean())
                    next = rng.nextInt(0x0000, 0xD800);
                else
                    next = rng.nextInt(0xD801, 0xDFFF);
                codePoints[i] = next;
            }

            return new String(codePoints, 0, codePoints.length);
        };
    }

    public static Generator<String> englishAlphabet(int minLength, int maxLength)
    {
        return new StringGenerator(minLength, maxLength, 97, 122);
    }

    public static Generator<Byte> int8()
    {
        return rng -> (byte) rng.nextInt();
    }

    public static Generator<Short> int16()
    {
        return rng -> (short) rng.nextInt();
    }

    public static Generator<Integer> int32()
    {
        return EntropySource::nextInt;
    }

    public static Generator<Integer> int32(int min, int max)
    {
        return rng -> rng.nextInt(min, max);
    }

    public static Generator<Long> int64()
    {
        return new LongGenerator();
    }

    public static Generator<Long> int64(long min, long max)
    {
        return rng -> rng.nextLong(min, max);
    }

    public static Generator<Boolean> bool()
    {
        return EntropySource::nextBoolean;
    }

    public static Generator<Double> doubles()
    {
        return EntropySource::nextDouble;
    }

    public static Generator<Float> floats()
    {
        return EntropySource::nextFloat;
    }

    public static Generator<InetAddress> inetAddr()
    {
        return new InetAddressGenerator();
    }

    public static <T> Generator<T> inetAddr(Generator<T> delegate)
    {
        return new UniqueGenerator<>(delegate, 10);
    }

    public static Generator<ByteBuffer> bytes(int minSize, int maxSize)
    {
        return rng -> {
            int size = rng.nextInt(minSize, maxSize);
            byte[] bytes = new byte[size];
            for (int i = 0; i < size; )
                for (long v = rng.next(),
                     n = Math.min(size - i, Long.SIZE / Byte.SIZE);
                     n-- > 0; v >>= Byte.SIZE)
                    bytes[i++] = (byte) v;
            return ByteBuffer.wrap(bytes);
        };
    }

    public static Generator<UUID> uuidGen()
    {
        return rng -> {
            long msb = rng.next();
            // Adopted from JDK code, UUID#randomUUID
            // randomBytes[6]  &= 0x0f;  /* clear version        */
            msb &= ~(0xFL << 12);
            // randomBytes[6]  |= 0x40;  /* set to version 4     */
            msb |= (0x40L << 8);
            long lsb = rng.next();
            // randomBytes[8]  &= 0x3f;  /* clear variant        */
            lsb &= ~(0x3L << 62);
            // randomBytes[8]  |= 0x80;  /* set to IETF variant  */
            lsb |= (0x2L << 62);
            return new UUID(msb, lsb);
        };
    }

    public static Generator<BigInteger> bigInt()
    {
        return rng -> BigInteger.valueOf(rng.next());
    }

    public static Generator<BigDecimal> bigDecimal()
    {
        return rng -> BigDecimal.valueOf(rng.next());
    }

    public static Generator<TimeUUID> timeuuid()
    {
        return new Generator<TimeUUID>()
        {
            public TimeUUID generate(EntropySource rng)
            {
                return TimeUUID.atUnixMicrosWithLsb(rng.nextLong(0, Long.MAX_VALUE),
                                                    makeClockSeqAndNode(rng));
            }

            private long makeClockSeqAndNode(EntropySource rng)
            {
                long clock = rng.nextInt();

                long lsb = 0;
                lsb |= 0x8000000000000000L;                 // variant (2 bits)
                lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
                lsb |= makeNode(rng);                       // 6 bytes
                return lsb;
            }

            private long makeNode(EntropySource rng)
            {
                // ideally, we'd use the MAC address, but java doesn't expose that.
                long v = rng.next();
                byte[] hash = new byte[] { (byte) (0xff & v),
                                           (byte) (0xff & (v << 8)),
                                           (byte) (0xff & (v << 16)),
                                           (byte) (0xff & (v << 24)),
                                           (byte) (0xff & (v << 32)),
                                           (byte) (0xff & (v << 40))
                };
                long node = 0;
                for (int i = 0; i < 6; i++)
                    node |= (0x00000000000000ff & (long)hash[i]) << (5-i)*8;
                assert (0xff00000000000000L & node) == 0;

                return node | 0x0000010000000000L;
            }
        };
    }

    public static <T> TrackingGenerator<T> tracking(Generator<T> delegate)
    {
        return new TrackingGenerator<>(delegate);
    }

    public static class TrackingGenerator<T> implements Generator<T>
    {
        private final Set<T> generated;
        private final Generator<T> delegate;
        public TrackingGenerator(Generator<T> delegate)
        {
            this.generated = new HashSet<>();
            this.delegate = delegate;
        }

        public Iterable<T> generated()
        {
            return generated;
        }

        @Override
        public T generate(EntropySource rng)
        {
            T next = delegate.generate(rng);
            generated.add(next);
            return next;
        }
    }

    public static <T> Generator<T> unique(Generator<T> delegate)
    {
        return new UniqueGenerator<>(delegate, 100);
    }


    /**
     * WARNING: uses hash code as a proxy for equality
     */
    public static class UniqueGenerator<T> implements Generator<T>
    {
        private final Set<Integer> hashCodes = new HashSet<>();
        private final Generator<T> delegate;
        private final int maxSteps;

        public UniqueGenerator(Generator<T> delegate, int maxSteps)
        {
            this.delegate = delegate;
            this.maxSteps = maxSteps;
        }

        /**
         *
         */
        public void clear()
        {
            hashCodes.clear();
        }

        public T generate(EntropySource rng)
        {
            for (int i = 0; i < maxSteps; i++)
            {
                T v = delegate.generate(rng);
                int hashCode = v.hashCode();
                Invariants.checkState(hashCode != System.identityHashCode(v), "hashCode was not overridden for type %s", v.getClass());
                if (hashCodes.contains(hashCode))
                    continue;
                hashCodes.add(hashCode);
                return v;
            }

            throw new IllegalStateException(String.format("Could not generate a unique value within %d from %s", maxSteps, delegate));
        }
    }

    public static final class StringGenerator implements Generator<String>
    {
        private final int minLength;
        private final int maxLength;
        private final int minChar;
        private final int maxChar;

        public StringGenerator(int minLength, int maxLength, int minChar, int maxChar)
        {
            this.minLength = minLength;
            this.maxLength = maxLength;
            this.minChar = minChar;
            this.maxChar = maxChar;
        }

        @Override
        public String generate(EntropySource rng)
        {
            int length = rng.nextInt(minLength, maxLength);
            int[] codePoints = new int[length];
            for (int i = 0; i < length; i++)
                codePoints[i] = rng.nextInt(minChar, maxChar);
            return new String(codePoints, 0, codePoints.length);
        }
    }

    public static final class LongGenerator implements Generator<Long>
    {
        @Override
        public Long generate(EntropySource rng)
        {
            return rng.next();
        }
    }

    public static class InetAddrAndPortGenerator implements Generator<InetAddressAndPort>
    {
        private final int port;
        public InetAddrAndPortGenerator()
        {
            this(9042);
        }

        public InetAddrAndPortGenerator(int port)
        {
            this.port = port;
        }

        @Override
        public InetAddressAndPort generate(EntropySource rng)
        {
            int orig = rng.nextInt();
            byte[] bytes = new byte[]{ (byte) (orig & 0xff),
                                       (byte) ((orig >> 8) & 0xff),
                                       (byte) ((orig >> 16) & 0xff),
                                       (byte) ((orig >> 24) & 0xff) };
            try
            {
                return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), bytes, port);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class InetAddressGenerator implements Generator<InetAddress>
    {
        @Override
        public InetAddress generate(EntropySource rng)
        {
            int orig = rng.nextInt();
            byte[] bytes = new byte[]{ (byte) (orig & 0xff),
                                       (byte) ((orig >> 8) & 0xff),
                                       (byte) ((orig >> 16) & 0xff),
                                       (byte) ((orig >> 24) & 0xff) };
            try
            {
                return InetAddress.getByAddress(bytes);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> Generator<T> pick(List<T> ts)
    {
        if (ts.isEmpty())
            throw new IllegalStateException("Can't pick from an empty list");
        return (rng) -> ts.get(rng.nextInt(0, ts.size()));
    }

    public static <T> Generator<T> pick(T... ts)
    {
        return pick(Arrays.asList(ts));
    }

    public static <T> Generator<List<T>> list(int minSize, int maxSize, Generator<T> gen)
    {
        return rng -> {
            List<T> objects = new ArrayList<>();
            int size = rng.nextInt(minSize, maxSize);
            for (int i = 0; i < size; i++)
                objects.add(gen.generate(rng));

            return objects;
        };
    }

    public static Generator<Object[]> zipArray(Generator<?>... gens)
    {
        return rng -> {
            Object[] objects = new Object[gens.length];
            for (int i = 0; i < objects.length; i++)
                objects[i] = gens[i].generate(rng);

            return objects;
        };
    }

    public static <T> Generator<List<T>> subsetGenerator(List<T> list)
    {
        return subsetGenerator(list, 0, list.size() - 1);
    }

    public static <T> Generator<List<T>> subsetGenerator(List<T> list, int minSize, int maxSize)
    {
        return (rng) -> {
            int count = rng.nextInt(minSize, maxSize);
            Set<T> set = new HashSet<>();
            for (int i = 0; i < count; i++)
                set.add(list.get(rng.nextInt(minSize, maxSize)));

            return (List<T>) new ArrayList<>(set);
        };
    }

    public static <T extends Enum<T>> Generator<T> enumValues(Class<T> e)
    {
        return pick(Arrays.asList(e.getEnumConstants()));
    }

    public static <T> Generator<List<T>> list(Generator<T> of, int maxSize)
    {
        return list(of, 0, maxSize);
    }

    public static <T> Generator<List<T>> list(Generator<T> of, int minSize, int maxSize)
    {
        return (rng) -> {
            int count = rng.nextInt(minSize, maxSize);
            return of.generate(rng, count);
        };
    }

    public static <T> Generator<T> constant(T constant)
    {
        return (random) -> constant;
    }

    public static <T> Generator<T> constant(Supplier<T> constant)
    {
        return (random) -> constant.get();
    }
}