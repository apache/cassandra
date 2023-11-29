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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.collect.Range;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_BLOB_SHARED_SEED;

public final class Generators
{
    private static final Logger logger = LoggerFactory.getLogger(Generators.class);

    private static final Constraint INT_CONSTRAINT = Constraint.between(Integer.MIN_VALUE, Integer.MAX_VALUE);
    private static final Constraint LONG_CONSTRAINT = Constraint.between(Long.MIN_VALUE, Long.MAX_VALUE);

    private static final int MAX_BLOB_LENGTH = 1 * 1024 * 1024;

    private static final Constraint DNS_DOMAIN_PARTS_CONSTRAINT = Constraint.between(1, 127);

    private static final char[] LETTER_DOMAIN = createLetterDomain();
    private static final Constraint LETTER_CONSTRAINT = Constraint.between(0, LETTER_DOMAIN.length - 1).withNoShrinkPoint();
    private static final char[] LETTER_OR_DIGIT_DOMAIN = createLetterOrDigitDomain();
    private static final Constraint LETTER_OR_DIGIT_CONSTRAINT = Constraint.between(0, LETTER_OR_DIGIT_DOMAIN.length - 1).withNoShrinkPoint();
    private static final char[] REGEX_WORD_DOMAIN = createRegexWordDomain();
    private static final Constraint REGEX_WORD_CONSTRAINT = Constraint.between(0, REGEX_WORD_DOMAIN.length - 1).withNoShrinkPoint();
    private static final char[] DNS_DOMAIN_PART_DOMAIN = createDNSDomainPartDomain();
    private static final Constraint DNS_DOMAIN_PART_CONSTRAINT = Constraint.between(0, DNS_DOMAIN_PART_DOMAIN.length - 1).withNoShrinkPoint();

    public static final Gen<String> IDENTIFIER_GEN = Generators.regexWord(SourceDSL.integers().between(1, 50));

    public static Gen<Character> letterOrDigit()
    {
        return SourceDSL.integers().between(0, LETTER_OR_DIGIT_DOMAIN.length - 1).map(idx -> LETTER_OR_DIGIT_DOMAIN[idx]);
    }

    public static final Gen<UUID> UUID_RANDOM_GEN = rnd -> {
        long most = rnd.next(Constraint.none());
        most &= 0x0f << 8; /* clear version        */
        most += 0x40 << 8; /* set to version 4     */
        long least = rnd.next(Constraint.none());
        least &= 0x3fl << 56; /* clear variant        */
        least |= 0x80l << 56; /* set to IETF variant  */
        return new UUID(most, least);
    };

    public static final Gen<UUID> UUID_TIME_GEN = rnd -> {
        long most = rnd.next(Constraint.none());
        most &= 0x0f << 8; /* clear version        */
        most += 0x10 << 8; /* set to version 1     */
        long least = rnd.next(Constraint.none());
        least &= 0x3fl << 56; /* clear variant        */
        least |= 0x80l << 56; /* set to IETF variant  */
        return new UUID(most, least);
    };

    public static final Gen<String> DNS_DOMAIN_NAME = rnd -> {
        // how many parts to generate
        int numParts = (int) rnd.next(DNS_DOMAIN_PARTS_CONSTRAINT);
        int MAX_LENGTH = 253;
        int MAX_PART_LENGTH = 63;
        // to make sure the string is within the max allowed length (253), cap each part uniformily
        Constraint partSizeConstraint = Constraint.between(1, Math.min(Math.max(1, (int) Math.ceil((MAX_LENGTH - numParts) / numParts)), MAX_PART_LENGTH));
        StringBuilder sb = new StringBuilder(MAX_LENGTH);
        for (int i = 0; i < numParts; i++)
        {
            int partSize = (int) rnd.next(partSizeConstraint);
            // -_ not allowed in the first or last position of a part, so special case these
            // also, only use letters as first char doesn't allow digits uniformailly
            sb.append(LETTER_DOMAIN[(int) rnd.next(LETTER_CONSTRAINT)]);
            for (int j = 1; j < partSize; j++)
                sb.append(DNS_DOMAIN_PART_DOMAIN[(int) rnd.next(DNS_DOMAIN_PART_CONSTRAINT)]);
            if (isDash(sb.charAt(sb.length() - 1)))
            {
                // need to replace
                sb.setCharAt(sb.length() - 1, LETTER_OR_DIGIT_DOMAIN[(int) rnd.next(LETTER_OR_DIGIT_CONSTRAINT)]);
            }
            sb.append('.'); // domain allows . at the end (part of spec) so don't need to worry about removing
        }
        return sb.toString();
    };

    private static final class Ipv4AddressGen implements Gen<byte[]>
    {
        public byte[] generate(RandomnessSource rnd)
        {
            byte[] bytes = new byte[4];
            ByteArrayUtil.putInt(bytes, 0, (int) rnd.next(INT_CONSTRAINT));
            return bytes;
        }
    }
    public static final Gen<byte[]> IPV4_ADDRESS = new Ipv4AddressGen();
    private static final class Ipv6AddressGen implements Gen<byte[]>
    {
        public byte[] generate(RandomnessSource rnd)
        {
            byte[] bytes = new byte[16];
            ByteArrayUtil.putLong(bytes, 0, rnd.next(LONG_CONSTRAINT));
            ByteArrayUtil.putLong(bytes, 8, rnd.next(LONG_CONSTRAINT));
            return bytes;
        }
    }
    public static final Gen<byte[]> IPV6_ADDRESS = new Ipv6AddressGen();

    public static final Gen<InetAddress> INET_4_ADDRESS_RESOLVED_GEN = rnd -> {
        try
        {
            return InetAddress.getByAddress(DNS_DOMAIN_NAME.generate(rnd), IPV4_ADDRESS.generate(rnd));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    };

    public static final Gen<InetAddress> INET_4_ADDRESS_UNRESOLVED_GEN = rnd -> {
        try
        {
            return InetAddress.getByAddress(null, IPV4_ADDRESS.generate(rnd));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    };
    public static final Gen<InetAddress> INET_4_ADDRESS_GEN = INET_4_ADDRESS_RESOLVED_GEN.mix(INET_4_ADDRESS_UNRESOLVED_GEN);

    public static final Gen<InetAddress> INET_6_ADDRESS_RESOLVED_GEN = rnd -> {
        try
        {
            return InetAddress.getByAddress(DNS_DOMAIN_NAME.generate(rnd), IPV6_ADDRESS.generate(rnd));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    };

    public static final Gen<InetAddress> INET_6_ADDRESS_UNRESOLVED_GEN = rnd -> {
        try
        {
            return InetAddress.getByAddress(null, IPV6_ADDRESS.generate(rnd));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    };
    public static final Gen<InetAddress> INET_6_ADDRESS_GEN = INET_6_ADDRESS_RESOLVED_GEN.mix(INET_6_ADDRESS_UNRESOLVED_GEN);
    public static final Gen<InetAddress> INET_ADDRESS_GEN = INET_4_ADDRESS_GEN.mix(INET_6_ADDRESS_GEN);
    public static final Gen<InetAddress> INET_ADDRESS_UNRESOLVED_GEN = INET_4_ADDRESS_UNRESOLVED_GEN.mix(INET_6_ADDRESS_UNRESOLVED_GEN);

    /**
     * Implements a valid utf-8 generator.
     *
     * Implementation note, currently relies on getBytes to strip out non-valid utf-8 chars, so is slow
     */
    public static final Gen<String> UTF_8_GEN = utf8(0, 1024);

    // time generators
    // all time is boxed in the future around 50 years from today: Aug 20th, 2020 UTC
    public static final Gen<Timestamp> TIMESTAMP_GEN;
    public static final Gen<Date> DATE_GEN;
    public static final Gen<Long> TIMESTAMP_NANOS;
    public static final Gen<Long> SMALL_TIME_SPAN_NANOS; // generate nanos in [0, 10] seconds
    public static final Gen<Long> TINY_TIME_SPAN_NANOS; // generate nanos in [0, 1) seconds

    static
    {
        long secondInNanos = 1_000_000_000L;
        ZonedDateTime now = ZonedDateTime.of(2020, 8, 20,
                                             0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime startOfTime = now.minusYears(50);
        ZonedDateTime endOfDays = now.plusYears(50);
        Constraint millisConstraint = Constraint.between(startOfTime.toInstant().toEpochMilli(), endOfDays.toInstant().toEpochMilli());
        Constraint nanosInSecondConstraint = Constraint.between(0, secondInNanos - 1);
        // Represents the timespan based on the most of the default request timeouts. See DatabaseDescriptor
        Constraint smallTimeSpanNanosConstraint = Constraint.between(0, 10 * secondInNanos);
        TIMESTAMP_GEN = rnd -> {
            Timestamp ts = new Timestamp(rnd.next(millisConstraint));
            ts.setNanos((int) rnd.next(nanosInSecondConstraint));
            return ts;
        };
        DATE_GEN = TIMESTAMP_GEN.map(t -> new Date(t.getTime()));
        TIMESTAMP_NANOS = TIMESTAMP_GEN.map(t -> TimeUnit.MILLISECONDS.toNanos(t.getTime()) + t.getNanos());
        SMALL_TIME_SPAN_NANOS = rnd -> rnd.next(smallTimeSpanNanosConstraint);
        TINY_TIME_SPAN_NANOS = rnd -> rnd.next(nanosInSecondConstraint);
    }

    private Generators()
    {

    }

    /**
     * Generates values which match the {@link Predicate}.  The main difference with {@link Gen#assuming(Predicate)}
     * is that this does not stop if not enough matches are found.
     */
    public static <T> Gen<T> filter(Gen<T> gen, Predicate<T> fn) {
        return new FilterGen(gen, fn);
    }

    /**
     * Generates values which match the {@link Predicate}.  The main difference with {@link Gen#assuming(Predicate)}
     * is that failing is controlled at the generator level.
     */
    public static <T> Gen<T> filter(Gen<T> gen, int maxAttempts, Predicate<T> fn) {
        return new BoundedFilterGen<>(gen, maxAttempts, fn);
    }

    public static Gen<String> regexWord(Gen<Integer> sizes)
    {
        return string(sizes, REGEX_WORD_DOMAIN);
    }

    public static Gen<String> string(Gen<Integer> sizes, char[] domain)
    {
        // note, map is overloaded so String::new is ambugious to javac, so need a lambda here
        return charArray(sizes, domain).map(c -> new String(c));
    }

    public static Gen<char[]> charArray(Gen<Integer> sizes, char[] domain)
    {
        Constraint constraints = Constraint.between(0, domain.length - 1).withNoShrinkPoint();
        Gen<char[]> gen = td -> {
            int size = sizes.generate(td);
            char[] is = new char[size];
            for (int i = 0; i != size; i++)
            {
                int idx = (int) td.next(constraints);
                is[i] = domain[idx];
            }
            return is;
        };
        gen.describedAs(String::new);
        return gen;
    }

    private static char[] createLetterDomain()
    {
        // [a-zA-Z]
        char[] domain = new char[26 * 2];

        int offset = 0;
        // A-Z
        for (int c = 65; c < 91; c++)
            domain[offset++] = (char) c;
        // a-z
        for (int c = 97; c < 123; c++)
            domain[offset++] = (char) c;
        return domain;
    }

    private static char[] createLetterOrDigitDomain()
    {
        // [a-zA-Z0-9]
        char[] domain = new char[26 * 2 + 10];

        int offset = 0;
        // 0-9
        for (int c = 48; c < 58; c++)
            domain[offset++] = (char) c;
        // A-Z
        for (int c = 65; c < 91; c++)
            domain[offset++] = (char) c;
        // a-z
        for (int c = 97; c < 123; c++)
            domain[offset++] = (char) c;
        return domain;
    }

    private static char[] createRegexWordDomain()
    {
        // \w == [a-zA-Z_0-9] the only difference with letterOrDigit is the addition of _
        return ArrayUtils.add(createLetterOrDigitDomain(), (char) 95); // 95 is _
    }

    private static char[] createDNSDomainPartDomain()
    {
        // [a-zA-Z0-9_-] the only difference with regex word is the addition of -
        return ArrayUtils.add(createRegexWordDomain(), (char) 45); // 45 is -
    }

    public static Gen<ByteBuffer> bytes(int min, int max)
    {
        return bytes(min, max, SourceDSL.arbitrary().constant(BBCases.HEAP));
    }

    public static Gen<ByteBuffer> bytesAnyType(int min, int max)
    {
        return bytes(min, max, SourceDSL.arbitrary().enumValues(BBCases.class));
    }

    private static Gen<ByteBuffer> bytes(int min, int max, Gen<BBCases> cases)
    {
        if (min < 0)
            throw new IllegalArgumentException("Asked for negative bytes; given " + min);
        if (max > MAX_BLOB_LENGTH)
            throw new IllegalArgumentException("Requested bytes larger than shared bytes allowed; " +
                                               "asked for " + max + " but only have " + MAX_BLOB_LENGTH);
        if (max < min)
            throw new IllegalArgumentException("Max was less than min; given min=" + min + " and max=" + max);
        Constraint sizeConstraint = Constraint.between(min, max);
        return rnd -> {
            // since Constraint is immutable and the max was checked, its already proven to be int
            int size = (int) rnd.next(sizeConstraint);
            // to add more randomness, also shift offset in the array so the same size doesn't yield the same bytes
            int offset = (int) rnd.next(Constraint.between(0, MAX_BLOB_LENGTH - size));

            return handleCases(cases, rnd, offset, size);
        };
    };

    private enum BBCases { HEAP, READ_ONLY_HEAP, DIRECT, READ_ONLY_DIRECT }

    private static ByteBuffer handleCases(Gen<BBCases> cases, RandomnessSource rnd, int offset, int size) {
        switch (cases.generate(rnd))
        {
            case HEAP: return ByteBuffer.wrap(LazySharedBlob.SHARED_BYTES, offset, size);
            case READ_ONLY_HEAP: return ByteBuffer.wrap(LazySharedBlob.SHARED_BYTES, offset, size).asReadOnlyBuffer();
            case DIRECT: return directBufferFromSharedBlob(offset, size);
            case READ_ONLY_DIRECT: return directBufferFromSharedBlob(offset, size).asReadOnlyBuffer();
            default: throw new AssertionError("can't wait for jdk 17!");
        }
    }

    private static ByteBuffer directBufferFromSharedBlob(int offset, int size) {
        ByteBuffer bb = ByteBuffer.allocateDirect(size);
        bb.put(LazySharedBlob.SHARED_BYTES, offset, size);
        bb.flip();
        return bb;
    }

     /**
     * Implements a valid utf-8 generator.
     *
     * Implementation note, currently relies on getBytes to strip out non-valid utf-8 chars, so is slow
     */
    public static Gen<String> utf8(int min, int max)
    {
        return SourceDSL.strings()
                 .basicMultilingualPlaneAlphabet()
                 .ofLengthBetween(min, max)
                 .map(s -> new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
    }

    public static Gen<BigInteger> bigInt()
    {
        return bigInt(SourceDSL.integers().between(1, 32));
    }

    public static Gen<BigInteger> bigInt(Gen<Integer> numBitsGen)
    {
        Gen<Integer> signumGen = SourceDSL.arbitrary().pick(-1, 0, 1);
        return rnd -> {
            int signum = signumGen.generate(rnd);
            if (signum == 0)
                return BigInteger.ZERO;
            int numBits = numBitsGen.generate(rnd);
            if (numBits < 0)
                throw new IllegalArgumentException("numBits must be non-negative");
            int numBytes = (int)(((long)numBits+7)/8); // avoid overflow

            // Generate random bytes and mask out any excess bits
            byte[] randomBits = new byte[0];
            if (numBytes > 0) {
                randomBits = bytes(numBytes, numBytes).map(bb -> ByteBufferUtil.getArray(bb)).generate(rnd);
                int excessBits = 8*numBytes - numBits;
                randomBits[0] &= (1 << (8-excessBits)) - 1;
            }
            return new BigInteger(signum, randomBits);
        };
    }

    public static Gen<BigDecimal> bigDecimal()
    {
        return bigDecimal(SourceDSL.integers().between(1, 100), bigInt());
    }

    public static Gen<BigDecimal> bigDecimal(Gen<Integer> scaleGen, Gen<BigInteger> bigIntegerGen)
    {
        return rnd -> {
            int scale = scaleGen.generate(rnd);
            BigInteger bigInt = bigIntegerGen.generate(rnd);
            return new BigDecimal(bigInt, scale);
        };
    }

    public static <T> Gen<T> unique(Gen<T> gen)
    {
        Set<T> dedup = new HashSet<>();
        return filter(gen, dedup::add);
    }

    public static <T> Gen<T> cached(Gen<T> gen)
    {
        Object cacheMissed = new Object();
        return new Gen<T>()
        {
            private Object value = cacheMissed;
            @Override
            public T generate(RandomnessSource randomnessSource)
            {
                if (value == cacheMissed)
                    value = gen.generate(randomnessSource);
                return (T) value;
            }
        };
    }

    private static boolean isDash(char c)
    {
        switch (c)
        {
            case 45: // -
            case 95: // _
                return true;
            default:
                return false;
        }
    }

    private static final class LazySharedBlob
    {
        private static final byte[] SHARED_BYTES;

        static
        {
            long blobSeed = TEST_BLOB_SHARED_SEED.getLong(System.currentTimeMillis());
            logger.info("Shared blob Gen used seed {}", blobSeed);

            Random random = new Random(blobSeed);
            byte[] bytes = new byte[MAX_BLOB_LENGTH];
            random.nextBytes(bytes);

            SHARED_BYTES = bytes;
        }
    }

    private static final class FilterGen<T> implements Gen<T>
    {
        private final Gen<T> gen;
        private final Predicate<T> fn;

        private FilterGen(Gen<T> gen, Predicate<T> fn)
        {
            this.gen = gen;
            this.fn = fn;
        }

        public T generate(RandomnessSource rs)
        {
            while (true)
            {
                T value = gen.generate(rs);
                if (fn.test(value))
                {
                    return value;
                }
            }
        }
    }

    private static final class BoundedFilterGen<T> implements Gen<T>
    {
        private final Gen<T> gen;
        private final int maxAttempts;
        private final Predicate<T> fn;

        private BoundedFilterGen(Gen<T> gen, int maxAttempts, Predicate<T> fn)
        {
            this.gen = gen;
            this.maxAttempts = maxAttempts;
            this.fn = fn;
        }

        public T generate(RandomnessSource rs)
        {
            for (int i = 0; i < maxAttempts; i++)
            {
                T value = gen.generate(rs);
                if (fn.test(value))
                {
                    return value;
                }
            }
            throw new IllegalStateException("Gave up trying to find values matching assumptions after " + maxAttempts + " attempts");
        }
    }

    public static Gen<Range<Integer>> forwardRanges(int min, int max)
    {
        return SourceDSL.integers().between(min, max)
                        .flatMap(start -> SourceDSL.integers().between(start, max)
                                                   .map(end -> Range.closed(start, end)));
    }
}
