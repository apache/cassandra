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

import java.nio.ByteBuffer;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link MurmurHash}.
 * <p>
 * Tests verify determinism, byte[]/ByteBuffer equivalence, and inverse round-trip
 * correctness for the various MurmurHash functions.
 */
public class MurmurHashPropertyTest
{
    /** Generator for random byte arrays of length 0..256. */
    private static final Gen<byte[]> BYTE_ARRAY_GEN = AccordGenerators.byteArray(Gens.ints().between(0, 256));

    /** Generator for random byte arrays whose length is exactly 16 (for hash3 inverse). */
    private static final Gen<byte[]> ALIGNED_16_BYTE_GEN = AccordGenerators.byteArrayOfSize(16);

    // ---- Determinism ----

    @Test
    public void hash32Determinism()
    {
        qt().forAll(BYTE_ARRAY_GEN, Gens.ints().all()).check((data, seed) -> {
            ByteBuffer buf = ByteBuffer.wrap(data);
            int h1 = MurmurHash.hash32(buf, 0, data.length, seed);
            int h2 = MurmurHash.hash32(buf, 0, data.length, seed);
            assertThat(h1).describedAs("hash32 must be deterministic").isEqualTo(h2);
        });
    }

    @Test
    public void hash2_64Determinism()
    {
        qt().forAll(BYTE_ARRAY_GEN, Gens.longs().all()).check((data, seed) -> {
            long h1 = MurmurHash.hash2_64(data, 0, data.length, seed);
            long h2 = MurmurHash.hash2_64(data, 0, data.length, seed);
            assertThat(h1).describedAs("hash2_64(byte[]) must be deterministic").isEqualTo(h2);
        });
    }

    @Test
    public void hash3_x64_128Determinism()
    {
        qt().forAll(BYTE_ARRAY_GEN, Gens.longs().all()).check((data, seed) -> {
            long[] result1 = new long[2];
            long[] result2 = new long[2];
            MurmurHash.hash3_x64_128(data, 0, data.length, seed, result1);
            MurmurHash.hash3_x64_128(data, 0, data.length, seed, result2);
            assertThat(result1).describedAs("hash3_x64_128(byte[]) must be deterministic").isEqualTo(result2);
        });
    }

    // ---- ByteBuffer / byte[] equivalence ----

    @Test
    public void hash2_64ByteBufferEquivalence()
    {
        qt().forAll(BYTE_ARRAY_GEN, Gens.longs().all()).check((data, seed) -> {
            long fromArray = MurmurHash.hash2_64(data, 0, data.length, seed);
            long fromBuffer = MurmurHash.hash2_64(ByteBuffer.wrap(data), 0, data.length, seed);
            assertThat(fromBuffer)
                .describedAs("hash2_64 ByteBuffer and byte[] must produce the same result")
                .isEqualTo(fromArray);
        });
    }

    @Test
    public void hash3_x64_128ByteBufferEquivalence()
    {
        qt().forAll(BYTE_ARRAY_GEN, Gens.longs().all()).check((data, seed) -> {
            long[] resultArray = new long[2];
            long[] resultBuffer = new long[2];
            MurmurHash.hash3_x64_128(data, 0, data.length, seed, resultArray);
            MurmurHash.hash3_x64_128(ByteBuffer.wrap(data), 0, data.length, seed, resultBuffer);
            assertThat(resultBuffer)
                .describedAs("hash3_x64_128 ByteBuffer and byte[] must produce the same result")
                .isEqualTo(resultArray);
        });
    }

    // ---- ByteBuffer with non-zero offset equivalence ----

    @Test
    public void hash2_64OffsetEquivalence()
    {
        qt().check(rs -> {
            int padding = rs.nextInt(1, 33);
            byte[] data = BYTE_ARRAY_GEN.next(rs);
            long seed = rs.nextLong();

            // Create a ByteBuffer with leading padding
            byte[] padded = new byte[padding + data.length];
            System.arraycopy(data, 0, padded, padding, data.length);
            ByteBuffer buf = ByteBuffer.wrap(padded);

            long fromArray = MurmurHash.hash2_64(data, 0, data.length, seed);
            long fromBuffer = MurmurHash.hash2_64(buf, padding, data.length, seed);
            assertThat(fromBuffer)
                .describedAs("hash2_64 with offset must match byte[] result")
                .isEqualTo(fromArray);
        });
    }

    @Test
    public void hash3_x64_128OffsetEquivalence()
    {
        qt().check(rs -> {
            int padding = rs.nextInt(1, 33);
            byte[] data = BYTE_ARRAY_GEN.next(rs);
            long seed = rs.nextLong();

            byte[] padded = new byte[padding + data.length];
            System.arraycopy(data, 0, padded, padding, data.length);
            ByteBuffer buf = ByteBuffer.wrap(padded);

            long[] resultArray = new long[2];
            long[] resultBuffer = new long[2];
            MurmurHash.hash3_x64_128(data, 0, data.length, seed, resultArray);
            MurmurHash.hash3_x64_128(buf, padding, data.length, seed, resultBuffer);
            assertThat(resultBuffer)
                .describedAs("hash3_x64_128 with offset must match byte[] result")
                .isEqualTo(resultArray);
        });
    }

    // ---- Inverse round-trips ----

    @Test
    public void fmixInverseRoundTrip()
    {
        qt().forAll(Gens.longs().all()).check(k -> {
            long hashed = MurmurHash.fmix(k);
            long recovered = MurmurHash.invFmix(hashed);
            assertThat(recovered)
                .describedAs("invFmix(fmix(k)) must equal k")
                .isEqualTo(k);
        });
    }

    @Test
    public void invFmixForwardRoundTrip()
    {
        qt().forAll(Gens.longs().all()).check(k -> {
            long unhashed = MurmurHash.invFmix(k);
            long recovered = MurmurHash.fmix(unhashed);
            assertThat(recovered)
                .describedAs("fmix(invFmix(k)) must equal k")
                .isEqualTo(k);
        });
    }

    @Test
    public void hash3InverseRoundTripSeedZero()
    {
        qt().forAll(ALIGNED_16_BYTE_GEN).check(input -> {
            long[] hashResult = new long[2];
            MurmurHash.hash3_x64_128(input, 0, 16, 0, hashResult);
            long[] recovered = MurmurHash.inv_hash3_x64_128(hashResult);

            // inv_hash3_x64_128 applies Long.reverseBytes, so the returned longs
            // are in big-endian byte order. Extract bytes accordingly.
            byte[] reconstructed = new byte[16];
            for (int i = 0; i < 8; i++)
            {
                reconstructed[i] = (byte) (recovered[0] >>> ((7 - i) * 8));
                reconstructed[i + 8] = (byte) (recovered[1] >>> ((7 - i) * 8));
            }
            assertThat(reconstructed)
                .describedAs("inv_hash3_x64_128 must recover the original 16-byte input with seed=0")
                .isEqualTo(input);
        });
    }

    // ---- invRShiftXor correctness ----

    @Test
    public void invRShiftXorCorrectness()
    {
        qt().forAll(Gens.longs().all(), Gens.ints().between(1, 63)).check((value, shift) -> {
            long shifted = value ^ (value >>> shift);
            long recovered = MurmurHash.invRShiftXor(shifted, shift);
            assertThat(recovered)
                .describedAs("invRShiftXor must recover the original value for shift=%d", shift)
                .isEqualTo(value);
        });
    }

    // ---- Empty input handling ----

    @Test
    public void emptyInputHash32()
    {
        qt().forAll(Gens.ints().all()).check(seed -> {
            ByteBuffer buf = ByteBuffer.allocate(0);
            int h1 = MurmurHash.hash32(buf, 0, 0, seed);
            int h2 = MurmurHash.hash32(buf, 0, 0, seed);
            assertThat(h1).describedAs("Empty input hash32 must be deterministic").isEqualTo(h2);
        });
    }

    @Test
    public void emptyInputHash2_64()
    {
        qt().forAll(Gens.longs().all()).check(seed -> {
            long h1 = MurmurHash.hash2_64(new byte[0], 0, 0, seed);
            long h2 = MurmurHash.hash2_64(ByteBuffer.allocate(0), 0, 0, seed);
            assertThat(h1)
                .describedAs("Empty input hash2_64 must be equivalent between byte[] and ByteBuffer")
                .isEqualTo(h2);
        });
    }

    @Test
    public void emptyInputHash3_x64_128()
    {
        qt().forAll(Gens.longs().all()).check(seed -> {
            long[] resultArray = new long[2];
            long[] resultBuffer = new long[2];
            MurmurHash.hash3_x64_128(new byte[0], 0, 0, seed, resultArray);
            MurmurHash.hash3_x64_128(ByteBuffer.allocate(0), 0, 0, seed, resultBuffer);
            assertThat(resultBuffer)
                .describedAs("Empty input hash3_x64_128 must be equivalent between byte[] and ByteBuffer")
                .isEqualTo(resultArray);
        });
    }
}
