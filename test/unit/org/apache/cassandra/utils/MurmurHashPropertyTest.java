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
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link MurmurHash}.
 * <p>
 * Tests verify byte[]/ByteBuffer equivalence, inverse round-trip
 * correctness, and regression values for the various MurmurHash functions.
 */
public class MurmurHashPropertyTest
{
    /** Generator for random byte arrays of length 0..256. */
    private static final Gen<byte[]> BYTE_ARRAY_GEN = AccordGenerators.byteArray(Gens.ints().between(0, 256));

    /** Generator for random byte arrays whose length is exactly 16 (for hash3 inverse). */
    private static final Gen<byte[]> ALIGNED_16_BYTE_GEN = AccordGenerators.byteArrayOfSize(16);

    // ---- ByteBuffer / byte[] equivalence ----

    @Test
    public void hash2_64ByteBufferEquivalence()
    {
        qt().forAll(BYTE_ARRAY_GEN, Gens.longs().all()).check((data, seed) -> {
            long fromArray = MurmurHash.hash2_64(data, 0, data.length, seed);
            long fromHeap = MurmurHash.hash2_64(ByteBuffer.wrap(data), 0, data.length, seed);
            assertThat(fromHeap)
                .describedAs("hash2_64 heap ByteBuffer and byte[] must produce the same result")
                .isEqualTo(fromArray);

            // Also test with a direct ByteBuffer
            ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
            direct.put(data);
            direct.flip();
            long fromDirect = MurmurHash.hash2_64(direct, 0, data.length, seed);
            assertThat(fromDirect)
                .describedAs("hash2_64 direct ByteBuffer and byte[] must produce the same result")
                .isEqualTo(fromArray);
        });
    }

    @Test
    public void hash3_x64_128ByteBufferEquivalence()
    {
        qt().forAll(BYTE_ARRAY_GEN, Gens.longs().all()).check((data, seed) -> {
            long[] resultArray = new long[2];
            long[] resultHeap = new long[2];
            long[] resultDirect = new long[2];
            MurmurHash.hash3_x64_128(data, 0, data.length, seed, resultArray);
            MurmurHash.hash3_x64_128(ByteBuffer.wrap(data), 0, data.length, seed, resultHeap);
            assertThat(resultHeap)
                .describedAs("hash3_x64_128 heap ByteBuffer and byte[] must produce the same result")
                .isEqualTo(resultArray);

            ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
            direct.put(data);
            direct.flip();
            MurmurHash.hash3_x64_128(direct, 0, data.length, seed, resultDirect);
            assertThat(resultDirect)
                .describedAs("hash3_x64_128 direct ByteBuffer and byte[] must produce the same result")
                .isEqualTo(resultArray);
        });
    }

    // ---- ByteBuffer with non-zero offset equivalence ----

    @Test
    public void hash32OffsetEquivalence()
    {
        qt().check(rs -> {
            int padding = rs.nextInt(1, 33);
            byte[] data = BYTE_ARRAY_GEN.next(rs);
            int seed = rs.nextInt();

            // Create a ByteBuffer with leading padding
            byte[] padded = new byte[padding + data.length];
            System.arraycopy(data, 0, padded, padding, data.length);
            ByteBuffer paddedBuf = ByteBuffer.wrap(padded);

            int fromDirect = MurmurHash.hash32(ByteBuffer.wrap(data), 0, data.length, seed);
            int fromOffset = MurmurHash.hash32(paddedBuf, padding, data.length, seed);
            assertThat(fromOffset)
                .describedAs("hash32 with offset must match direct result")
                .isEqualTo(fromDirect);
        });
    }

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

    // ---- invTailReverse ----
    // Note: MurmurHash.invTailReverse is a public method with no test coverage.
    // Testing it in isolation is non-trivial because:
    // 1. It has specific byte-order conventions (Long.reverseBytes at entry, Longs big-endian
    //    at exit) that are only meaningful in the context of the full hash3 inverse pipeline.
    // 2. It has a latent bug: BitSet.toByteArray() can return a shorter array when high bits
    //    are zero, causing ArrayIndexOutOfBoundsException for certain inputs.
    // A proper test would require implementing the full non-16-byte hash3 inverse pipeline
    // (which doesn't exist in the codebase). The 16-byte case is covered by
    // hash3InverseRoundTripSeedZero, which uses inv_hash3_x64_128 (no tail processing needed).

    // ---- Known test vectors (golden values) ----

    @Test
    public void knownTestVectors()
    {
        byte[] empty = new byte[0];
        byte[] singleZero = new byte[]{ 0x00 };
        byte[] hello = "Hello".getBytes(StandardCharsets.US_ASCII);

        // Regression tests: these values were generated by this implementation and pinned.
        // They verify the hash function has not changed, not that it matches an external spec.
        // hash32
        assertThat(MurmurHash.hash32(ByteBuffer.wrap(empty), 0, 0, 0))
            .describedAs("hash32(empty, seed=0)")
            .isEqualTo(0);
        assertThat(MurmurHash.hash32(ByteBuffer.wrap(singleZero), 0, 1, 0))
            .describedAs("hash32({0x00}, seed=0)")
            .isEqualTo(-380735811);
        assertThat(MurmurHash.hash32(ByteBuffer.wrap(hello), 0, hello.length, 0))
            .describedAs("hash32(\"Hello\", seed=0)")
            .isEqualTo(1826530862);
        assertThat(MurmurHash.hash32(ByteBuffer.wrap(hello), 0, hello.length, 42))
            .describedAs("hash32(\"Hello\", seed=42)")
            .isEqualTo(120081362);

        // hash2_64
        assertThat(MurmurHash.hash2_64(empty, 0, 0, 0L))
            .describedAs("hash2_64(empty, seed=0)")
            .isEqualTo(0L);
        assertThat(MurmurHash.hash2_64(singleZero, 0, 1, 0L))
            .describedAs("hash2_64({0x00}, seed=0)")
            .isEqualTo(6351753276682545529L);
        assertThat(MurmurHash.hash2_64(hello, 0, hello.length, 0L))
            .describedAs("hash2_64(\"Hello\", seed=0)")
            .isEqualTo(6940510666185404851L);
        assertThat(MurmurHash.hash2_64(hello, 0, hello.length, 42L))
            .describedAs("hash2_64(\"Hello\", seed=42)")
            .isEqualTo(-5190831759633619065L);

        // hash3_x64_128
        long[] result = new long[2];

        MurmurHash.hash3_x64_128(empty, 0, 0, 0L, result);
        assertThat(result).describedAs("hash3_x64_128(empty, seed=0)")
                          .isEqualTo(new long[]{ 0L, 0L });

        MurmurHash.hash3_x64_128(singleZero, 0, 1, 0L, result);
        assertThat(result).describedAs("hash3_x64_128({0x00}, seed=0)")
                          .isEqualTo(new long[]{ 5048724184180415669L, 5864299874987029891L });

        MurmurHash.hash3_x64_128(hello, 0, hello.length, 0L, result);
        assertThat(result).describedAs("hash3_x64_128(\"Hello\", seed=0)")
                          .isEqualTo(new long[]{ 3871253994707141660L, -6917270852172884668L });

        MurmurHash.hash3_x64_128(hello, 0, hello.length, 42L, result);
        assertThat(result).describedAs("hash3_x64_128(\"Hello\", seed=42)")
                          .isEqualTo(new long[]{ 2550721319707356219L, -6862742243595569438L });
    }

    // ---- Empty input handling ----

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
