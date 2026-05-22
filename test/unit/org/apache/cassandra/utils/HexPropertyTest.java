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

import java.util.Arrays;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property-based tests for {@link Hex}.
 */
public class HexPropertyTest
{
    /** Generate a random byte array of length 0..128. */
    private static final Gen<byte[]> BYTE_ARRAY_GEN = AccordGenerators.byteArray(Gens.ints().between(0, 128));

    /**
     * Generate a valid mixed-case hex string by converting random bytes to hex
     * and then randomly uppercasing some of the hex letter characters (a-f).
     * This ensures tests exercise both lowercase and uppercase hex input.
     */
    private static final Gen<String> HEX_STRING_GEN = rs -> {
        byte[] bytes = BYTE_ARRAY_GEN.next(rs);
        String lowercase = Hex.bytesToHex(bytes);
        char[] chars = lowercase.toCharArray();
        for (int i = 0; i < chars.length; i++)
        {
            if (chars[i] >= 'a' && chars[i] <= 'f' && rs.nextBoolean())
                chars[i] = (char) (chars[i] - 'a' + 'A');
        }
        return new String(chars);
    };

    /**
     * Round-trip: hexToBytes(bytesToHex(bytes)) should recover the original bytes.
     */
    @Test
    public void roundTripBytesToHexToBytes()
    {
        qt().forAll(BYTE_ARRAY_GEN).check(bytes -> {
            String hex = Hex.bytesToHex(bytes);
            byte[] recovered = Hex.hexToBytes(hex);
            assertThat(recovered).isEqualTo(bytes);
        });
    }

    /**
     * Inverse round-trip: bytesToHex(hexToBytes(s)) should equal s.toLowerCase()
     * for any valid hex string.
     */
    @Test
    public void roundTripHexToBytesToHex()
    {
        qt().forAll(HEX_STRING_GEN).check(hex -> {
            byte[] bytes = Hex.hexToBytes(hex);
            String recovered = Hex.bytesToHex(bytes);
            assertThat(recovered).isEqualTo(hex.toLowerCase());
        });
    }

    /**
     * bytesToHex with offset/length should produce the same result as
     * bytesToHex on the corresponding sub-array.
     */
    @Test
    public void bytesToHexOffsetLength()
    {
        qt().check(rs -> {
            byte[] bytes = BYTE_ARRAY_GEN.next(rs);
            if (bytes.length == 0)
            {
                assertThat(Hex.bytesToHex(bytes, 0, 0)).isEqualTo(Hex.bytesToHex(new byte[0]));
                return;
            }

            int offset = rs.nextInt(0, bytes.length);
            int length = rs.nextInt(0, bytes.length - offset + 1);

            String fromOffsetLength = Hex.bytesToHex(bytes, offset, length);
            String fromSubArray = Hex.bytesToHex(Arrays.copyOfRange(bytes, offset, offset + length));
            assertThat(fromOffsetLength).isEqualTo(fromSubArray);
        });
    }

    /**
     * parseLong correctness: for any long value, converting to hex with
     * Long.toHexString and parsing back with Hex.parseLong should recover
     * the original value.
     */
    @Test
    public void parseLongRoundTrip()
    {
        qt().forAll(Gens.longs().all()).check(value -> {
            String hex = Long.toHexString(value);
            long recovered = Hex.parseLong(hex, 0, hex.length());
            assertThat(recovered).isEqualTo(value);
        });
    }

    /**
     * parseLong agrees with Long.parseUnsignedLong for valid lowercase hex
     * strings of 1..16 characters.
     */
    @Test
    public void parseLongAgreesWithParseUnsignedLong()
    {
        qt().check(rs -> {
            // Generate a valid lowercase hex string of length 1..16
            int len = rs.nextInt(1, 17);
            char[] chars = new char[len];
            for (int i = 0; i < len; i++)
            {
                int nibble = rs.nextInt(16);
                chars[i] = "0123456789abcdef".charAt(nibble);
            }
            String hex = new String(chars);

            // Avoid overflow: Long.parseUnsignedLong throws for values > 16 hex digits,
            // but 16 hex digits can overflow if leading nibble > 'f' (impossible) or
            // the value exceeds unsigned long max. Both methods should agree.
            long fromHex = Hex.parseLong(hex, 0, hex.length());
            long fromJdk = Long.parseUnsignedLong(hex, 16);
            assertThat(fromHex).isEqualTo(fromJdk);
        });
    }

    /**
     * parseLong works correctly with arbitrary start/end offsets within a
     * larger string.
     */
    @Test
    public void parseLongWithOffsets()
    {
        qt().forAll(Gens.longs().all()).check(value -> {
            String hex = Long.toHexString(value);

            // Embed the hex string in a larger string with random prefix/suffix
            String prefix = "deadbeef";
            String suffix = "cafebabe";
            String padded = prefix + hex + suffix;

            int start = prefix.length();
            int end = start + hex.length();
            long recovered = Hex.parseLong(padded, start, end);
            assertThat(recovered).isEqualTo(value);
        });
    }

    /**
     * Verifies that {@link Hex#parseLong} correctly handles uppercase hex characters A-F
     * in addition to lowercase a-f and digits 0-9.
     * <p>
     * Generates mixed-case hex strings and verifies parseLong agrees with
     * {@link Long#parseUnsignedLong(String, int)}.
     */
    @Test
    public void parseLongHandlesUppercase()
    {
        qt().check(rs -> {
            // Generate a valid mixed-case hex string of length 1..16
            int len = rs.nextInt(1, 17);
            char[] chars = new char[len];
            for (int i = 0; i < len; i++)
            {
                int nibble = rs.nextInt(16);
                char c = "0123456789abcdef".charAt(nibble);
                // Randomly uppercase hex letter characters
                if (c >= 'a' && c <= 'f' && rs.nextBoolean())
                    c = (char) (c - 'a' + 'A');
                chars[i] = c;
            }
            String hex = new String(chars);

            long fromHex = Hex.parseLong(hex, 0, hex.length());
            long fromJdk = Long.parseUnsignedLong(hex, 16);
            assertThat(fromHex).isEqualTo(fromJdk);
        });
    }

    /**
     * Odd-length hex strings must throw NumberFormatException.
     */
    @Test
    public void hexToBytesRejectsOddLengthStrings()
    {
        qt().check(rs -> {
            // Generate an odd-length string of valid hex characters
            int len = rs.nextInt(0, 64) * 2 + 1; // always odd
            char[] chars = new char[len];
            for (int i = 0; i < len; i++)
            {
                int nibble = rs.nextInt(16);
                chars[i] = "0123456789abcdef".charAt(nibble);
            }
            String oddHex = new String(chars);

            assertThatThrownBy(() -> Hex.hexToBytes(oddHex))
                .isInstanceOf(NumberFormatException.class);
        });
    }

    /**
     * Hex strings containing non-hex characters must throw NumberFormatException.
     */
    @Test
    public void hexToBytesRejectsNonHexCharacters()
    {
        qt().check(rs -> {
            // Start with a valid even-length hex string (at least 2 chars)
            int pairCount = rs.nextInt(1, 33);
            char[] chars = new char[pairCount * 2];
            for (int i = 0; i < chars.length; i++)
            {
                int nibble = rs.nextInt(16);
                chars[i] = "0123456789abcdef".charAt(nibble);
            }

            // Replace one character with a non-hex character
            int pos = rs.nextInt(0, chars.length);
            char bad;
            do
            {
                bad = (char) rs.nextInt(0, 128);
            }
            while ((bad >= '0' && bad <= '9') || (bad >= 'a' && bad <= 'f') || (bad >= 'A' && bad <= 'F'));
            chars[pos] = bad;

            String invalid = new String(chars);
            assertThatThrownBy(() -> Hex.hexToBytes(invalid))
                .isInstanceOf(NumberFormatException.class);
        });
    }

    /**
     * bytesToHex always produces a string of even length equal to 2 * input length,
     * containing only lowercase hex characters.
     */
    @Test
    public void bytesToHexOutputFormat()
    {
        qt().forAll(BYTE_ARRAY_GEN).check(bytes -> {
            String hex = Hex.bytesToHex(bytes);
            assertThat(hex).hasSize(bytes.length * 2);
            assertThat(hex).matches("[0-9a-f]*");
        });
    }
}
