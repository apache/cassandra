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
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.apache.cassandra.io.compress.ICompressor.WrappedArray;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LZ4CompressorTest
{

    LZ4Compressor compressor;

    @Before
    public void setUp()
    {
        compressor = LZ4Compressor.create(Collections.<String, String>emptyMap());
    }

    public void test(byte[] data, int off, int len) throws IOException
    {
        final int outOffset = 3;
        final WrappedArray out = new WrappedArray(new byte[outOffset + compressor.initialCompressedBufferLength(len)]);
        new Random().nextBytes(out.buffer);
        final int compressedLength = compressor.compress(data, off, len, out, outOffset);
        final int restoredOffset = 5;
        final byte[] restored = new byte[restoredOffset + len];
        new Random().nextBytes(restored);
        final int decompressedLength = compressor.uncompress(out.buffer, outOffset, compressedLength, restored, restoredOffset);
        assertEquals(decompressedLength, len);
        assertArrayEquals(Arrays.copyOfRange(data, off, off + len),
                          Arrays.copyOfRange(restored, restoredOffset, restoredOffset + decompressedLength));
    }

    public void test(byte[] data) throws IOException
    {
        test(data, 0, data.length);
    }

    @Test
    public void testEmptyArray() throws IOException
    {
        test(new byte[0]);
    }

    @Test
    public void testShortArray() throws UnsupportedEncodingException, IOException
    {
        test("Cassandra".getBytes("UTF-8"), 1, 7);
    }

    @Test
    public void testLongArray() throws UnsupportedEncodingException, IOException
    {
        byte[] data = new byte[1 << 20];
        test(data, 13, 1 << 19);
        new Random(0).nextBytes(data);
        test(data, 13, 1 << 19);
    }
}
