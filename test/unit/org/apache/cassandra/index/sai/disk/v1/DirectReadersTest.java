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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static junit.framework.Assert.assertEquals;

public class DirectReadersTest extends SaiRandomizedTest
{
    @Test
    public void testDirectReader() throws IOException
    {
        byte bitsPerValue = 1;
        while (bitsPerValue <= 4)
        {
            testDirectReader(bitsPerValue);
            bitsPerValue *= 2;
        }
        while (bitsPerValue <= 32)
        {
            testDirectReader(bitsPerValue);
            bitsPerValue += 4;
        }
        bitsPerValue = 40;
        while (bitsPerValue <= 64) {
            testDirectReader(bitsPerValue);
            bitsPerValue += 8;
        }
    }

    private void testDirectReader(byte bitsPerValue) throws IOException
    {
        var n = 10000;
        var originals = new long[n];
        var out = new ByteBuffersDataOutput();
        var writer = DirectWriter.getInstance(out, n, bitsPerValue);
        for (int i = 0; i < n; i++) {
            var L = randomValueWithBits(bitsPerValue);
            originals[i] = L;
            writer.add(L);
        }
        writer.finish();

        var in = out.toDataInput();
        var reader = DirectReaders.getReaderForBitsPerValue(bitsPerValue);
        for (int i = 0; i < n; i++) {
            var L = reader.get(in, 0, i);
            assertEquals(String.format("error decoding at %s bits per value", bitsPerValue), originals[i], L);
        }
    }

    private long randomValueWithBits(int bitsPerValue)
    {
        return randomLong() >>> (64 - bitsPerValue);
    }
}
