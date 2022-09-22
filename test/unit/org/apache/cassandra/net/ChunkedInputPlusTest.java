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
package org.apache.cassandra.net;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.*;

public class ChunkedInputPlusTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyIterable()
    {
        ChunkedInputPlus.of(Collections.emptyList());
    }

    @Test
    public void testUnderRead() throws IOException
    {
        List<ShareableBytes> chunks = Lists.newArrayList(
            chunk(1, 1), chunk(2, 2), chunk(3, 3)
        );

        try (ChunkedInputPlus input = ChunkedInputPlus.of(chunks))
        {
            byte[] readBytes = new byte[5];
            input.readFully(readBytes);
            assertArrayEquals(new byte[] { 1, 2, 2, 3, 3 }, readBytes);

            assertFalse(chunks.get(0).hasRemaining());
            assertFalse(chunks.get(1).hasRemaining());
            assertTrue (chunks.get(2).hasRemaining());

            assertTrue (chunks.get(0).isReleased());
            assertTrue (chunks.get(1).isReleased());
            assertFalse(chunks.get(2).isReleased());
        }

        // close should release the last chunk
        assertTrue(chunks.get(2).isReleased());
    }

    @Test
    public void testExactRead() throws IOException
    {
        List<ShareableBytes> chunks = Lists.newArrayList(
            chunk(1, 1), chunk(2, 2), chunk(3, 3)
        );

        try (ChunkedInputPlus input = ChunkedInputPlus.of(chunks))
        {
            byte[] readBytes = new byte[6];
            input.readFully(readBytes);
            assertArrayEquals(new byte[] { 1, 2, 2, 3, 3, 3 }, readBytes);

            assertFalse(chunks.get(0).hasRemaining());
            assertFalse(chunks.get(1).hasRemaining());
            assertFalse(chunks.get(2).hasRemaining());

            assertTrue (chunks.get(0).isReleased());
            assertTrue (chunks.get(1).isReleased());
            assertFalse(chunks.get(2).isReleased());
        }

        // close should release the last chunk
        assertTrue(chunks.get(2).isReleased());
    }

    @Test
    public void testOverRead() throws IOException
    {
        List<ShareableBytes> chunks = Lists.newArrayList(
            chunk(1, 1), chunk(2, 2), chunk(3, 3)
        );

        boolean eofCaught = false;
        try (ChunkedInputPlus input = ChunkedInputPlus.of(chunks))
        {
            byte[] readBytes = new byte[7];
            input.readFully(readBytes);
            assertArrayEquals(new byte[] { 1, 2, 2, 3, 3, 3, 4 }, readBytes);
        }
        catch (EOFException e)
        {
            eofCaught = true;

            assertFalse(chunks.get(0).hasRemaining());
            assertFalse(chunks.get(1).hasRemaining());
            assertFalse(chunks.get(2).hasRemaining());

            assertTrue (chunks.get(2).isReleased());
            assertTrue (chunks.get(1).isReleased());
            assertTrue (chunks.get(2).isReleased());
        }
        assertTrue(eofCaught);
    }

    @Test
    public void testRemainder() throws IOException
    {
        List<ShareableBytes> chunks = Lists.newArrayList(
            chunk(1, 1), chunk(2, 2), chunk(3, 3)
        );

        try (ChunkedInputPlus input = ChunkedInputPlus.of(chunks))
        {
            byte[] readBytes = new byte[5];
            input.readFully(readBytes);
            assertArrayEquals(new byte[] { 1, 2, 2, 3, 3 }, readBytes);

            assertEquals(1, input.remainder());

            assertTrue(chunks.get(0).isReleased());
            assertTrue(chunks.get(1).isReleased());
            assertTrue(chunks.get(2).isReleased()); // should be released by remainder()
        }
    }

    private ShareableBytes chunk(int size, int fill)
    {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        Arrays.fill(buffer.array(), (byte) fill);
        return ShareableBytes.wrap(buffer);
    }
}
