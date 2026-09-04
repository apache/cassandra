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
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * {@link DataOutputBuffer#readFully(DataInputPlus, int)} copies straight into
 * {@code buffer.array()}. That call requires a heap-backed buffer: a direct buffer throws
 * {@link UnsupportedOperationException} from {@link ByteBuffer#array()}.
 *
 * {@code DataOutputBuffer}'s default allocator is heap ({@link DataOutputBuffer#DataOutputBuffer()}
 * uses the no-arg super constructor, which is heap-backed), but {@link DataOutputBuffer#allocate}
 * is overridable, and {@code scratchBuffer} overrides it to allocate off-heap. This test drives
 * that unsafe configuration directly, without going through the scratch buffer, so it does not
 * depend on system properties.
 */
public class DataOutputBufferTest
{
    /** A DataOutputBuffer whose backing buffer is always direct, regardless of allocate_type. */
    private static class DirectDataOutputBuffer extends DataOutputBuffer
    {
        DirectDataOutputBuffer(int size)
        {
            super(size);
        }

        @Override
        protected ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }
    }

    @Test
    public void readFullyOnHeapBufferCopiesBytes() throws Exception
    {
        byte[] payload = randomBytes(64);
        try (DataOutputBuffer dob = new DataOutputBuffer(16))
        {
            dob.readFully(new DataInputBuffer(payload), payload.length);
            assertArrayEquals(payload, dob.toByteArray());
        }
    }

    @Test
    public void readFullyOnHeapBufferGrowsToFit() throws Exception
    {
        // Initial capacity is smaller than the payload, so readFully must expandToFit first.
        byte[] payload = randomBytes(4096);
        try (DataOutputBuffer dob = new DataOutputBuffer(16))
        {
            dob.readFully(new DataInputBuffer(payload), payload.length);
            assertArrayEquals(payload, dob.toByteArray());
        }
    }

    /**
     * Pins the defect: a direct-backed DataOutputBuffer must not reach readFully's heap-array
     * fast path unguarded. Today it does, and this throws UnsupportedOperationException instead
     * of taking a safe fallback.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void readFullyOnDirectBufferFailsFast() throws Exception
    {
        byte[] payload = randomBytes(64);
        try (DataOutputBuffer dob = new DirectDataOutputBuffer(16))
        {
            dob.readFully(new DataInputBuffer(payload), payload.length);
        }
    }

    private static byte[] randomBytes(int length)
    {
        byte[] bytes = new byte[length];
        new Random(42).nextBytes(bytes);
        return bytes;
    }
}
