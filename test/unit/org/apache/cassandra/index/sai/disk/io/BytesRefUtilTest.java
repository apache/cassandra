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
package org.apache.cassandra.index.sai.disk.io;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BytesRefUtilTest
{
    @Test
    public void shouldCopyBufferToBytesRef()
    {
        final Random random = new Random();
        final byte[] expectedBytes = new byte[21];
        random.nextBytes(expectedBytes);
        final BytesRefBuilder refBuilder = new BytesRefBuilder();

        BytesRefUtil.copyBufferToBytesRef(ByteBuffer.wrap(expectedBytes), refBuilder);
        final BytesRef actualBytesRef = refBuilder.get();

        assertEquals(expectedBytes.length, actualBytesRef.length);
        final byte[] actualBytes = new byte[actualBytesRef.length];
        System.arraycopy(actualBytesRef.bytes, actualBytesRef.offset, actualBytes, 0, actualBytesRef.length);
        assertArrayEquals(expectedBytes, actualBytes);
    }
}
