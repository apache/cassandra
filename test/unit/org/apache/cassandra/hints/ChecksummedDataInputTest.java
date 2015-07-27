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
package org.apache.cassandra.hints;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ChecksummedDataInputTest
{
    @Test
    public void testReadMethods() throws IOException
    {
        // Make sure this array is bigger than the reader buffer size
        // so we test updating the crc across buffer boundaries
        byte[] b = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE * 2];
        for (int i = 0; i < b.length; i++)
            b[i] = (byte)i;

        ByteBuffer buffer;

        // fill a bytebuffer with some input
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(127);
            out.write(b);
            out.writeBoolean(false);
            out.writeByte(10);
            out.writeChar('t');
            out.writeDouble(3.3);
            out.writeFloat(2.2f);
            out.writeInt(42);
            out.writeLong(Long.MAX_VALUE);
            out.writeShort(Short.MIN_VALUE);
            out.writeUTF("utf");
            out.writeVInt(67L);
            out.writeUnsignedVInt(88L);
            out.writeBytes("abcdefghi");

            buffer = out.buffer();
        }

        // calculate expected CRC
        CRC32 crc = new CRC32();
        FBUtilities.updateChecksum(crc, buffer);

        // save the buffer to file to create a RAR
        File file = File.createTempFile("testReadMethods", "1");
        file.deleteOnExit();
        try (SequentialWriter writer = SequentialWriter.open(file))
        {
            writer.write(buffer);
            writer.writeInt((int) crc.getValue());
            writer.finish();
        }

        assertTrue(file.exists());
        assertEquals(buffer.remaining() + 4, file.length());

        try (ChecksummedDataInput reader = ChecksummedDataInput.open(file))
        {
            reader.limit(buffer.remaining() + 4);

            // assert that we read all the right values back
            assertEquals(127, reader.read());
            byte[] bytes = new byte[b.length];
            reader.readFully(bytes);
            assertTrue(Arrays.equals(bytes, b));
            assertEquals(false, reader.readBoolean());
            assertEquals(10, reader.readByte());
            assertEquals('t', reader.readChar());
            assertEquals(3.3, reader.readDouble());
            assertEquals(2.2f, reader.readFloat());
            assertEquals(42, reader.readInt());
            assertEquals(Long.MAX_VALUE, reader.readLong());
            assertEquals(Short.MIN_VALUE, reader.readShort());
            assertEquals("utf", reader.readUTF());
            assertEquals(67L, reader.readVInt());
            assertEquals(88L, reader.readUnsignedVInt());
            assertEquals("abcdefghi", new String(ByteBufferUtil.read(reader, 9).array(), StandardCharsets.UTF_8));

            // assert that the crc matches, and that we've read exactly as many bytes as expected
            assertTrue(reader.checkCrc());
            assertEquals(0, reader.bytesRemaining());

            reader.checkLimit(0);
        }
    }

    @Test
    public void testResetCrc() throws IOException
    {
        CRC32 crc = new CRC32();
        ByteBuffer buffer;

        // fill a bytebuffer with some input
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(127);
            out.writeBoolean(false);
            out.writeByte(10);
            out.writeChar('t');

            buffer = out.buffer();
            FBUtilities.updateChecksum(crc, buffer);
            out.writeInt((int) crc.getValue());

            int bufferPos = out.getLength();
            out.writeDouble(3.3);
            out.writeFloat(2.2f);
            out.writeInt(42);

            buffer = out.buffer();
            buffer.position(bufferPos);
            crc.reset();
            FBUtilities.updateChecksum(crc, buffer);

            out.writeInt((int) crc.getValue());
            buffer = out.buffer();
        }

        // save the buffer to file to create a RAR
        File file = File.createTempFile("testResetCrc", "1");
        file.deleteOnExit();
        try (SequentialWriter writer = SequentialWriter.open(file))
        {
            writer.write(buffer);
            writer.finish();
        }

        assertTrue(file.exists());
        assertEquals(buffer.remaining(), file.length());

        try (ChecksummedDataInput reader = ChecksummedDataInput.open(file))
        {
            reader.limit(buffer.remaining());

            // assert that we read all the right values back
            assertEquals(127, reader.read());
            assertEquals(false, reader.readBoolean());
            assertEquals(10, reader.readByte());
            assertEquals('t', reader.readChar());
            assertTrue(reader.checkCrc());

            reader.resetCrc();
            assertEquals(3.3, reader.readDouble());
            assertEquals(2.2f, reader.readFloat());
            assertEquals(42, reader.readInt());
            assertTrue(reader.checkCrc());
            assertEquals(0, reader.bytesRemaining());
        }
    }

    @Test
    public void testFailedCrc() throws IOException
    {
        CRC32 crc = new CRC32();
        ByteBuffer buffer;

        // fill a bytebuffer with some input
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(127);
            out.writeBoolean(false);
            out.writeByte(10);
            out.writeChar('t');

            buffer = out.buffer();
            FBUtilities.updateChecksum(crc, buffer);

            // update twice so it won't match
            FBUtilities.updateChecksum(crc, buffer);
            out.writeInt((int) crc.getValue());

            buffer = out.buffer();
        }

        // save the buffer to file to create a RAR
        File file = File.createTempFile("testFailedCrc", "1");
        file.deleteOnExit();
        try (SequentialWriter writer = SequentialWriter.open(file))
        {
            writer.write(buffer);
            writer.finish();
        }

        assertTrue(file.exists());
        assertEquals(buffer.remaining(), file.length());

        try (ChecksummedDataInput reader = ChecksummedDataInput.open(file))
        {
            reader.limit(buffer.remaining());

            // assert that we read all the right values back
            assertEquals(127, reader.read());
            assertEquals(false, reader.readBoolean());
            assertEquals(10, reader.readByte());
            assertEquals('t', reader.readChar());
            assertFalse(reader.checkCrc());
            assertEquals(0, reader.bytesRemaining());
        }
    }
}
