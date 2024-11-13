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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.RateLimitedTrackedDataInputPlus;
import org.apache.cassandra.streaming.StreamManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RateLimitedTrackedDataInputPlusTest
{

    @Before
    public void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testBatchSizes() throws Exception
    {
        internalTestBytesRead(2);
        internalTestBytesRead(10);
        internalTestBytesRead(100);

    }

    @Test
    public void testWronglySetTotalToReadValueCauseException() throws Exception
    {
        DatabaseDescriptor.setStreamThroughputInboundMebibytesPerSecAsInt(1);
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        try
        {
            // boolean
            out.writeBoolean(true);
            // byte
            out.writeByte(0x1);
            // char
            out.writeChar('a');
            // short
            out.writeShort(1);
            // int
            out.writeInt(1);
            // long
            out.writeLong(1L);
            // float
            out.writeFloat(1.0f);
            // double
            out.writeDouble(1.0d);

            // String
            out.writeUTF("abc");
            testData = baos.toByteArray();
        }
        finally
        {
            out.close();
        }

        DataInputPlus.DataInputStreamPlus stream = new DataInputBuffer(testData);
        RateLimitedTrackedDataInputPlus reader = new RateLimitedTrackedDataInputPlus(stream, -1, StreamManager.getInboundRateLimiter(), testData.length, 10);

        // reset the bytesRead and totalBytesToRead to 10, this should not happen because this means all data have been read out
        // test if this happens, it will cause error
        reader.reset(10, 10);
        try
        {
            reader.readBoolean();
            fail();
        } catch (AssertionError e)
        {
            assertTrue(e.getMessage().contains("Trying to acquire 0 bytes which is not greater than 0 and this means something wrong."));
        }
    }

    private void internalTestBytesRead(int batchSize) throws Exception
    {
        DatabaseDescriptor.setStreamThroughputInboundMebibytesPerSecAsInt(1);
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        try
        {
            // boolean
            out.writeBoolean(true);
            // byte
            out.writeByte(0x1);
            // char
            out.writeChar('a');
            // short
            out.writeShort(1);
            // int
            out.writeInt(1);
            // long
            out.writeLong(1L);
            // float
            out.writeFloat(1.0f);
            // double
            out.writeDouble(1.0d);

            // String
            out.writeUTF("abc");
            testData = baos.toByteArray();
        }
        finally
        {
            out.close();
        }

        DataInputPlus.DataInputStreamPlus stream = new DataInputBuffer(testData);
        RateLimitedTrackedDataInputPlus reader = new RateLimitedTrackedDataInputPlus(stream, -1, StreamManager.getInboundRateLimiter(), testData.length, batchSize);

        try
        {
            int expectedAcquiredBytesFromRateLimiter = Math.min(batchSize, testData.length);
            // boolean = 1byte
            boolean bool = reader.readBoolean();
            assertTrue(bool);
            assertEquals(1, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 1);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // byte = 1byte
            byte b = reader.readByte();
            assertEquals(b, 0x1);
            assertEquals(2, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 1);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // char = 2byte
            char c = reader.readChar();
            assertEquals('a', c);
            assertEquals(4, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 2);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // short = 2bytes
            short s = reader.readShort();
            assertEquals(1, s);
            assertEquals((short) 6, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 2);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // int = 4bytes
            int i = reader.readInt();
            assertEquals(1, i);
            assertEquals(10, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 4);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // long = 8bytes
            long l = reader.readLong();
            assertEquals(1L, l);
            assertEquals(18, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 8);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // float = 4bytes
            float f = reader.readFloat();
            assertEquals(1.0f, f, 0);
            assertEquals(22, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 4);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // double = 8bytes
            double d = reader.readDouble();
            assertEquals(1.0d, d, 0);
            assertEquals(30, reader.getBytesRead());
            expectedAcquiredBytesFromRateLimiter = getNewExpectedAcquiredBytes(expectedAcquiredBytesFromRateLimiter, batchSize, 8);
            assertEquals(expectedAcquiredBytesFromRateLimiter, reader.getCurrentAcquiredBytes());
            // String("abc") = 2(string size) + 3 = 5 bytes
            String str = reader.readUTF();
            assertEquals("abc", str);
            assertEquals(35, reader.getBytesRead());
            // after last value is read out, the acquiredBytesToRead should always be 0
            assertEquals(0, reader.getCurrentAcquiredBytes());

            assertEquals(testData.length, reader.getBytesRead());
        }
        finally
        {
            stream.close();
        }

        reader.reset(0, 3);
        assertEquals(0, reader.getBytesRead());
        assertEquals(3, reader.getTotalBytesToRead());
    }

    private int getNewExpectedAcquiredBytes(int currentValue, int batchSize, int newBytesToRead)
    {
        while (currentValue < newBytesToRead)
        {
            currentValue += batchSize;
        }
        return currentValue - newBytesToRead;
    }
}
