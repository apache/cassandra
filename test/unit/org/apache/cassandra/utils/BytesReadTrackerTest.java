/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;

import org.apache.cassandra.io.util.BytesReadTracker;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.io.util.TrackedInputStream;

public class BytesReadTrackerTest
{

    @Test
    public void testBytesRead() throws Exception
    {
        internalTestBytesRead(true);
        internalTestBytesRead(false);
    }

    @Test
    public void testUnsignedRead() throws Exception
    {
        internalTestUnsignedRead(true);
        internalTestUnsignedRead(false);
    }

    @Test
    public void testSkipBytesAndReadFully() throws Exception
    {
        internalTestSkipBytesAndReadFully(true);
        internalTestSkipBytesAndReadFully(false);
    }

    @Test
    public void testReadLine() throws Exception
    {
        internalTestReadLine(true);
        internalTestReadLine(false);
    }

    public void internalTestBytesRead(boolean inputStream) throws Exception
    {
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

        DataInputPlus.DataInputStreamPlus in = new DataInputPlus.DataInputStreamPlus(new ByteArrayInputStream(testData));
        BytesReadTracker tracker = inputStream? new TrackedInputStream(in) : new TrackedDataInputPlus(in);
        DataInputPlus reader = inputStream? new DataInputPlus.DataInputStreamPlus((TrackedInputStream)tracker) : (DataInputPlus) tracker;

        try
        {
            // boolean = 1byte
            boolean bool = reader.readBoolean();
            assertTrue(bool);
            assertEquals(1, tracker.getBytesRead());
            // byte = 1byte
            byte b = reader.readByte();
            assertEquals(b, 0x1);
            assertEquals(2, tracker.getBytesRead());
            // char = 2byte
            char c = reader.readChar();
            assertEquals('a', c);
            assertEquals(4, tracker.getBytesRead());
            // short = 2bytes
            short s = reader.readShort();
            assertEquals(1, s);
            assertEquals((short) 6, tracker.getBytesRead());
            // int = 4bytes
            int i = reader.readInt();
            assertEquals(1, i);
            assertEquals(10, tracker.getBytesRead());
            // long = 8bytes
            long l = reader.readLong();
            assertEquals(1L, l);
            assertEquals(18, tracker.getBytesRead());
            // float = 4bytes
            float f = reader.readFloat();
            assertEquals(1.0f, f, 0);
            assertEquals(22, tracker.getBytesRead());
            // double = 8bytes
            double d = reader.readDouble();
            assertEquals(1.0d, d, 0);
            assertEquals(30, tracker.getBytesRead());
            // String("abc") = 2(string size) + 3 = 5 bytes
            String str = reader.readUTF();
            assertEquals("abc", str);
            assertEquals(35, tracker.getBytesRead());

            assertEquals(testData.length, tracker.getBytesRead());
        }
        finally
        {
            in.close();
        }

        tracker.reset(0);
        assertEquals(0, tracker.getBytesRead());
    }

    public void internalTestUnsignedRead(boolean inputStream) throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        try
        {
            // byte
            out.writeByte(0x1);
            // short
            out.writeShort(1);
            testData = baos.toByteArray();
        }
        finally
        {
            out.close();
        }

        DataInputPlus.DataInputStreamPlus in = new DataInputPlus.DataInputStreamPlus(new ByteArrayInputStream(testData));
        BytesReadTracker tracker = inputStream? new TrackedInputStream(in) : new TrackedDataInputPlus(in);
        DataInputPlus reader = inputStream? new DataInputPlus.DataInputStreamPlus((TrackedInputStream)tracker) : (DataInputPlus) tracker;

        try
        {
            // byte = 1byte
            int b = reader.readUnsignedByte();
            assertEquals(b, 1);
            assertEquals(1, tracker.getBytesRead());
            // short = 2bytes
            int s = reader.readUnsignedShort();
            assertEquals(1, s);
            assertEquals(3, tracker.getBytesRead());

            assertEquals(testData.length, tracker.getBytesRead());
        }
        finally
        {
            in.close();
        }
    }

    public void internalTestSkipBytesAndReadFully(boolean inputStream) throws Exception
    {
        String testStr = "1234567890";
        byte[] testData = testStr.getBytes();

        DataInputPlus.DataInputStreamPlus in = new DataInputPlus.DataInputStreamPlus(new ByteArrayInputStream(testData));
        BytesReadTracker tracker = inputStream? new TrackedInputStream(in) : new TrackedDataInputPlus(in);
        DataInputPlus reader = inputStream? new DataInputPlus.DataInputStreamPlus((TrackedInputStream)tracker) : (DataInputPlus) tracker;

        try
        {
            // read first 5 bytes
            byte[] out = new byte[5];
            reader.readFully(out, 0, 5);
            assertEquals("12345", new String(out));
            assertEquals(5, tracker.getBytesRead());

            // then skip 2 bytes
            reader.skipBytes(2);
            assertEquals(7, tracker.getBytesRead());

            // and read the rest
            out = new byte[3];
            reader.readFully(out);
            assertEquals("890", new String(out));
            assertEquals(10, tracker.getBytesRead());

            assertEquals(testData.length, tracker.getBytesRead());
        }
        finally
        {
            in.close();
        }
    }

    public void internalTestReadLine(boolean inputStream) throws Exception
    {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream("1".getBytes()));
        BytesReadTracker tracker = inputStream? new TrackedInputStream(in) : new TrackedDataInputPlus(in);
        DataInputPlus reader = inputStream? new DataInputPlus.DataInputStreamPlus((TrackedInputStream)tracker) : (DataInputPlus) tracker;

        try
        {
            String line = reader.readLine();
            if (inputStream)
                assertEquals(line, "1");
            else
                fail("Should have thrown UnsupportedOperationException");
        }
        catch (UnsupportedOperationException e)
        {
            if (inputStream)
                fail("Should have not thrown UnsupportedOperationException");
        }
        finally
        {
            in.close();
        }
    }
}
