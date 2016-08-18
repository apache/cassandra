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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.cassandra.config.DatabaseDescriptor;

public class ChecksummedRandomAccessReaderTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void readFully() throws IOException
    {
        final File data = File.createTempFile("testReadFully", "data");
        final File crc = File.createTempFile("testReadFully", "crc");

        final byte[] expected = new byte[70 * 1024];   // bit more than crc chunk size, so we can test rebuffering.
        ThreadLocalRandom.current().nextBytes(expected);

        try (SequentialWriter writer = new ChecksummedSequentialWriter(data, crc, null, SequentialWriterOption.DEFAULT))
        {
            writer.write(expected);
            writer.finish();
        }

        assert data.exists();

        try (RandomAccessReader reader = ChecksummedRandomAccessReader.open(data, crc))
        {
            byte[] b = new byte[expected.length];
            reader.readFully(b);

            assertArrayEquals(expected, b);

            assertTrue(reader.isEOF());
        }
    }

    @Test
    public void seek() throws IOException
    {
        final File data = File.createTempFile("testSeek", "data");
        final File crc = File.createTempFile("testSeek", "crc");

        final byte[] dataBytes = new byte[70 * 1024];   // bit more than crc chunk size
        ThreadLocalRandom.current().nextBytes(dataBytes);

        try (SequentialWriter writer = new ChecksummedSequentialWriter(data, crc, null, SequentialWriterOption.DEFAULT))
        {
            writer.write(dataBytes);
            writer.finish();
        }

        assert data.exists();

        try (RandomAccessReader reader = ChecksummedRandomAccessReader.open(data, crc))
        {

            final int seekPosition = 66000;
            reader.seek(seekPosition);

            byte[] b = new byte[dataBytes.length - seekPosition];
            reader.readFully(b);

            byte[] expected = Arrays.copyOfRange(dataBytes, seekPosition, dataBytes.length);

            assertArrayEquals(expected, b);

            assertTrue(reader.isEOF());
        }
    }

    @Test(expected = CorruptFileException.class)
    public void corruptionDetection() throws IOException
    {
        final File data = File.createTempFile("corruptionDetection", "data");
        final File crc = File.createTempFile("corruptionDetection", "crc");

        final byte[] expected = new byte[5 * 1024];
        Arrays.fill(expected, (byte) 0);

        try (SequentialWriter writer = new ChecksummedSequentialWriter(data, crc, null, SequentialWriterOption.DEFAULT))
        {
            writer.write(expected);
            writer.finish();
        }

        assert data.exists();

        // simulate corruption of file
        try (RandomAccessFile dataFile = new RandomAccessFile(data, "rw"))
        {
            dataFile.seek(1024);
            dataFile.write((byte) 5);
        }

        try (RandomAccessReader reader = ChecksummedRandomAccessReader.open(data, crc))
        {
            byte[] b = new byte[expected.length];
            reader.readFully(b);

            assertArrayEquals(expected, b);

            assertTrue(reader.isEOF());
        }
    }
}
