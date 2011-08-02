/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.compress;

import java.io.*;

import org.junit.Test;

import org.apache.cassandra.io.util.*;

public class CompressedRandomAccessReaderTest
{
    @Test
    public void testResetAndTruncate() throws IOException
    {
        // test reset in current buffer or previous one
        testResetAndTruncate(false, 10);
        testResetAndTruncate(false, CompressedSequentialWriter.CHUNK_LENGTH);
    }

    @Test
    public void testResetAndTruncateCompressed() throws IOException
    {
        // test reset in current buffer or previous one
        testResetAndTruncate(true, 10);
        testResetAndTruncate(true, CompressedSequentialWriter.CHUNK_LENGTH);
    }

    private void testResetAndTruncate(boolean compressed, int junkSize) throws IOException
    {
        String filename = "corruptFile";
        File f = new File(filename);

        try
        {
            SequentialWriter writer = compressed
                ? new CompressedSequentialWriter(f, filename + ".metadata", false)
                : new SequentialWriter(f, CompressedSequentialWriter.CHUNK_LENGTH, false);

            writer.write("The quick ".getBytes());
            FileMark mark = writer.mark();
            writer.write("blue fox jumps over the lazy dog".getBytes());

            // write enough to be sure to change chunk
            for (int i = 0; i < junkSize; ++i)
            {
                writer.write((byte)1);
            }

            writer.resetAndTruncate(mark);
            writer.write("brown fox jumps over the lazy dog".getBytes());
            writer.close();

            assert f.exists();
            RandomAccessReader reader = compressed
                ? new CompressedRandomAccessReader(filename, new CompressionMetadata(filename + ".metadata", f.length()), false)
                : new RandomAccessReader(f, CompressedSequentialWriter.CHUNK_LENGTH, false);
            String expected = "The quick brown fox jumps over the lazy dog";
            assert reader.length() == expected.length();
            byte[] b = new byte[expected.length()];
            reader.readFully(b);
            assert new String(b).equals(expected) : "Expecting '" + expected + "', got '" + new String(b) + "'";
        }
        finally
        {
            // cleanup
            if (f.exists())
                f.delete();
            File metadata = new File(filename + ".metadata");
            if (compressed && metadata.exists())
                metadata.delete();
        }
    }
}
