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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.cassandra.utils.ChecksumType;

public class DataIntegrityMetadata
{
    public static class ChecksumValidator implements Closeable
    {
        private final ChecksumType checksumType;
        private final RandomAccessReader reader;
        public final int chunkSize;

        public ChecksumValidator(File dataFile, File crcFile) throws IOException
        {
            this(ChecksumType.CRC32,
                 RandomAccessReader.open(crcFile),
                 dataFile.absolutePath());
        }

        public ChecksumValidator(ChecksumType checksumType, RandomAccessReader reader, String dataFilename) throws IOException
        {
            this.checksumType = checksumType;
            this.reader = reader;
            chunkSize = reader.readInt();
        }

        public void seek(long offset)
        {
            long start = chunkStart(offset);
            reader.seek(((start / chunkSize) * 4L) + 4); // 8 byte checksum per chunk + 4 byte header/chunkLength
        }

        public long chunkStart(long offset)
        {
            long startChunk = offset / chunkSize;
            return startChunk * chunkSize;
        }

        public void validate(byte[] bytes, int start, int end) throws IOException
        {
            int calculatedValue = (int) checksumType.of(bytes, start, end);
            int storedValue = reader.readInt();
            if (calculatedValue != storedValue)
                throw new IOException(String.format("Corrupted file: integrity check (%s) failed for %s: %d != %d", checksumType.name(), reader.getPath(), storedValue, calculatedValue));
        }

        /**
         * validates the checksum with the bytes from the specified buffer.
         *
         * Upon return, the buffer's position will
         * be updated to its limit; its limit will not have been changed.
         */
        public void validate(ByteBuffer buffer) throws IOException
        {
            int calculatedValue = (int) checksumType.of(buffer);
            int storedValue = reader.readInt();
            if (calculatedValue != storedValue)
                throw new IOException(String.format("Corrupted file: integrity check (%s) failed for %s: %d != %d", checksumType.name(), reader.getPath(), storedValue, calculatedValue));
        }

        public void close()
        {
            reader.close();
        }
    }

    public static class FileDigestValidator
    {
        private final Checksum checksum;
        private final File dataFile;
        private final File digestFile;

        public FileDigestValidator(File dataFile, File digestFile) throws IOException
        {
            this.dataFile = dataFile;
            this.digestFile = digestFile;
            this.checksum = ChecksumType.CRC32.newInstance();
        }

        // Validate the entire file
        public void validate() throws IOException
        {
            try (RandomAccessReader digestReader = RandomAccessReader.open(digestFile);
                 RandomAccessReader dataReader = RandomAccessReader.open(dataFile);
                 CheckedInputStream checkedInputStream = new CheckedInputStream(dataReader, checksum);)
            {
                long storedDigestValue = Long.parseLong(digestReader.readLine());
                byte[] chunk = new byte[64 * 1024];
                while (checkedInputStream.read(chunk) > 0) ;
                long calculatedDigestValue = checkedInputStream.getChecksum().getValue();
                if (storedDigestValue != calculatedDigestValue)
                    throw new IOException(String.format("Corrupted file: integrity check (digest) failed for %s: %d != %d", dataFile, storedDigestValue, calculatedDigestValue));
            }
        }
    }
}
