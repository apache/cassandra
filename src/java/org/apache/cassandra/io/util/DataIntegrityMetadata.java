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
import java.io.File;
import java.io.IOException;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Throwables;

public class DataIntegrityMetadata
{
    public static ChecksumValidator checksumValidator(Descriptor desc) throws IOException
    {
        return new ChecksumValidator(desc);
    }

    public static class ChecksumValidator implements Closeable
    {
        private final ChecksumType checksumType;
        private final RandomAccessReader reader;
        public final int chunkSize;
        private final String dataFilename;

        public ChecksumValidator(Descriptor descriptor) throws IOException
        {
            this(descriptor.version.uncompressedChecksumType(),
                 RandomAccessReader.open(new File(descriptor.filenameFor(Component.CRC))),
                 descriptor.filenameFor(Component.DATA));
        }

        public ChecksumValidator(ChecksumType checksumType, RandomAccessReader reader, String dataFilename) throws IOException
        {
            this.checksumType = checksumType;
            this.reader = reader;
            this.dataFilename = dataFilename;
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
            int current = (int) checksumType.of(bytes, start, end);
            int actual = reader.readInt();
            if (current != actual)
                throw new IOException("Corrupted File : " + dataFilename);
        }

        public void close()
        {
            reader.close();
        }
    }

    public static FileDigestValidator fileDigestValidator(Descriptor desc) throws IOException
    {
        return new FileDigestValidator(desc);
    }

    public static class FileDigestValidator implements Closeable
    {
        private final Checksum checksum;
        private final RandomAccessReader digestReader;
        private final RandomAccessReader dataReader;
        private final Descriptor descriptor;
        private long storedDigestValue;

        public FileDigestValidator(Descriptor descriptor) throws IOException
        {
            this.descriptor = descriptor;
            checksum = descriptor.version.uncompressedChecksumType().newInstance();
            digestReader = RandomAccessReader.open(new File(descriptor.filenameFor(descriptor.digestComponent)));
            dataReader = RandomAccessReader.open(new File(descriptor.filenameFor(Component.DATA)));
            try
            {
                storedDigestValue = Long.parseLong(digestReader.readLine());
            }
            catch (Exception e)
            {
                close();
                // Attempting to create a FileDigestValidator without a DIGEST file will fail
                throw new IOException("Corrupted SSTable : " + descriptor.filenameFor(Component.DATA));
            }
        }

        // Validate the entire file
        public void validate() throws IOException
        {
            CheckedInputStream checkedInputStream = new CheckedInputStream(dataReader, checksum);
            byte[] chunk = new byte[64 * 1024];

            while( checkedInputStream.read(chunk) > 0 ) { }
            long calculatedDigestValue = checkedInputStream.getChecksum().getValue();
            if (storedDigestValue != calculatedDigestValue)
            {
                throw new IOException("Corrupted SSTable : " + descriptor.filenameFor(Component.DATA));
            }
        }

        public void close()
        {
            Throwables.perform(digestReader::close,
                               dataReader::close);
        }
    }
}
