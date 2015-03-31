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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.DataOutput;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import com.google.common.base.Charsets;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.CRC32Factory;
import org.apache.cassandra.utils.FBUtilities;

public class DataIntegrityMetadata
{
    public static ChecksumValidator checksumValidator(Descriptor desc) throws IOException
    {
        return new ChecksumValidator(desc);
    }

    public static class ChecksumValidator implements Closeable
    {
        private final Checksum checksum;
        private final RandomAccessReader reader;
        private final Descriptor descriptor;
        public final int chunkSize;

        public ChecksumValidator(Descriptor descriptor) throws IOException
        {
            this.descriptor = descriptor;
            checksum = descriptor.version.hasAllAdlerChecksums() ? new Adler32() : CRC32Factory.instance.create();
            reader = RandomAccessReader.open(new File(descriptor.filenameFor(Component.CRC)));
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
            checksum.update(bytes, start, end);
            int current = (int) checksum.getValue();
            checksum.reset();
            int actual = reader.readInt();
            if (current != actual)
                throw new IOException("Corrupted SSTable : " + descriptor.filenameFor(Component.DATA));
        }

        public void close()
        {
            reader.close();
        }
    }

    public static class ChecksumWriter
    {
        private final Adler32 incrementalChecksum = new Adler32();
        private final DataOutput incrementalOut;
        private final Adler32 fullChecksum = new Adler32();

        public ChecksumWriter(DataOutput incrementalOut)
        {
            this.incrementalOut = incrementalOut;
        }

        public void writeChunkSize(int length)
        {
            try
            {
                incrementalOut.writeInt(length);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        // checksumIncrementalResult indicates if the checksum we compute for this buffer should itself be
        // included in the full checksum, translating to if the partial checksum is serialized along with the
        // data it checksums (in which case the file checksum as calculated by external tools would mismatch if
        // we did not include it), or independently.

        // CompressedSequentialWriters serialize the partial checksums inline with the compressed data chunks they
        // corroborate, whereas ChecksummedSequentialWriters serialize them to a different file.
        public void append(byte[] buffer, int start, int end, boolean checksumIncrementalResult)
        {
            try
            {
                int incrementalChecksumValue;

                incrementalChecksum.update(buffer, start, end);
                incrementalChecksumValue = (int) incrementalChecksum.getValue();
                incrementalOut.writeInt(incrementalChecksumValue);
                incrementalChecksum.reset();

                fullChecksum.update(buffer, start, end);

                if (checksumIncrementalResult)
                {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(incrementalChecksumValue);
                    fullChecksum.update(byteBuffer.array(), 0, byteBuffer.array().length);
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public void appendDirect(ByteBuffer bb)
        {
            try
            {
                ByteBuffer toAppend = bb.duplicate();
                toAppend.mark();
                FBUtilities.directCheckSum(incrementalChecksum, toAppend);
                toAppend.reset();

                incrementalOut.writeInt((int) incrementalChecksum.getValue());
                incrementalChecksum.reset();

                FBUtilities.directCheckSum(fullChecksum, toAppend);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public void writeFullChecksum(Descriptor descriptor)
        {
            File outFile = new File(descriptor.filenameFor(Component.DIGEST));
            BufferedWriter out = null;
            try
            {
                out = Files.newBufferedWriter(outFile.toPath(), Charsets.UTF_8);
                out.write(String.valueOf(fullChecksum.getValue()));
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, outFile);
            }
            finally
            {
                FileUtils.closeQuietly(out);
            }
        }
    }
}
