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

import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

import javax.annotation.Nonnull;

import org.apache.cassandra.io.FSWriteError;

public class ChecksumWriter
{
    private final CRC32 incrementalChecksum = new CRC32();
    private final DataOutput incrementalOut;
    private final CRC32 fullChecksum = new CRC32();

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
    public void appendDirect(ByteBuffer bb, boolean checksumIncrementalResult)
    {
        try
        {
            ByteBuffer toAppend = bb.duplicate();
            toAppend.mark();
            incrementalChecksum.update(toAppend);
            toAppend.reset();

            int incrementalChecksumValue = (int) incrementalChecksum.getValue();
            incrementalOut.writeInt(incrementalChecksumValue);

            fullChecksum.update(toAppend);
            if (checksumIncrementalResult)
            {
                ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt(incrementalChecksumValue);
                assert byteBuffer.arrayOffset() == 0;
                fullChecksum.update(byteBuffer.array(), 0, byteBuffer.array().length);
            }
            incrementalChecksum.reset();

        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void writeFullChecksum(@Nonnull File digestFile)
    {
        try (FileOutputStreamPlus fos = new FileOutputStreamPlus(digestFile))
        {
            fos.write(String.valueOf(fullChecksum.getValue()).getBytes(StandardCharsets.UTF_8));
            fos.flush();
            fos.getChannel().force(true);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, digestFile);
        }
    }
}

