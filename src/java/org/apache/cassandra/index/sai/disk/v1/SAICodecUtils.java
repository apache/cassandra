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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.apache.lucene.codecs.CodecUtil.CODEC_MAGIC;
import static org.apache.lucene.codecs.CodecUtil.FOOTER_MAGIC;
import static org.apache.lucene.codecs.CodecUtil.footerLength;
import static org.apache.lucene.codecs.CodecUtil.readBEInt;
import static org.apache.lucene.codecs.CodecUtil.readBELong;
import static org.apache.lucene.codecs.CodecUtil.writeBEInt;
import static org.apache.lucene.codecs.CodecUtil.writeBELong;

public class SAICodecUtils
{
    // Lucene switched from big-endian to little-endian file format, but retained
    // big-endian values in CodecUtils header and footer for compatibility.
    // We follow their lead and use explicitly big-endian values here.

    public static final String FOOTER_POINTER = "footerPointer";

    public static void writeHeader(IndexOutput out) throws IOException
    {
        writeBEInt(out, CODEC_MAGIC);
        out.writeString(Version.LATEST.toString());
    }

    public static void writeFooter(IndexOutput out) throws IOException
    {
        writeBEInt(out, FOOTER_MAGIC);
        writeBEInt(out, 0);
        writeChecksum(out);
    }

    public static void checkHeader(DataInput in) throws IOException
    {
        final int actualMagic = readBEInt(in);
        if (actualMagic != CODEC_MAGIC)
        {
            throw new CorruptIndexException("codec header mismatch: actual header=" + actualMagic + " vs expected header=" + CODEC_MAGIC, in);
        }
        final Version actualVersion = Version.parse(in.readString());
        if (!actualVersion.onOrAfter(Version.EARLIEST))
        {
            throw new IOException("Unsupported version: " + actualVersion);
        }
    }

    public static void checkFooter(ChecksumIndexInput in) throws IOException
    {
        validateFooter(in, false);
        long actualChecksum = in.getChecksum();
        long expectedChecksum = readChecksum(in);
        if (expectedChecksum != actualChecksum)
        {
            throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + Long.toHexString(expectedChecksum) +
                                            " actual=" + Long.toHexString(actualChecksum), in);
        }
    }

    public static void validate(IndexInput input) throws IOException
    {
        checkHeader(input);
        validateFooterAndResetPosition(input);
    }

    public static void validate(IndexInput input, long footerPointer) throws IOException
    {
        checkHeader(input);

        long current = input.getFilePointer();
        input.seek(footerPointer);
        validateFooter(input, true);

        input.seek(current);
    }

    /**
     * See {@link org.apache.lucene.codecs.CodecUtil#checksumEntireFile(org.apache.lucene.store.IndexInput)}.
     * @param input IndexInput to validate.
     * @throws IOException if a corruption is detected.
     */
    public static void validateChecksum(IndexInput input) throws IOException
    {
        IndexInput clone = input.clone();
        clone.seek(0L);
        ChecksumIndexInput in = IndexFileUtils.getBufferedChecksumIndexInput(clone);

        assert in.getFilePointer() == 0L : in.getFilePointer() + " bytes already read from this input!";

        if (in.length() < (long) footerLength())
            throw new CorruptIndexException("misplaced codec footer (file truncated?): length=" + in.length() + " but footerLength==" + footerLength(), input);
        else
        {
            in.seek(in.length() - (long) footerLength());
            checkFooter(in);
        }
    }

    // Copied from Lucene PackedInts as they are not public

    public static int checkBlockSize(int blockSize, int minBlockSize, int maxBlockSize)
    {
        if (blockSize >= minBlockSize && blockSize <= maxBlockSize)
        {
            if ((blockSize & blockSize - 1) != 0)
            {
                throw new IllegalArgumentException("blockSize must be a power of two, got " + blockSize);
            }
            else
            {
                return Integer.numberOfTrailingZeros(blockSize);
            }
        }
        else
        {
            throw new IllegalArgumentException("blockSize must be >= " + minBlockSize + " and <= " + maxBlockSize + ", got " + blockSize);
        }
    }

    public static int numBlocks(long size, int blockSize)
    {
        if (size < 0)
            throw new IllegalArgumentException("size cannot be negative");

        int numBlocks = (int)(size / (long)blockSize) + (size % (long)blockSize == 0L ? 0 : 1);
        if ((long)numBlocks * (long)blockSize < size)
        {
            throw new IllegalArgumentException("size is too large for this block size");
        }
        else
        {
            return numBlocks;
        }
    }

    // Copied from Lucene BlockPackedReaderIterator as they are not public

    /**
     * Same as DataInput.readVLong but supports negative values
     */
    public static long readVLong(DataInput in) throws IOException
    {
        byte b = in.readByte();
        if (b >= 0) return b;
        long i = b & 0x7FL;
        b = in.readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0xFFL) << 56;
        return i;
    }

    public static void validateFooterAndResetPosition(IndexInput in) throws IOException
    {
        long position = in.getFilePointer();
        long fileLength = in.length();
        long footerLength = footerLength();
        long footerPosition = fileLength - footerLength;

        if (footerPosition < 0)
        {
            throw new CorruptIndexException("invalid codec footer (file truncated?): file length=" + fileLength + ", footer length=" + footerLength, in);
        }

        in.seek(footerPosition);
        validateFooter(in, false);
        in.seek(position);
    }

    /**
     * Copied from org.apache.lucene.codecs.CodecUtil.validateFooter(IndexInput).
     *
     * If the file is segmented then the footer can exist in the middle of the file
     * so, we shouldn't check that the footer size is correct, we just check that the
     * footer values are correct.
     */
    private static void validateFooter(IndexInput in, boolean segmented) throws IOException
    {
        long remaining = in.length() - in.getFilePointer();
        long expected = footerLength();

        if (!segmented)
        {
            if (remaining < expected)
            {
                throw new CorruptIndexException("misplaced codec footer (file truncated?): remaining=" + remaining + ", expected=" + expected + ", fp=" + in.getFilePointer(), in);
            }
            else if (remaining > expected)
            {
                throw new CorruptIndexException("misplaced codec footer (file extended?): remaining=" + remaining + ", expected=" + expected + ", fp=" + in.getFilePointer(), in);
            }
        }

        final int magic = readBEInt(in);

        if (magic != FOOTER_MAGIC)
        {
            throw new CorruptIndexException("codec footer mismatch (file truncated?): actual footer=" + magic + " vs expected footer=" + FOOTER_MAGIC, in);
        }

        final int algorithmID = readBEInt(in);

        if (algorithmID != 0)
        {
            throw new CorruptIndexException("codec footer mismatch: unknown algorithmID: " + algorithmID, in);
        }
    }

    // Copied from Lucene CodecUtil as they are not public

    /**
     * Writes checksum value as a 64-bit long to the output.
     * @throws IllegalStateException if CRC is formatted incorrectly (wrong bits set)
     * @throws IOException if an i/o error occurs
     */
    private static void writeChecksum(IndexOutput output) throws IOException
    {
        long value = output.getChecksum();
        if ((value & 0xFFFFFFFF00000000L) != 0)
        {
            throw new IllegalStateException("Illegal checksum: " + value + " (resource=" + output + ')');
        }
        writeBELong(output, value);
    }

    /**
     * Reads checksum value as a 64-bit long from the input.
     * @throws CorruptIndexException if CRC is formatted incorrectly (wrong bits set)
     * @throws IOException if an i/o error occurs
     */
    private static long readChecksum(IndexInput input) throws IOException
    {
        long value = readBELong(input);
        if ((value & 0xFFFFFFFF00000000L) != 0)
        {
            throw new CorruptIndexException("Illegal checksum: " + value, input);
        }
        return value;
    }
}
