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

import org.apache.lucene.store.ChecksumIndexInput;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.File;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.mockito.Mockito;

import static org.apache.lucene.codecs.CodecUtil.CODEC_MAGIC;
import static org.apache.lucene.codecs.CodecUtil.FOOTER_MAGIC;
import static org.apache.lucene.codecs.CodecUtil.writeBEInt;
import static org.apache.lucene.codecs.CodecUtil.writeBELong;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class SAICodecUtilsTest extends SAIRandomizedTester
{
    private File file;

    @Before
    public void createFile() throws Exception
    {
        file = new File(temporaryFolder.newFile());
    }

    @Test
    public void checkHeaderDoesNotFailWithValidHeader() throws Exception
    {
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file))
        {
            SAICodecUtils.checkHeader(input);
        }
    }

    @Test
    public void checkHeaderFailsOnInvalidMagicValue() throws Exception
    {
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            writeBEInt(writer, 1234);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file))
        {
            assertThatThrownBy(() -> SAICodecUtils.checkHeader(input))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("codec header mismatch: actual header=1234 vs expected header=" + CODEC_MAGIC);
        }
    }

    @Test
    public void checkHeaderFailsOnInvalidVersion() throws Exception
    {
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            writeBEInt(writer, CODEC_MAGIC);
            writer.writeString("zz");
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file))
        {
            assertThatThrownBy(() -> SAICodecUtils.checkHeader(input))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The version string zz does not represent a valid SAI version. It should be one of aa");
        }
    }

    @Test
    public void checkFooterDoesNotFailWithValidFooter() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());
            SAICodecUtils.writeFooter(writer);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file);
             ChecksumIndexInput checksumIndexInput = IndexFileUtils.getBufferedChecksumIndexInput(input))
        {
            SAICodecUtils.checkHeader(checksumIndexInput);
            for (int value = 0; value < numBytes; value++)
                checksumIndexInput.readByte();
            SAICodecUtils.checkFooter(checksumIndexInput);
        }
    }

    @Test
    public void checkFooterFailsWithMissingFooter() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file);
             ChecksumIndexInput checksumIndexInput = IndexFileUtils.getBufferedChecksumIndexInput(input))
        {
            SAICodecUtils.checkHeader(checksumIndexInput);
            for (int value = 0; value < numBytes; value++)
                checksumIndexInput.readByte();
            assertThatThrownBy(() -> SAICodecUtils.checkFooter(checksumIndexInput))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("misplaced codec footer (file truncated?): remaining=0, expected=16");
        }
    }

    @Test
    public void checkFooterFailsWithExtendedFooter() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());
            SAICodecUtils.writeFooter(writer);
            writeBEInt(writer, 1234);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file);
             ChecksumIndexInput checksumIndexInput = IndexFileUtils.getBufferedChecksumIndexInput(input))
        {
            SAICodecUtils.checkHeader(checksumIndexInput);
            for (int value = 0; value < numBytes; value++)
                checksumIndexInput.readByte();
            assertThatThrownBy(() -> SAICodecUtils.checkFooter(checksumIndexInput))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("misplaced codec footer (file extended?): remaining=20, expected=16");
        }
    }

    @Test
    public void checkFooterFailsWithInvalidFooter() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());

            writeBEInt(writer, 1234);
            writeBEInt(writer, 0);
            writeBELong(writer, writer.getChecksum());
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file);
             ChecksumIndexInput checksumIndexInput = IndexFileUtils.getBufferedChecksumIndexInput(input))
        {
            SAICodecUtils.checkHeader(checksumIndexInput);
            for (int value = 0; value < numBytes; value++)
                checksumIndexInput.readByte();
            assertThatThrownBy(() -> SAICodecUtils.checkFooter(checksumIndexInput))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("codec footer mismatch (file truncated?): actual footer=1234 vs expected footer=-1071082520");
        }
    }

    @Test
    public void checkFooterFailsWithInvalidAlgorithmId() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());

            writeBEInt(writer, FOOTER_MAGIC);
            writeBEInt(writer, 1);
            writeBELong(writer, writer.getChecksum());
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file);
             ChecksumIndexInput checksumIndexInput = IndexFileUtils.getBufferedChecksumIndexInput(input))
        {
            SAICodecUtils.checkHeader(checksumIndexInput);
            for (int value = 0; value < numBytes; value++)
                checksumIndexInput.readByte();
            assertThatThrownBy(() -> SAICodecUtils.checkFooter(checksumIndexInput))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("codec footer mismatch: unknown algorithmID: 1 ");
        }
    }

    @Test
    public void checkFooterFailsWithInvalidChecksum() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());

            writeBEInt(writer, FOOTER_MAGIC);
            writeBEInt(writer, 0);
            writeBELong(writer, 0);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file);
             ChecksumIndexInput checksumIndexInput = IndexFileUtils.getBufferedChecksumIndexInput(input))
        {
            SAICodecUtils.checkHeader(checksumIndexInput);
            for (int value = 0; value < numBytes; value++)
                checksumIndexInput.readByte();
            assertThatThrownBy(() -> SAICodecUtils.checkFooter(checksumIndexInput))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("checksum failed (hardware problem?) : expected=0 actual=");
        }
    }

    @Test
    public void checkFooterFailsWithIllegalChecksum() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());

            writeBEInt(writer, FOOTER_MAGIC);
            writeBEInt(writer, 0);
            writeBELong(writer, 0xFFFFFFFF00000000L);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file);
             ChecksumIndexInput checksumIndexInput = IndexFileUtils.getBufferedChecksumIndexInput(input))
        {
            SAICodecUtils.checkHeader(checksumIndexInput);
            for (int value = 0; value < numBytes; value++)
                checksumIndexInput.readByte();
            assertThatThrownBy(() -> SAICodecUtils.checkFooter(checksumIndexInput))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("Illegal checksum: -4294967296 ");
        }
    }

    @Test
    public void validateFooterAndResetPositionFailsWithShortFile() throws Exception
    {
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file))
        {
            assertThatThrownBy(() -> SAICodecUtils.validateFooterAndResetPosition(input))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("invalid codec footer (file truncated?): file length=7, footer length=16 ");
        }
    }

    @Test
    public void validateChecksumFailsWithInvalidChecksum() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());

            writeBEInt(writer, FOOTER_MAGIC);
            writeBEInt(writer, 0);
            writeBELong(writer, 0);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file))
        {
            SAICodecUtils.checkHeader(input);
            for (int value = 0; value < numBytes; value++)
                input.readByte();
            assertThatThrownBy(() -> SAICodecUtils.validateChecksum(input))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("checksum failed (hardware problem?) : expected=0 actual=");
        }
    }

    @Test
    public void validateChecksumFailsWithIllegalChecksum() throws Exception
    {
        int numBytes = nextInt(1000, 10000);
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file))
        {
            SAICodecUtils.writeHeader(writer);
            for (int value = 0; value < numBytes; value++)
                writer.writeByte(getRandom().nextByte());

            writeBEInt(writer, FOOTER_MAGIC);
            writeBEInt(writer, 0);
            writeBELong(writer, 0xFFFFFFFF00000000L);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(file))
        {
            SAICodecUtils.checkHeader(input);
            for (int value = 0; value < numBytes; value++)
                input.readByte();
            assertThatThrownBy(() -> SAICodecUtils.validateChecksum(input))
            .isInstanceOf(CorruptIndexException.class)
            .hasMessageContaining("Illegal checksum: -4294967296 ");
        }
    }

    @Test
    public void writeCRCFailsWithInvalidCRC() throws Exception
    {
        IndexOutput indexOutput = Mockito.mock(IndexOutput.class);
        when(indexOutput.getChecksum()).thenReturn(0xFFFFFFFF00000000L);
        assertThatThrownBy(() -> SAICodecUtils.writeFooter(indexOutput))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Illegal checksum: -4294967296 ");
    }

    @Test
    public void checkBlockSizeTest()
    {
        // Block size must be within min and max
        assertThatThrownBy(() -> SAICodecUtils.checkBlockSize(1024, 32, 512))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blockSize must be >= 32 and <= 512, got 1024");

        // Block size must be power of 2
        assertThatThrownBy(() -> SAICodecUtils.checkBlockSize(99, 32, 512))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blockSize must be a power of two, got 99");

        // Check block shift size is correct
        assertEquals(6, SAICodecUtils.checkBlockSize(64, 32, 512));
        assertEquals(7, SAICodecUtils.checkBlockSize(128, 32, 512));
        assertEquals(8, SAICodecUtils.checkBlockSize(256, 32, 512));
    }

    @Test
    public void numBlocksTest()
    {
        assertEquals(0, SAICodecUtils.numBlocks(0, 512));
        assertEquals(1, SAICodecUtils.numBlocks(500, 512));
        assertEquals(2, SAICodecUtils.numBlocks(1000, 512));
        assertEquals(3, SAICodecUtils.numBlocks(1025, 512));

        assertThatThrownBy(() -> SAICodecUtils.numBlocks(-1, 512))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("size cannot be negative");

        // This will integer overflow the number of blocks and result in an error
        assertThatThrownBy(() -> SAICodecUtils.numBlocks((long)Integer.MAX_VALUE * 512L + 1L, 512))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("size is too large for this block size");
    }
}
