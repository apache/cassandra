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
package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.io.util.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Random;
import java.util.function.BiFunction;

import javax.crypto.Cipher;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogSegmentReader.CompressedSegmenter;
import org.apache.cassandra.db.commitlog.CommitLogSegmentReader.EncryptedSegmenter;
import org.apache.cassandra.db.commitlog.CommitLogSegmentReader.SyncSegment;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.security.CipherFactory;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SegmentReaderTest
{
    static final Random random = new Random();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void compressedSegmenter_LZ4() throws IOException
    {
        compressedSegmenter(LZ4Compressor.create(Collections.emptyMap()));
    }

    @Test
    public void compressedSegmenter_Snappy() throws IOException
    {
        compressedSegmenter(SnappyCompressor.create(null));
    }

    @Test
    public void compressedSegmenter_Deflate() throws IOException
    {
        compressedSegmenter(DeflateCompressor.create(null));
    }

    @Test
    public void compressedSegmenter_Zstd() throws IOException
    {
        compressedSegmenter(ZstdCompressor.create(Collections.emptyMap()));
    }

    private void compressedSegmenter(ICompressor compressor) throws IOException
    {
        int rawSize = (1 << 15) - 137;
        ByteBuffer plainTextBuffer = compressor.preferredBufferType().allocate(rawSize);
        byte[] b = new byte[rawSize];
        random.nextBytes(b);
        plainTextBuffer.put(b);
        plainTextBuffer.flip();

        int uncompressedHeaderSize = 4;  // need to add in the plain text size to the block we write out
        int length = compressor.initialCompressedBufferLength(rawSize);
        ByteBuffer compBuffer = ByteBufferUtil.ensureCapacity(null, length + uncompressedHeaderSize, true, compressor.preferredBufferType());
        compBuffer.putInt(rawSize);
        compressor.compress(plainTextBuffer, compBuffer);
        compBuffer.flip();

        File compressedFile = FileUtils.createTempFile("compressed-segment-", ".log");
        compressedFile.deleteOnExit();
        FileOutputStreamPlus fos = new FileOutputStreamPlus(compressedFile);
        fos.getChannel().write(compBuffer);
        fos.close();

        try (RandomAccessReader reader = RandomAccessReader.open(compressedFile))
        {
            CompressedSegmenter segmenter = new CompressedSegmenter(compressor, reader);
            int fileLength = (int) compressedFile.length();
            SyncSegment syncSegment = segmenter.nextSegment(0, fileLength);
            FileDataInput fileDataInput = syncSegment.input;
            ByteBuffer fileBuffer = readBytes(fileDataInput, rawSize);

            plainTextBuffer.flip();
            Assert.assertEquals(plainTextBuffer, fileBuffer);

            // CompressedSegmenter includes the Sync header length in the syncSegment.endPosition (value)
            Assert.assertEquals(rawSize, syncSegment.endPosition - CommitLogSegment.SYNC_MARKER_SIZE);
        }
    }

    private ByteBuffer readBytes(FileDataInput input, int len)
    {
        byte[] buf = new byte[len];
        try
        {
            input.readFully(buf);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return ByteBuffer.wrap(buf);
    }

    private ByteBuffer readBytesSeek(FileDataInput input, int len)
    {
        byte[] buf = new byte[len];

        /// divide output buffer into 5
        int[] offsets = new int[] { 0, len / 5, 2 * len / 5, 3 * len / 5, 4 * len / 5, len };
        
        //seek offset
        long inputStart = input.getFilePointer();

        for (int i = 0; i < offsets.length - 1; i++)
        {
            try
            {
                // seek to beginning of offet
                input.seek(inputStart + offsets[i]);
                //read this segment
                input.readFully(buf, offsets[i], offsets[i + 1] - offsets[i]);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        return ByteBuffer.wrap(buf);
    }

    @Test
    public void encryptedSegmenterRead() throws IOException
    {
        underlyingEncryptedSegmenterTest((s, t) -> readBytes(s, t));
    }

    @Test
    public void encryptedSegmenterSeek() throws IOException
    {
        underlyingEncryptedSegmenterTest((s, t) -> readBytesSeek(s, t));
    }

    public void underlyingEncryptedSegmenterTest(BiFunction<FileDataInput, Integer, ByteBuffer> readFun)
            throws IOException
    {
        EncryptionContext context = EncryptionContextGenerator.createContext(true);
        CipherFactory cipherFactory = new CipherFactory(context.getTransparentDataEncryptionOptions());

        int plainTextLength = (1 << 13) - 137;
        ByteBuffer plainTextBuffer = ByteBuffer.allocate(plainTextLength);
        random.nextBytes(plainTextBuffer.array());

        ByteBuffer compressedBuffer = EncryptionUtils.compress(plainTextBuffer, null, true, context.getCompressor());
        Cipher cipher = cipherFactory.getEncryptor(context.getTransparentDataEncryptionOptions().cipher, context.getTransparentDataEncryptionOptions().key_alias);
        File encryptedFile = FileUtils.createTempFile("encrypted-segment-", ".log");
        encryptedFile.deleteOnExit();
        FileChannel channel = encryptedFile.newReadWriteChannel();
        channel.write(ByteBufferUtil.bytes(plainTextLength));
        EncryptionUtils.encryptAndWrite(compressedBuffer, channel, true, cipher);
        channel.close();

        try (RandomAccessReader reader = RandomAccessReader.open(encryptedFile))
        {
            context = EncryptionContextGenerator.createContext(cipher.getIV(), true);
            EncryptedSegmenter segmenter = new EncryptedSegmenter(reader, context);
            SyncSegment syncSegment = segmenter.nextSegment(0, (int) reader.length());

            // EncryptedSegmenter includes the Sync header length in the syncSegment.endPosition (value)
            Assert.assertEquals(plainTextLength, syncSegment.endPosition - CommitLogSegment.SYNC_MARKER_SIZE);
            ByteBuffer fileBuffer = readFun.apply(syncSegment.input, plainTextLength);
            plainTextBuffer.position(0);
            Assert.assertEquals(plainTextBuffer, fileBuffer);
        }
    }
}
