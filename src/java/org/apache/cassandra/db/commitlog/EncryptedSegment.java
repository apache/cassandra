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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.crypto.Cipher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.security.EncryptionUtils.ENCRYPTED_BLOCK_HEADER_SIZE;

/**
 * Writes encrypted segments to disk. Data is compressed before encrypting to (hopefully) reduce the size of the data into
 * the encryption algorithms.
 *
 * The format of the encrypted commit log is as follows:
 * - standard commit log header (as written by {@link CommitLogDescriptor#writeHeader(ByteBuffer, CommitLogDescriptor)})
 * - a series of 'sync segments' that are written every time the commit log is sync()'ed
 * -- a sync section header, see {@link CommitLogSegment#writeSyncMarker(long, ByteBuffer, int, int, int)}
 * -- total plain text length for this section
 * -- a series of encrypted data blocks, each of which contains:
 * --- the length of the encrypted block (cipher text)
 * --- the length of the unencrypted data (compressed text)
 * --- the encrypted block, which contains:
 * ---- the length of the plain text (raw) data
 * ---- block of compressed data
 *
 * Notes:
 * - "length of the unencrypted data" is different from the length of resulting decrypted buffer as encryption adds padding
 * to the output buffer, and we need to ignore that padding when processing.
 */
public class EncryptedSegment extends FileDirectSegment
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSegment.class);

    private static final int ENCRYPTED_SECTION_HEADER_SIZE = SYNC_MARKER_SIZE + 4;

    private final EncryptionContext encryptionContext;
    private final Cipher cipher;

    public EncryptedSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager)
    {
        super(commitLog, manager);
        this.encryptionContext = commitLog.configuration.getEncryptionContext();

        try
        {
            cipher = encryptionContext.getEncryptor();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
        logger.debug("created a new encrypted commit log segment: {}", logFile);
        // Keep reusable buffers on-heap regardless of compression preference so we avoid copy off/on repeatedly during decryption
        manager.getBufferPool().setPreferredReusableBufferType(BufferType.ON_HEAP);
    }

    protected Map<String, String> additionalHeaderParameters()
    {
        Map<String, String> map = encryptionContext.toHeaderParameters();
        map.put(EncryptionContext.ENCRYPTION_IV, Hex.bytesToHex(cipher.getIV()));
        return map;
    }

    ByteBuffer createBuffer(CommitLog commitLog)
    {
        // Note: we want to keep the compression buffers on-heap as we need those bytes for encryption,
        // and we want to avoid copying from off-heap (compression buffer) to on-heap encryption APIs
        return manager.getBufferPool().createBuffer(BufferType.ON_HEAP);
    }

    void write(int startMarker, int nextMarker)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        final int length = nextMarker - contentStart;
        // The length may be 0 when the segment is being closed.
        assert length > 0 || length == 0 && !isStillAllocating();

        final ICompressor compressor = encryptionContext.getCompressor();
        final int blockSize = encryptionContext.getChunkLength();
        try
        {
            ByteBuffer inputBuffer = buffer.duplicate();
            inputBuffer.limit(contentStart + length).position(contentStart);
            ByteBuffer buffer = manager.getBufferPool().getThreadLocalReusableBuffer(DatabaseDescriptor.getCommitLogSegmentSize());

            // save space for the sync marker at the beginning of this section
            final long syncMarkerPosition = lastWrittenPos;
            channel.position(syncMarkerPosition + ENCRYPTED_SECTION_HEADER_SIZE);

            // loop over the segment data in encryption buffer sized chunks
            while (contentStart < nextMarker)
            {
                int nextBlockSize = nextMarker - blockSize > contentStart ? blockSize : nextMarker - contentStart;
                ByteBuffer slice = inputBuffer.duplicate();
                slice.limit(contentStart + nextBlockSize).position(contentStart);

                buffer = EncryptionUtils.compress(slice, buffer, true, compressor);

                // reuse the same buffer for the input and output of the encryption operation
                buffer = EncryptionUtils.encryptAndWrite(buffer, channel, true, cipher);

                contentStart += nextBlockSize;
                manager.addSize(buffer.limit() + ENCRYPTED_BLOCK_HEADER_SIZE);
            }

            lastWrittenPos = channel.position();

            // rewind to the beginning of the section and write out the sync marker
            buffer.position(0).limit(ENCRYPTED_SECTION_HEADER_SIZE);
            writeSyncMarker(id, buffer, 0, (int) syncMarkerPosition, (int) lastWrittenPos);
            buffer.putInt(SYNC_MARKER_SIZE, length);
            buffer.rewind();
            manager.addSize(buffer.limit());

            channel.position(syncMarkerPosition);
            channel.write(buffer);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public long onDiskSize()
    {
        return lastWrittenPos;
    }
}
