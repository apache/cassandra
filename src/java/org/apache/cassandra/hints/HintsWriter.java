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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.utils.FBUtilities.updateChecksum;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;
import static org.apache.cassandra.utils.Throwables.perform;

class HintsWriter implements AutoCloseable
{
    static final int PAGE_SIZE = 4096;

    private final File directory;
    private final HintsDescriptor descriptor;
    private final File file;
    protected final FileChannel channel;
    private final int fd;
    protected final CRC32 globalCRC;

    private volatile long lastSyncPosition = 0L;

    protected HintsWriter(File directory, HintsDescriptor descriptor, File file, FileChannel channel, int fd, CRC32 globalCRC)
    {
        this.directory = directory;
        this.descriptor = descriptor;
        this.file = file;
        this.channel = channel;
        this.fd = fd;
        this.globalCRC = globalCRC;
    }

    static HintsWriter create(File directory, HintsDescriptor descriptor) throws IOException
    {
        File file = descriptor.file(directory);

        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        int fd = NativeLibrary.getfd(channel);

        CRC32 crc = new CRC32();

        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            // write the descriptor
            descriptor.serialize(dob);
            ByteBuffer descriptorBytes = dob.unsafeGetBufferAndFlip();
            updateChecksum(crc, descriptorBytes);
            channel.write(descriptorBytes);

            if (descriptor.isEncrypted())
                return new EncryptedHintsWriter(directory, descriptor, file, channel, fd, crc);
            if (descriptor.isCompressed())
                return new CompressedHintsWriter(directory, descriptor, file, channel, fd, crc);
            return new HintsWriter(directory, descriptor, file, channel, fd, crc);
        }
        catch (Throwable e)
        {
            channel.close();
            throw e;
        }
    }

    HintsDescriptor descriptor()
    {
        return descriptor;
    }

    private void writeChecksum()
    {
        File checksumFile = descriptor.checksumFile(directory);
        try (OutputStream out = Files.newOutputStream(checksumFile.toPath()))
        {
            out.write(Integer.toHexString((int) globalCRC.getValue()).getBytes(StandardCharsets.UTF_8));
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, checksumFile);
        }
    }

    public void close()
    {
        perform(file, Throwables.FileOpType.WRITE, this::doFsync, channel::close);

        writeChecksum();
    }

    public void fsync()
    {
        perform(file, Throwables.FileOpType.WRITE, this::doFsync);
    }

    private void doFsync() throws IOException
    {
        SyncUtil.force(channel, true);
        lastSyncPosition = channel.position();
    }

    Session newSession(ByteBuffer buffer)
    {
        try
        {
            return new Session(buffer, channel.size());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
    }

    @VisibleForTesting
    File getFile()
    {
        return file;
    }

    /**
     * Writes byte buffer into the file channel. Buffer should be flipped before calling this
     */
    protected void writeBuffer(ByteBuffer bb) throws IOException
    {
        updateChecksum(globalCRC, bb);
        channel.write(bb);
    }

    /**
     * The primary goal of the Session class is to be able to share the same buffers among potentially dozens or hundreds
     * of hints writers, and ensure that their contents are always written to the underlying channels in the end.
     */
    final class Session implements AutoCloseable
    {
        private final ByteBuffer buffer;

        private final long initialSize;
        private long bytesWritten;

        Session(ByteBuffer buffer, long initialSize)
        {
            buffer.clear();
            bytesWritten = 0L;

            this.buffer = buffer;
            this.initialSize = initialSize;
        }

        @VisibleForTesting
        long getBytesWritten()
        {
            return bytesWritten;
        }

        long position()
        {
            return initialSize + bytesWritten;
        }

        /**
         * Appends the serialized hint (with CRC included) to this session's aggregation buffer,
         * writes to the underlying channel when the buffer is overflown.
         *
         * @param hint the serialized hint (with CRC included)
         * @throws IOException
         */
        void append(ByteBuffer hint) throws IOException
        {
            bytesWritten += hint.remaining();

            // if the hint to write won't fit in the aggregation buffer, flush it
            if (hint.remaining() > buffer.remaining())
            {
                buffer.flip();
                writeBuffer(buffer);
                buffer.clear();
            }

            // if the hint fits in the aggregation buffer, then update the aggregation buffer,
            // otherwise write the hint buffer to the channel
            if (hint.remaining() <= buffer.remaining())
            {
                buffer.put(hint);
            }
            else
            {
                writeBuffer(hint);
            }
        }

        /**
         * Serializes and appends the hint (with CRC included) to this session's aggregation buffer,
         * writes to the underlying channel when the buffer is overflown.
         *
         * Used mainly by tests and {@link LegacyHintsMigrator}
         *
         * @param hint the unserialized hint
         * @throws IOException
         */
        void append(Hint hint) throws IOException
        {
            int hintSize = (int) Hint.serializer.serializedSize(hint, descriptor.messagingVersion());
            int totalSize = hintSize + HintsBuffer.ENTRY_OVERHEAD_SIZE;

            if (totalSize > buffer.remaining())
                flushBuffer();

            ByteBuffer hintBuffer = totalSize <= buffer.remaining()
                                  ? buffer
                                  : ByteBuffer.allocate(totalSize);

            CRC32 crc = new CRC32();
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(hintBuffer))
            {
                out.writeInt(hintSize);
                updateChecksumInt(crc, hintSize);
                out.writeInt((int) crc.getValue());

                long startPosition = out.position();
                Hint.serializer.serialize(hint, out, descriptor.messagingVersion());
                long actualSize = out.position() - startPosition;
                checkState(actualSize == hintSize, "Serialized hint size doesn't match calculated hint size");
                updateChecksum(crc, hintBuffer, hintBuffer.position() - hintSize, hintSize);
                out.writeInt((int) crc.getValue());
            }

            if (hintBuffer == buffer)
                bytesWritten += totalSize;
            else
                append((ByteBuffer) hintBuffer.flip());
        }

        /**
         * Closes the session - flushes the aggregation buffer (if not empty), does page aligning, and potentially fsyncs.
         * @throws IOException
         */
        public void close() throws IOException
        {
            flushBuffer();
            maybeFsync();
            maybeSkipCache();
        }

        private void flushBuffer() throws IOException
        {
            buffer.flip();

            if (buffer.remaining() > 0)
            {
                writeBuffer(buffer);
            }

            buffer.clear();
        }

        private void maybeFsync()
        {
            if (position() >= lastSyncPosition + DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024L)
                fsync();
        }

        private void maybeSkipCache()
        {
            long position = position();

            // don't skip page cache for tiny files, on the assumption that if they are tiny, the target node is probably
            // alive, and if so, the file will be closed and dispatched shortly (within a minute), and the file will be dropped.
            if (position >= DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024L)
                NativeLibrary.trySkipCache(fd, 0, position - (position % PAGE_SIZE), file.path());
        }
    }
}
