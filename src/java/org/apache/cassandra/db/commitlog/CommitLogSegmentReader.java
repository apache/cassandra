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
import java.util.Iterator;
import java.util.zip.CRC32;
import javax.crypto.Cipher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.commitlog.EncryptedFileSegmentInputStream.ChunkProvider;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.*;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.SYNC_MARKER_SIZE;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Read each sync section of a commit log, iteratively.
 */
public class CommitLogSegmentReader implements Iterable<CommitLogSegmentReader.SyncSegment>
{
    private final CommitLogReadHandler handler;
    private final CommitLogDescriptor descriptor;
    private final RandomAccessReader reader;
    private final Segmenter segmenter;
    private final boolean tolerateTruncation;

    /**
     * ending position of the current sync section.
     */
    protected int end;

    protected CommitLogSegmentReader(CommitLogReadHandler handler,
                                     CommitLogDescriptor descriptor,
                                     RandomAccessReader reader,
                                     boolean tolerateTruncation)
    {
        this.handler = handler;
        this.descriptor = descriptor;
        this.reader = reader;
        this.tolerateTruncation = tolerateTruncation;

        end = (int) reader.getFilePointer();
        if (descriptor.getEncryptionContext().isEnabled())
            segmenter = new EncryptedSegmenter(descriptor, reader);
        else if (descriptor.compression != null)
            segmenter = new CompressedSegmenter(descriptor, reader);
        else
            segmenter = new NoOpSegmenter(reader);
    }

    public Iterator<SyncSegment> iterator()
    {
        return new SegmentIterator();
    }

    protected class SegmentIterator extends AbstractIterator<CommitLogSegmentReader.SyncSegment>
    {
        protected SyncSegment computeNext()
        {
            while (true)
            {
                try
                {
                    final int currentStart = end;
                    end = readSyncMarker(descriptor, currentStart, reader);
                    if (end == -1)
                    {
                        return endOfData();
                    }
                    if (end > reader.length())
                    {
                        // the CRC was good (meaning it was good when it was written and still looks legit), but the file is truncated now.
                        // try to grab and use as much of the file as possible, which might be nothing if the end of the file truly is corrupt
                        end = (int) reader.length();
                    }
                    return segmenter.nextSegment(currentStart + SYNC_MARKER_SIZE, end);
                }
                catch(CommitLogSegmentReader.SegmentReadException e)
                {
                    try
                    {
                        handler.handleUnrecoverableError(new CommitLogReadException(
                                                    e.getMessage(),
                                                    CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                                    !e.invalidCrc && tolerateTruncation));
                    }
                    catch (IOException ioe)
                    {
                        throw new RuntimeException(ioe);
                    }
                }
                catch (IOException e)
                {
                    try
                    {
                        boolean tolerateErrorsInSection = tolerateTruncation & segmenter.tolerateSegmentErrors(end, reader.length());
                        // if no exception is thrown, the while loop will continue
                        handler.handleUnrecoverableError(new CommitLogReadException(
                                                    e.getMessage(),
                                                    CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                                    tolerateErrorsInSection));
                    }
                    catch (IOException ioe)
                    {
                        throw new RuntimeException(ioe);
                    }
                }
            }
        }
    }

    private int readSyncMarker(CommitLogDescriptor descriptor, int offset, RandomAccessReader reader) throws IOException
    {
        if (offset > reader.length() - SYNC_MARKER_SIZE)
        {
            // There was no room in the segment to write a final header. No data could be present here.
            return -1;
        }
        reader.seek(offset);
        CRC32 crc = new CRC32();
        updateChecksumInt(crc, (int) (descriptor.id & 0xFFFFFFFFL));
        updateChecksumInt(crc, (int) (descriptor.id >>> 32));
        updateChecksumInt(crc, (int) reader.getPosition());
        final int end = reader.readInt();
        long filecrc = reader.readInt() & 0xffffffffL;
        if (crc.getValue() != filecrc)
        {
            if (end != 0 || filecrc != 0)
            {
                String msg = String.format("Encountered bad header at position %d of commit log %s, with invalid CRC. " +
                             "The end of segment marker should be zero.", offset, reader.getPath());
                throw new SegmentReadException(msg, true);
            }
            return -1;
        }
        else if (end < offset || end > reader.length())
        {
            String msg = String.format("Encountered bad header at position %d of commit log %s, with bad position but valid CRC", offset, reader.getPath());
            throw new SegmentReadException(msg, false);
        }
        return end;
    }

    public static class SegmentReadException extends IOException
    {
        public final boolean invalidCrc;

        public SegmentReadException(String msg, boolean invalidCrc)
        {
            super(msg);
            this.invalidCrc = invalidCrc;
        }
    }

    public static class SyncSegment
    {
        /** the 'buffer' to replay commit log data from */
        public final FileDataInput input;

        /** offset in file where this section begins. */
        public final int fileStartPosition;

        /** offset in file where this section ends. */
        public final int fileEndPosition;

        /** the logical ending position of the buffer */
        public final int endPosition;

        public final boolean toleratesErrorsInSection;

        public SyncSegment(FileDataInput input, int fileStartPosition, int fileEndPosition, int endPosition, boolean toleratesErrorsInSection)
        {
            this.input = input;
            this.fileStartPosition = fileStartPosition;
            this.fileEndPosition = fileEndPosition;
            this.endPosition = endPosition;
            this.toleratesErrorsInSection = toleratesErrorsInSection;
        }
    }

    /**
     * Derives the next section of the commit log to be replayed. Section boundaries are derived from the commit log sync markers.
     */
    interface Segmenter
    {
        /**
         * Get the next section of the commit log to replay.
         *
         * @param startPosition the position in the file to begin reading at
         * @param nextSectionStartPosition the file position of the beginning of the next section
         * @return the buffer and it's logical end position
         * @throws IOException
         */
        SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) throws IOException;

        /**
         * Determine if we tolerate errors in the current segment.
         */
        default boolean tolerateSegmentErrors(int segmentEndPosition, long fileLength)
        {
            return segmentEndPosition >= fileLength || segmentEndPosition < 0;
        }
    }

    static class NoOpSegmenter implements Segmenter
    {
        private final RandomAccessReader reader;

        public NoOpSegmenter(RandomAccessReader reader)
        {
            this.reader = reader;
        }

        public SyncSegment nextSegment(int startPosition, int nextSectionStartPosition)
        {
            reader.seek(startPosition);
            return new SyncSegment(reader, startPosition, nextSectionStartPosition, nextSectionStartPosition, true);
        }

        public boolean tolerateSegmentErrors(int end, long length)
        {
            return true;
        }
    }

    static class CompressedSegmenter implements Segmenter
    {
        private final ICompressor compressor;
        private final RandomAccessReader reader;
        private byte[] compressedBuffer;
        private byte[] uncompressedBuffer;
        private long nextLogicalStart;

        public CompressedSegmenter(CommitLogDescriptor desc, RandomAccessReader reader)
        {
            this(CompressionParams.createCompressor(desc.compression), reader);
        }

        public CompressedSegmenter(ICompressor compressor, RandomAccessReader reader)
        {
            this.compressor = compressor;
            this.reader = reader;
            compressedBuffer = new byte[0];
            uncompressedBuffer = new byte[0];
            nextLogicalStart = reader.getFilePointer();
        }

        @SuppressWarnings("resource")
        public SyncSegment nextSegment(final int startPosition, final int nextSectionStartPosition) throws IOException
        {
            reader.seek(startPosition);
            int uncompressedLength = reader.readInt();

            int compressedLength = nextSectionStartPosition - (int)reader.getPosition();
            if (compressedLength > compressedBuffer.length)
                compressedBuffer = new byte[(int) (1.2 * compressedLength)];
            reader.readFully(compressedBuffer, 0, compressedLength);

            if (uncompressedLength > uncompressedBuffer.length)
               uncompressedBuffer = new byte[(int) (1.2 * uncompressedLength)];
            int count = compressor.uncompress(compressedBuffer, 0, compressedLength, uncompressedBuffer, 0);
            nextLogicalStart += SYNC_MARKER_SIZE;
            FileDataInput input = new FileSegmentInputStream(ByteBuffer.wrap(uncompressedBuffer, 0, count), reader.getPath(), nextLogicalStart);
            nextLogicalStart += uncompressedLength;
            return new SyncSegment(input, startPosition, nextSectionStartPosition, (int)nextLogicalStart, tolerateSegmentErrors(nextSectionStartPosition, reader.length()));
        }
    }

    static class EncryptedSegmenter implements Segmenter
    {
        private final RandomAccessReader reader;
        private final ICompressor compressor;
        private final Cipher cipher;

        /**
         * the result of the decryption is written into this buffer.
         */
        private ByteBuffer decryptedBuffer;

        /**
         * the result of the decryption is written into this buffer.
         */
        private ByteBuffer uncompressedBuffer;

        private final ChunkProvider chunkProvider;

        private long currentSegmentEndPosition;
        private long nextLogicalStart;

        public EncryptedSegmenter(CommitLogDescriptor descriptor, RandomAccessReader reader)
        {
            this(reader, descriptor.getEncryptionContext());
        }

        @VisibleForTesting
        EncryptedSegmenter(final RandomAccessReader reader, EncryptionContext encryptionContext)
        {
            this.reader = reader;
            decryptedBuffer = ByteBuffer.allocate(0);
            compressor = encryptionContext.getCompressor();
            nextLogicalStart = reader.getFilePointer();

            try
            {
                cipher = encryptionContext.getDecryptor();
            }
            catch (IOException ioe)
            {
                throw new FSReadError(ioe, reader.getPath());
            }

            chunkProvider = () -> {
                if (reader.getFilePointer() >= currentSegmentEndPosition)
                    return ByteBufferUtil.EMPTY_BYTE_BUFFER;
                try
                {
                    decryptedBuffer = EncryptionUtils.decrypt(reader, decryptedBuffer, true, cipher);
                    uncompressedBuffer = EncryptionUtils.uncompress(decryptedBuffer, uncompressedBuffer, true, compressor);
                    return uncompressedBuffer;
                }
                catch (IOException e)
                {
                    throw new FSReadError(e, reader.getPath());
                }
            };
        }

        @SuppressWarnings("resource")
        public SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) throws IOException
        {
            int totalPlainTextLength = reader.readInt();
            currentSegmentEndPosition = nextSectionStartPosition - 1;

            nextLogicalStart += SYNC_MARKER_SIZE;
            FileDataInput input = new EncryptedFileSegmentInputStream(reader.getPath(), nextLogicalStart, 0, totalPlainTextLength, chunkProvider);
            nextLogicalStart += totalPlainTextLength;
            return new SyncSegment(input, startPosition, nextSectionStartPosition, (int)nextLogicalStart, tolerateSegmentErrors(nextSectionStartPosition, reader.length()));
        }
    }
}
