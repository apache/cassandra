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
package org.apache.cassandra.io.compress;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.OpenOption;
import java.util.Optional;
import java.util.zip.CRC32;

import javax.annotation.Nullable;

import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChecksumWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.utils.Throwables.merge;

public class CompressedSequentialWriter extends SequentialWriter
{
    protected final ChecksumWriter crcMetadata;

    // holds offset in the file where current chunk should be written
    // changed only by flush() method where data buffer gets compressed and stored to the file
    protected long chunkOffset = 0;

    // index file writer (random I/O)
    protected final CompressionMetadata.Writer metadataWriter;
    private final ICompressor compressor;

    // used to store compressed data
    private ByteBuffer compressed;

    // holds a number of already written chunks
    protected int chunkCount = 0;

    protected long uncompressedSize = 0;
    protected long compressedSize = 0;

    protected final MetadataCollector sstableMetadataCollector;
    private final CompressionDictionaryManager compressionDictionaryManager;

    private final ByteBuffer crcCheckBuffer = ByteBuffer.allocate(4);
    protected final Optional<File> digestFile;

    private final int maxCompressedLength;
    private final boolean isDictionaryEnabled;

    private static ByteBuffer allocateBuffer(CompressionParams parameters)
    {
        return parameters.getSstableCompressor().preferredBufferType().allocate(parameters.chunkLength());
    }

    private static SequentialWriterOption buildOption(SequentialWriterOption option, CompressionParams parameters)
    {
        return SequentialWriterOption.newBuilder()
                                     .bufferSize(parameters.chunkLength())
                                     .bufferType(parameters.getSstableCompressor().preferredBufferType())
                                     .trickleFsync(option.trickleFsync())
                                     .trickleFsyncByteInterval(option.trickleFsyncByteInterval())
                                     .finishOnClose(option.finishOnClose())
                                     .build();
    }

    public CompressedSequentialWriter(File file,
                                      File offsetsFile,
                                      @Nullable File digestFile,
                                      SequentialWriterOption option,
                                      CompressionParams parameters,
                                      MetadataCollector sstableMetadataCollector)
    {
        this(file, offsetsFile, digestFile, option, parameters, sstableMetadataCollector, null);
    }

    /**
     * Create CompressedSequentialWriter with optional compression dictionary and channel options.
     *
     * @param file File to write
     * @param offsetsFile File to write compression metadata
     * @param digestFile File to write digest, or null if not needed
     * @param option Write option (buffer size and type will be set the same as compression params)
     * @param parameters Compression parameters
     * @param sstableMetadataCollector Metadata collector
     * @param compressionDictionaryManager manages compression dictionary; null if absent
     * @param extraOpenOptions additional options to pass to FileChannel.open (e.g., ExtendedOpenOption.DIRECT)
     */
    public CompressedSequentialWriter(File file,
                                      File offsetsFile,
                                      @Nullable File digestFile,
                                      SequentialWriterOption option,
                                      CompressionParams parameters,
                                      MetadataCollector sstableMetadataCollector,
                                      @Nullable CompressionDictionaryManager compressionDictionaryManager,
                                      OpenOption... extraOpenOptions)
    {
        super(file, allocateBuffer(parameters), buildOption(option, parameters), true, extraOpenOptions);
        ICompressor compressor = parameters.getSstableCompressor();
        this.digestFile = Optional.ofNullable(digestFile);

        // buffer for compression should be the same size as buffer itself
        compressed = compressor.preferredBufferType().allocate(compressor.initialCompressedBufferLength(buffer.capacity()));

        maxCompressedLength = parameters.maxCompressedLength();

        // Note that we cannot rely on the compressor type to tell whether dictionary compression is enabled.
        // Because the `CompressionParams` for this method is updated at the callsite, `DataComponent.buildWriter`.
        // See CASSANDRA-15379 for details regarding the optimization.
        // Meanwhile, as long as dictionary-based compression is enabled, we want to collect samples.
        this.isDictionaryEnabled = compressionDictionaryManager != null && compressionDictionaryManager.isEnabled();

        CompressionDictionary compressionDictionary = compressionDictionaryManager == null ? null : compressionDictionaryManager.getCurrent();
        if (compressionDictionary != null && compressor instanceof IDictionaryCompressor)
        {
            compressor = ((IDictionaryCompressor) compressor).getOrCopyWithDictionary(compressionDictionary);
        }
        else
        {
            // It is likely on the sstable flushing path and LZ4 compressor or something else is picked.
            // In this case, we disable the compression dictionary, i.e. do not attach the dictionary
            // bytes to the CompressionInfo component.
            compressionDictionary = null;
        }
        this.compressor = compressor;
        this.compressionDictionaryManager = compressionDictionaryManager;
        /* Index File (-CompressionInfo.db component) and it's header */
        metadataWriter = CompressionMetadata.Writer.open(parameters, offsetsFile, compressionDictionary);

        this.sstableMetadataCollector = sstableMetadataCollector;
        crcMetadata = createChecksumWriter();
    }

    /**
     * Creates the {@link ChecksumWriter} for the chunk and full-file checksums. Invoked from the constructor,
     * so overrides must not read subclass fields.
     */
    protected ChecksumWriter createChecksumWriter()
    {
        return new ChecksumWriter(new DataOutputStream(Channels.newOutputStream(channel)));
    }

    @Override
    public long getOnDiskFilePointer()
    {
        try
        {
            return fchannel.position();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    /**
     * Get a quick estimation on how many bytes have been written to disk
     *
     * It should for the most part be exactly the same as getOnDiskFilePointer()
     */
    @Override
    public long getEstimatedOnDiskBytesWritten()
    {
        return chunkOffset;
    }

    @Override
    public void flush()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void flushData()
    {
        // resetAndTruncate leaves fchannel.position() past EOF after its verification reads + truncate;
        // re-seek so the next chunk lands at chunkOffset. No-op under linear writes.
        seekToChunkStart();

        try
        {
            // compressing data with buffer re-use
            buffer.flip();
            compressed.clear();
            compressor.compress(buffer, compressed);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Compression exception", e); // shouldn't happen
        }

        int uncompressedLength = buffer.position();
        int compressedLength = compressed.position();
        uncompressedSize += uncompressedLength;
        ByteBuffer toWrite = compressed;
        if (compressedLength >= maxCompressedLength)
        {
            toWrite = buffer;
            if (uncompressedLength >= maxCompressedLength)
            {
                compressedLength = uncompressedLength;
            }
            else
            {
                // Pad the uncompressed data so that it reaches the max compressed length.
                // This could make the chunk appear longer, but this path is only reached at the end of the file, where
                // we use the file size to limit the buffer on reading.
                assert maxCompressedLength <= buffer.capacity();   // verified by CompressionParams.validate
                buffer.limit(maxCompressedLength);
                ByteBufferUtil.writeZeroes(buffer, maxCompressedLength - uncompressedLength);
                compressedLength = maxCompressedLength;
            }
        }
        compressedSize += compressedLength;

        // write an offset of the newly written chunk to the index file
        metadataWriter.addOffset(chunkOffset);
        chunkCount++;

        // write out the compressed data and checksum
        toWrite.flip();
        writeChunk(toWrite);
        lastFlushOffset = uncompressedSize;

        if (toWrite == buffer)
            buffer.position(uncompressedLength);

        // next chunk should be written right after current + length of the checksum (int)
        chunkOffset += compressedLength + 4;
        if (runPostFlush != null)
            runPostFlush.accept(getLastFlushOffset());
    }

    protected void writeChunk(ByteBuffer toWrite)
    {
        try
        {
            channel.write(toWrite);
            toWrite.rewind();
            crcMetadata.appendDirect(toWrite, true);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public CompressionMetadata open(long overrideLength)
    {
        if (overrideLength <= 0)
            overrideLength = uncompressedSize;
        return metadataWriter.open(overrideLength, chunkOffset);
    }

    @Override
    public DataPosition mark()
    {
        if (!buffer.hasRemaining())
            doFlush(0);
        return new CompressedFileWriterMark(chunkOffset, current(), buffer.position(), chunkCount + 1);
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        assert mark instanceof CompressedFileWriterMark;

        CompressedFileWriterMark realMark = (CompressedFileWriterMark) mark;

        // reset position
        long truncateTarget = realMark.uncDataOffset;

        if (realMark.chunkOffset == chunkOffset)
        {
            // simply drop bytes to the right of our mark
            buffer.position(realMark.validBufferBytes);
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        syncInternal();

        chunkOffset = realMark.chunkOffset;

        // compressed chunk size (- 4 bytes reserved for checksum)
        int chunkSize = (int) (metadataWriter.chunkOffsetBy(realMark.nextChunkIndex) - chunkOffset - 4);
        if (compressed.capacity() < chunkSize)
        {
            MemoryUtil.clean(compressed);
            compressed = compressor.preferredBufferType().allocate(chunkSize);
        }

        try
        {
            compressed.clear();
            compressed.limit(chunkSize);
            fchannel.position(chunkOffset);
            fchannel.read(compressed);

            try
            {
                // Repopulate buffer from compressed data
                buffer.clear();
                compressed.flip();
                if (chunkSize < maxCompressedLength)
                    compressor.uncompress(compressed, buffer);
                else
                    buffer.put(compressed);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize, e);
            }

            CRC32 checksum = new CRC32();
            compressed.rewind();
            checksum.update(compressed);

            crcCheckBuffer.clear();
            fchannel.read(crcCheckBuffer);
            crcCheckBuffer.flip();
            if (crcCheckBuffer.getInt() != (int) checksum.getValue())
                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize);
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }
        catch (EOFException e)
        {
            throw new CorruptSSTableException(new CorruptBlockException(getPath(), chunkOffset, chunkSize), getPath());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }

        // Mark as dirty so we can guarantee the newly buffered bytes won't be lost on a rebuffer
        buffer.position(realMark.validBufferBytes);

        bufferOffset = truncateTarget - buffer.position();
        chunkCount = realMark.nextChunkIndex - 1;

        // truncate data and index file
        truncate(chunkOffset, bufferOffset);
        metadataWriter.resetAndTruncate(realMark.nextChunkIndex - 1);
    }

    private void truncate(long toFileSize, long toBufferOffset)
    {
        try
        {
            fchannel.truncate(toFileSize);
            lastFlushOffset = toBufferOffset;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    protected void writeDigestFile()
    {
        digestFile.ifPresent(crcMetadata::writeFullChecksum);
    }

    /**
     * Seek to the offset where next compressed data chunk should be stored.
     * Subclasses may override if they manage their own channel.
     */
    protected void seekToChunkStart()
    {
        if (getOnDiskFilePointer() != chunkOffset)
        {
            try
            {
                fchannel.position(chunkOffset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, getPath());
            }
        }
    }

    // Page management using chunk boundaries

    @Override
    public int maxBytesInPage()
    {
        return buffer.capacity();
    }

    @Override
    public void padToPageBoundary()
    {
        if (buffer.position() == 0)
            return;

        int padLength = bytesLeftInPage();

        // Flush as much as we have
        doFlush(0);
        // But pretend we had a whole chunk
        bufferOffset += padLength;
        lastFlushOffset += padLength;
    }

    @Override
    public int bytesLeftInPage()
    {
        return buffer.remaining();
    }

    @Override
    public long paddedPosition()
    {
        return position() + (buffer.position() == 0 ? 0 : buffer.remaining());
    }

    protected class TransactionalProxy extends SequentialWriter.TransactionalProxy
    {
        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return super.doCommit(metadataWriter.commit(accumulate));
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return super.doAbort(metadataWriter.abort(accumulate));
        }

        @Override
        protected void doPrepare()
        {
            syncInternal();
            writeDigestFile();
            sstableMetadataCollector.addCompressionRatio(compressedSize, uncompressedSize);
            metadataWriter.finalizeLength(current(), chunkCount).prepareToCommit();
        }

        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            accumulate = super.doPreCleanup(accumulate);
            if (compressed != null)
            {
                try
                {
                    MemoryUtil.clean(compressed);
                }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                compressed = null;
            }

            return accumulate;
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class CompressedFileWriterMark implements DataPosition
    {
        // chunk offset in the compressed file
        final long chunkOffset;
        // uncompressed data offset (real data offset)
        final long uncDataOffset;

        final int validBufferBytes;
        final int nextChunkIndex;

        public CompressedFileWriterMark(long chunkOffset, long uncDataOffset, int validBufferBytes, int nextChunkIndex)
        {
            this.chunkOffset = chunkOffset;
            this.uncDataOffset = uncDataOffset;
            this.validBufferBytes = validBufferBytes;
            this.nextChunkIndex = nextChunkIndex;
        }
    }
}
