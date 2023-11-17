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

import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;

/**
 * Holds metadata about compressed file
 * TODO extract interface ICompressionMetadata which will just provide non-resource properties
 */
public class CompressionMetadata extends WrappedSharedCloseable
{
    // dataLength can represent either the true length of the file
    // or some shorter value, in the case we want to impose a shorter limit on readers
    // (when early opening, we want to ensure readers cannot read past fully written sections)
    public final long dataLength;
    public final long compressedFileLength;
    private final Memory chunkOffsets;
    private final long chunkOffsetsSize;
    public final File chunksIndexFile;
    public final CompressionParams parameters;

    @VisibleForTesting
    public static CompressionMetadata open(File chunksIndexFile, long compressedLength, boolean hasMaxCompressedSize)
    {
        CompressionParams parameters;
        long dataLength;
        Memory chunkOffsets;

        try (FileInputStreamPlus stream = chunksIndexFile.newInputStream())
        {
            String compressorName = stream.readUTF();
            int optionCount = stream.readInt();
            Map<String, String> options = new HashMap<>(optionCount);
            for (int i = 0; i < optionCount; ++i)
            {
                String key = stream.readUTF();
                String value = stream.readUTF();
                options.put(key, value);
            }
            int chunkLength = stream.readInt();
            int maxCompressedSize = Integer.MAX_VALUE;
            if (hasMaxCompressedSize)
                maxCompressedSize = stream.readInt();
            try
            {
                parameters = new CompressionParams(compressorName, chunkLength, maxCompressedSize, options);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("Cannot create CompressionParams for stored parameters", e);
            }

            dataLength = stream.readLong();
            chunkOffsets = readChunkOffsets(stream);
        }
        catch (FileNotFoundException | NoSuchFileException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, chunksIndexFile);
        }

        return new CompressionMetadata(chunksIndexFile, parameters, chunkOffsets, chunkOffsets.size(), dataLength, compressedLength);
    }

    // do not call this constructor directly, unless used in testing
    @VisibleForTesting
    public CompressionMetadata(File chunksIndexFile,
                               CompressionParams parameters,
                               Memory chunkOffsets,
                               long chunkOffsetsSize,
                               long dataLength,
                               long compressedFileLength)
    {
        super(chunkOffsets);
        this.chunksIndexFile = chunksIndexFile;
        this.parameters = parameters;
        this.dataLength = dataLength;
        this.compressedFileLength = compressedFileLength;
        this.chunkOffsets = chunkOffsets;
        this.chunkOffsetsSize = chunkOffsetsSize;
    }

    private CompressionMetadata(CompressionMetadata copy)
    {
        super(copy);
        this.chunksIndexFile = copy.chunksIndexFile;
        this.parameters = copy.parameters;
        this.dataLength = copy.dataLength;
        this.compressedFileLength = copy.compressedFileLength;
        this.chunkOffsets = copy.chunkOffsets;
        this.chunkOffsetsSize = copy.chunkOffsetsSize;
    }

    public ICompressor compressor()
    {
        return parameters.getSstableCompressor();
    }

    public int chunkLength()
    {
        return parameters.chunkLength();
    }

    public int maxCompressedLength()
    {
        return parameters.maxCompressedLength();
    }

    /**
     * Returns the amount of memory in bytes used off heap.
     * @return the amount of memory in bytes used off heap
     */
    public long offHeapSize()
    {
        return chunkOffsets.size();
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        identities.add(chunkOffsets);
    }

    @Override
    public CompressionMetadata sharedCopy()
    {
        return new CompressionMetadata(this);
    }

    /**
     * Read offsets of the individual chunks from the given input.
     *
     * @param input Source of the data.
     *
     * @return collection of the chunk offsets.
     */
    private static Memory readChunkOffsets(FileInputStreamPlus input)
    {
        final int chunkCount;
        try
        {
            chunkCount = input.readInt();
            if (chunkCount <= 0)
                throw new IOException("Compressed file with 0 chunks encountered: " + input);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, input.file);
        }

        Memory offsets = Memory.allocate(chunkCount * 8L);
        int i = 0;
        try
        {

            for (i = 0; i < chunkCount; i++)
            {
                offsets.setLong(i * 8L, input.readLong());
            }

            return offsets;
        }
        catch (IOException e)
        {
            if (offsets != null)
                offsets.close();

            if (e instanceof EOFException)
            {
                String msg = String.format("Corrupted Index File %s: read %d but expected %d chunks.",
                                           input.file.path(), i, chunkCount);
                throw new CorruptSSTableException(new IOException(msg, e), input.file);
            }
            throw new FSReadError(e, input.file);
        }
    }

    /**
     * Get a chunk of compressed data (offset, length) corresponding to given position
     *
     * @param position Position in the file.
     * @return pair of chunk offset and length.
     */
    public Chunk chunkFor(long position)
    {
        // position of the chunk
        long idx = 8 * (position / parameters.chunkLength());

        if (idx >= chunkOffsetsSize)
            throw new CorruptSSTableException(new EOFException(), chunksIndexFile);

        if (idx < 0)
            throw new CorruptSSTableException(new IllegalArgumentException(String.format("Invalid negative chunk index %d with position %d", idx, position)),
                                              chunksIndexFile);

        long chunkOffset = chunkOffsets.getLong(idx);
        long nextChunkOffset = (idx + 8 == chunkOffsetsSize)
                                ? compressedFileLength
                                : chunkOffsets.getLong(idx + 8);

        return new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4)); // "4" bytes reserved for checksum
    }

    public long getDataOffsetForChunkOffset(long chunkOffset)
    {
        long l = 0;
        long h = (chunkOffsetsSize >> 3) - 1;
        long idx, offset;

        while (l <= h)
        {
            idx = (l + h) >>> 1;
            offset = chunkOffsets.getLong(idx << 3);

            if (offset < chunkOffset)
                l = idx + 1;
            else if (offset > chunkOffset)
                h = idx - 1;
            else
                return idx * parameters.chunkLength();
        }

        throw new IllegalArgumentException("No chunk with offset " + chunkOffset);
    }

    /**
     * @param sections Collection of sections in uncompressed file. Should not contain sections that overlap each other.
     * @return Total chunk size in bytes for given sections including checksum.
     */
    public long getTotalSizeForSections(Collection<SSTableReader.PartitionPositionBounds> sections)
    {
        long size = 0;
        long lastOffset = -1;
        for (SSTableReader.PartitionPositionBounds section : sections)
        {
            int startIndex = (int) (section.lowerPosition / parameters.chunkLength());

            int endIndex = (int) (section.upperPosition / parameters.chunkLength());
            if (section.upperPosition % parameters.chunkLength() == 0)
                endIndex--;

            for (int i = startIndex; i <= endIndex; i++)
            {
                long offset = i * 8L;
                long chunkOffset = chunkOffsets.getLong(offset);
                if (chunkOffset > lastOffset)
                {
                    lastOffset = chunkOffset;
                    long nextChunkOffset = offset + 8 == chunkOffsetsSize
                                                   ? compressedFileLength
                                                   : chunkOffsets.getLong(offset + 8);
                    size += (nextChunkOffset - chunkOffset);
                }
            }
        }
        return size;
    }

    /**
     * @param sections Collection of sections in uncompressed file
     * @return Array of chunks which corresponds to given sections of uncompressed file, sorted by chunk offset
     */
    public Chunk[] getChunksForSections(Collection<SSTableReader.PartitionPositionBounds> sections)
    {
        // use SortedSet to eliminate duplicates and sort by chunk offset
        SortedSet<Chunk> offsets = new TreeSet<>((o1, o2) -> Longs.compare(o1.offset, o2.offset));

        for (SSTableReader.PartitionPositionBounds section : sections)
        {
            int startIndex = (int) (section.lowerPosition / parameters.chunkLength());

            int endIndex = (int) (section.upperPosition / parameters.chunkLength());
            if (section.upperPosition % parameters.chunkLength() == 0)
                endIndex--;

            for (int i = startIndex; i <= endIndex; i++)
            {
                long offset = i * 8L;
                long chunkOffset = chunkOffsets.getLong(offset);
                long nextChunkOffset = offset + 8 == chunkOffsetsSize
                                     ? compressedFileLength
                                     : chunkOffsets.getLong(offset + 8);
                offsets.add(new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4))); // "4" bytes reserved for checksum
            }
        }

        return offsets.toArray(new Chunk[offsets.size()]);
    }

    public static class Writer extends Transactional.AbstractTransactional implements Transactional
    {
        // path to the file
        private final CompressionParams parameters;
        private final File file;
        private int maxCount = 100;
        private SafeMemory offsets = new SafeMemory(maxCount * 8L);
        private int count = 0;

        // provided by user when setDescriptor
        private long dataLength, chunkCount;

        private Writer(CompressionParams parameters, File file)
        {
            this.parameters = parameters;
            this.file = file;
        }

        public static Writer open(CompressionParams parameters, File file)
        {
            return new Writer(parameters, file);
        }

        public void addOffset(long offset)
        {
            if (count == maxCount)
            {
                SafeMemory newOffsets = offsets.copy((maxCount *= 2L) * 8L);
                offsets.close();
                offsets = newOffsets;
            }
            offsets.setLong(8L * count++, offset);
        }

        private void writeHeader(DataOutput out, long dataLength, int chunks)
        {
            try
            {
                out.writeUTF(parameters.getSstableCompressor().getClass().getSimpleName());
                out.writeInt(parameters.getOtherOptions().size());
                for (Map.Entry<String, String> entry : parameters.getOtherOptions().entrySet())
                {
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue());
                }

                // store the length of the chunk
                out.writeInt(parameters.chunkLength());
                out.writeInt(parameters.maxCompressedLength());
                // store position and reserve a place for uncompressed data length and chunks count
                out.writeLong(dataLength);
                out.writeInt(chunks);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, file);
            }
        }

        // we've written everything; wire up some final metadata state
        public Writer finalizeLength(long dataLength, int chunkCount)
        {
            this.dataLength = dataLength;
            this.chunkCount = chunkCount;
            return this;
        }

        @Override
        public void doPrepare()
        {
            assert chunkCount == count;

            // finalize the size of memory used if it won't now change;
            // unnecessary if already correct size
            if (offsets.size() != count * 8L)
            {
                SafeMemory tmp = offsets;
                offsets = offsets.copy(count * 8L);
                tmp.free();
            }

            // flush the data to disk
            try (FileOutputStreamPlus out = file.newOutputStream(File.WriteMode.OVERWRITE))
            {
                writeHeader(out, dataLength, count);
                for (int i = 0; i < count; i++)
                    out.writeLong(offsets.getLong(i * 8L));

                out.flush();
                out.sync();
            }
            catch (FileNotFoundException | NoSuchFileException fnfe)
            {
                throw new RuntimeException(fnfe);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, file);
            }
        }

        public CompressionMetadata open(long dataLength, long compressedLength)
        {
            SafeMemory tOffsets = this.offsets.sharedCopy();

            // calculate how many entries we need, if our dataLength is truncated
            int tCount = (int) (dataLength / parameters.chunkLength());
            if (dataLength % parameters.chunkLength() != 0)
                tCount++;

            assert tCount > 0;
            // grab our actual compressed length from the next offset from our the position we're opened to
            if (tCount < this.count)
                compressedLength = tOffsets.getLong(tCount * 8L);

            return new CompressionMetadata(file, parameters, tOffsets, tCount * 8L, dataLength, compressedLength);
        }

        /**
         * Get a chunk offset by it's index.
         *
         * @param chunkIndex Index of the chunk.
         *
         * @return offset of the chunk in the compressed file.
         */
        public long chunkOffsetBy(int chunkIndex)
        {
            return offsets.getLong(chunkIndex * 8L);
        }

        /**
         * Reset the writer so that the next chunk offset written will be the
         * one of {@code chunkIndex}.
         *
         * @param chunkIndex the next index to write
         */
        public void resetAndTruncate(int chunkIndex)
        {
            count = chunkIndex;
        }

        @Override
        protected Throwable doPostCleanup(Throwable failed)
        {
            return offsets.close(failed);
        }

        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return accumulate;
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return accumulate;
        }
    }

    /**
     * Holds offset and length of the file chunk
     */
    public static class Chunk
    {
        public static final IVersionedSerializer<Chunk> serializer = new ChunkSerializer();

        public final long offset;
        public final int length;

        public Chunk(long offset, int length)
        {
            assert(length > 0);

            this.offset = offset;
            this.length = length;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Chunk chunk = (Chunk) o;
            return length == chunk.length && offset == chunk.offset;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + length;
            return result;
        }

        @Override
        public String toString()
        {
            return String.format("Chunk<offset: %d, length: %d>", offset, length);
        }
    }

    static class ChunkSerializer implements IVersionedSerializer<Chunk>
    {
        @Override
        public void serialize(Chunk chunk, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(chunk.offset);
            out.writeInt(chunk.length);
        }

        @Override
        public Chunk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Chunk(in.readLong(), in.readInt());
        }

        @Override
        public long serializedSize(Chunk chunk, int version)
        {
            long size = TypeSizes.sizeof(chunk.offset);
            size += TypeSizes.sizeof(chunk.length);
            return size;
        }
    }
}
