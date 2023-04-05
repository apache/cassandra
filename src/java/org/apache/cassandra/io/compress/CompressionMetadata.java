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

import java.nio.file.NoSuchFileException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;

import org.apache.cassandra.io.util.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.primitives.Longs;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Holds metadata about compressed file
 */
public class CompressionMetadata
{
    // dataLength can represent either the true length of the file
    // or some shorter value, in the case we want to impose a shorter limit on readers
    // (when early opening, we want to ensure readers cannot read past fully written sections)
    public final long dataLength;
    public final long compressedFileLength;
    private final Memory chunkOffsets;
    private final long chunkOffsetsSize;
    public final String indexFilePath;
    public final CompressionParams parameters;

    /**
     * Create metadata about given compressed file including uncompressed data length, chunk size
     * and list of the chunk offsets of the compressed data.
     *
     * This is an expensive operation! Don't create more than one for each
     * sstable.
     *
     * @param dataFilePath Path to the compressed file
     *
     * @return metadata about given compressed file.
     */
    public static CompressionMetadata create(String dataFilePath)
    {
        return createWithLength(dataFilePath, new File(dataFilePath).length());
    }

    public static CompressionMetadata createWithLength(String dataFilePath, long compressedLength)
    {
        return new CompressionMetadata(Descriptor.fromFilename(dataFilePath), compressedLength);
    }

    @VisibleForTesting
    public CompressionMetadata(Descriptor desc, long compressedLength)
    {
        this(desc.filenameFor(Component.COMPRESSION_INFO), compressedLength, desc.version.hasMaxCompressedLength());
    }

    @VisibleForTesting
    public CompressionMetadata(String indexFilePath, long compressedLength, boolean hasMaxCompressedSize)
    {
        this.indexFilePath = indexFilePath;

        try (FileInputStreamPlus stream = new File(indexFilePath).newInputStream())
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
            compressedFileLength = compressedLength;
            chunkOffsets = readChunkOffsets(stream);
        }
        catch (FileNotFoundException | NoSuchFileException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, indexFilePath);
        }

        this.chunkOffsetsSize = chunkOffsets.size();
    }

    // do not call this constructor directly, unless used in testing
    @VisibleForTesting
    public CompressionMetadata(String filePath, CompressionParams parameters, Memory offsets, long offsetsSize, long dataLength, long compressedLength)
    {
        this.indexFilePath = filePath;
        this.parameters = parameters;
        this.dataLength = dataLength;
        this.compressedFileLength = compressedLength;
        this.chunkOffsets = offsets;
        this.chunkOffsetsSize = offsetsSize;
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

    public void addTo(Ref.IdentityCollection identities)
    {
        identities.add(chunkOffsets);
    }

    /**
     * Read offsets of the individual chunks from the given input.
     *
     * @param input Source of the data.
     *
     * @return collection of the chunk offsets.
     */
    private Memory readChunkOffsets(DataInput input)
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
            throw new FSReadError(e, indexFilePath);
        }

        @SuppressWarnings("resource")
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
                                           indexFilePath, i, chunkCount);
                throw new CorruptSSTableException(new IOException(msg, e), indexFilePath);
            }
            throw new FSReadError(e, indexFilePath);
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
            throw new CorruptSSTableException(new EOFException(), indexFilePath);

        if (idx < 0)
            throw new CorruptSSTableException(new IllegalArgumentException(String.format("Invalid negative chunk index %d with position %d", idx, position)),
                                              indexFilePath);

        long chunkOffset = chunkOffsets.getLong(idx);
        long nextChunkOffset = (idx + 8 == chunkOffsetsSize)
                                ? compressedFileLength
                                : chunkOffsets.getLong(idx + 8);

        return new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4)); // "4" bytes reserved for checksum
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

    public void close()
    {
        chunkOffsets.close();
    }

    public static class Writer extends Transactional.AbstractTransactional implements Transactional
    {
        // path to the file
        private final CompressionParams parameters;
        private final String filePath;
        private int maxCount = 100;
        private SafeMemory offsets = new SafeMemory(maxCount * 8L);
        private int count = 0;

        // provided by user when setDescriptor
        private long dataLength, chunkCount;

        private Writer(CompressionParams parameters, String path)
        {
            this.parameters = parameters;
            filePath = path;
        }

        public static Writer open(CompressionParams parameters, String path)
        {
            return new Writer(parameters, path);
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
                throw new FSWriteError(e, filePath);
            }
        }

        // we've written everything; wire up some final metadata state
        public Writer finalizeLength(long dataLength, int chunkCount)
        {
            this.dataLength = dataLength;
            this.chunkCount = chunkCount;
            return this;
        }

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
            try (FileOutputStreamPlus out = new FileOutputStreamPlus(filePath))
            {
                writeHeader(out, dataLength, count);
                for (int i = 0; i < count; i++)
                    out.writeLong(offsets.getLong(i * 8L));

                out.flush();
                out.sync();
            }
            catch (FileNotFoundException | NoSuchFileException fnfe)
            {
                throw Throwables.propagate(fnfe);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, filePath);
            }
        }

        @SuppressWarnings("resource")
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

            return new CompressionMetadata(filePath, parameters, tOffsets, tCount * 8L, dataLength, compressedLength);
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

        protected Throwable doPostCleanup(Throwable failed)
        {
            return offsets.close(failed);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return accumulate;
        }

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

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Chunk chunk = (Chunk) o;
            return length == chunk.length && offset == chunk.offset;
        }

        public int hashCode()
        {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + length;
            return result;
        }

        public String toString()
        {
            return String.format("Chunk<offset: %d, length: %d>", offset, length);
        }
    }

    static class ChunkSerializer implements IVersionedSerializer<Chunk>
    {
        public void serialize(Chunk chunk, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(chunk.offset);
            out.writeInt(chunk.length);
        }

        public Chunk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Chunk(in.readLong(), in.readInt());
        }

        public long serializedSize(Chunk chunk, int version)
        {
            long size = TypeSizes.sizeof(chunk.offset);
            size += TypeSizes.sizeof(chunk.length);
            return size;
        }
    }
}
