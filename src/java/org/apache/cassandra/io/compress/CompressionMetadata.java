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

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;

import org.apache.cassandra.cache.RefCountedMemory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.utils.Pair;

/**
 * Holds metadata about compressed file
 */
public class CompressionMetadata
{
    public final long dataLength;
    public final long compressedFileLength;
    public final boolean hasPostCompressionAdlerChecksums;
    private final Memory chunkOffsets;
    private final long chunkOffsetsSize;
    public final String indexFilePath;
    public final CompressionParameters parameters;

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
        Descriptor desc = Descriptor.fromFilename(dataFilePath);
        return new CompressionMetadata(desc.filenameFor(Component.COMPRESSION_INFO), new File(dataFilePath).length(), desc.version.hasPostCompressionAdlerChecksums());
    }

    @VisibleForTesting
    CompressionMetadata(String indexFilePath, long compressedLength, boolean hasPostCompressionAdlerChecksums)
    {
        this.indexFilePath = indexFilePath;
        this.hasPostCompressionAdlerChecksums = hasPostCompressionAdlerChecksums;

        DataInputStream stream;
        try
        {
            stream = new DataInputStream(new FileInputStream(indexFilePath));
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        try
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
            try
            {
                parameters = new CompressionParameters(compressorName, chunkLength, options);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("Cannot create CompressionParameters for stored parameters", e);
            }

            dataLength = stream.readLong();
            compressedFileLength = compressedLength;
            chunkOffsets = readChunkOffsets(stream);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, indexFilePath);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
        this.chunkOffsetsSize = chunkOffsets.size();
    }

    private CompressionMetadata(String filePath, CompressionParameters parameters, RefCountedMemory offsets, long offsetsSize, long dataLength, long compressedLength, boolean hasPostCompressionAdlerChecksums)
    {
        this.indexFilePath = filePath;
        this.parameters = parameters;
        this.dataLength = dataLength;
        this.compressedFileLength = compressedLength;
        this.hasPostCompressionAdlerChecksums = hasPostCompressionAdlerChecksums;
        this.chunkOffsets = offsets;
        offsets.reference();
        this.chunkOffsetsSize = offsetsSize;
    }

    public ICompressor compressor()
    {
        return parameters.sstableCompressor;
    }

    public int chunkLength()
    {
        return parameters.chunkLength();
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
        try
        {
            int chunkCount = input.readInt();
            Memory offsets = Memory.allocate(chunkCount * 8);

            for (int i = 0; i < chunkCount; i++)
            {
                try
                {
                    offsets.setLong(i * 8, input.readLong());
                }
                catch (EOFException e)
                {
                    String msg = String.format("Corrupted Index File %s: read %d but expected %d chunks.",
                                               indexFilePath, i, chunkCount);
                    throw new CorruptSSTableException(new IOException(msg, e), indexFilePath);
                }
            }

            return offsets;
        }
        catch (IOException e)
        {
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
        int idx = 8 * (int) (position / parameters.chunkLength());

        if (idx >= chunkOffsetsSize)
            throw new CorruptSSTableException(new EOFException(), indexFilePath);

        long chunkOffset = chunkOffsets.getLong(idx);
        long nextChunkOffset = (idx + 8 == chunkOffsets.size())
                                ? compressedFileLength
                                : chunkOffsets.getLong(idx + 8);

        return new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4)); // "4" bytes reserved for checksum
    }

    /**
     * @param sections Collection of sections in uncompressed file
     * @return Array of chunks which corresponds to given sections of uncompressed file, sorted by chunk offset
     */
    public Chunk[] getChunksForSections(Collection<Pair<Long, Long>> sections)
    {
        // use SortedSet to eliminate duplicates and sort by chunk offset
        SortedSet<Chunk> offsets = new TreeSet<Chunk>(new Comparator<Chunk>()
        {
            public int compare(Chunk o1, Chunk o2)
            {
                return Longs.compare(o1.offset, o2.offset);
            }
        });
        for (Pair<Long, Long> section : sections)
        {
            int startIndex = (int) (section.left / parameters.chunkLength());
            int endIndex = (int) (section.right / parameters.chunkLength());
            endIndex = section.right % parameters.chunkLength() == 0 ? endIndex - 1 : endIndex;
            for (int i = startIndex; i <= endIndex; i++)
            {
                long offset = i * 8;
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
        if (chunkOffsets instanceof RefCountedMemory)
            ((RefCountedMemory) chunkOffsets).unreference();
        else
            chunkOffsets.free();
    }

    public static class Writer
    {
        // path to the file
        private final CompressionParameters parameters;
        private final String filePath;
        private int maxCount = 100;
        private RefCountedMemory offsets = new RefCountedMemory(maxCount * 8);
        private int count = 0;
        private Version latestVersion =  DatabaseDescriptor.getSSTableFormat().info.getLatestVersion();


        private Writer(CompressionParameters parameters, String path)
        {
            this.parameters = parameters;
            filePath = path;
        }

        public static Writer open(CompressionParameters parameters, String path)
        {
            return new Writer(parameters, path);
        }

        public void addOffset(long offset)
        {
            if (count == maxCount)
            {
                RefCountedMemory newOffsets = offsets.copy((maxCount *= 2) * 8);
                offsets.unreference();
                offsets = newOffsets;
            }
            offsets.setLong(8 * count++, offset);
        }

        private void writeHeader(DataOutput out, long dataLength, int chunks)
        {
            try
            {
                out.writeUTF(parameters.sstableCompressor.getClass().getSimpleName());
                out.writeInt(parameters.otherOptions.size());
                for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
                {
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue());
                }

                // store the length of the chunk
                out.writeInt(parameters.chunkLength());
                // store position and reserve a place for uncompressed data length and chunks count
                out.writeLong(dataLength);
                out.writeInt(chunks);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, filePath);
            }
        }

        public CompressionMetadata openEarly(long dataLength, long compressedLength)
        {
            return new CompressionMetadata(filePath, parameters, offsets, count * 8L, dataLength, compressedLength, latestVersion.hasPostCompressionAdlerChecksums());
        }

        public CompressionMetadata openAfterClose(long dataLength, long compressedLength)
        {
            RefCountedMemory newOffsets = offsets.copy(count * 8L);
            offsets.unreference();
            return new CompressionMetadata(filePath, parameters, newOffsets, count * 8L, dataLength, compressedLength, latestVersion.hasPostCompressionAdlerChecksums());
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

        public void close(long dataLength, int chunks) throws IOException
        {
            DataOutputStream out = null;
            try
            {
            	out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
	            assert chunks == count;
	            writeHeader(out, dataLength, chunks);
	            for (int i = 0 ; i < count ; i++)
	                out.writeLong(offsets.getLong(i * 8));
            }
            finally
            {
                FileUtils.closeQuietly(out);
            }
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

        public Chunk deserialize(DataInput in, int version) throws IOException
        {
            return new Chunk(in.readLong(), in.readInt());
        }

        public long serializedSize(Chunk chunk, int version)
        {
            long size = TypeSizes.NATIVE.sizeof(chunk.offset);
            size += TypeSizes.NATIVE.sizeof(chunk.length);
            return size;
        }
    }
}
