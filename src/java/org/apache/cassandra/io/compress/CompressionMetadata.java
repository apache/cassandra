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

import java.io.*;
import java.util.*;

import com.google.common.primitives.Longs;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.BigLongArray;
import org.apache.cassandra.utils.Pair;

/**
 * Holds metadata about compressed file
 */
public class CompressionMetadata
{
    public final long dataLength;
    public final long compressedFileLength;
    private final BigLongArray chunkOffsets;
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
        return new CompressionMetadata(desc.filenameFor(Component.COMPRESSION_INFO), new File(dataFilePath).length());
    }

    // This is package protected because of the tests.
    CompressionMetadata(String indexFilePath, long compressedLength)
    {
        this.indexFilePath = indexFilePath;

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
            Map<String, String> options = new HashMap<String, String>();
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
    private BigLongArray readChunkOffsets(DataInput input)
    {
        try
        {
            int chunkCount = input.readInt();
            BigLongArray offsets = new BigLongArray(chunkCount);

            for (int i = 0; i < chunkCount; i++)
            {
                try
                {
                    offsets.set(i, input.readLong());
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
        int idx = (int) (position / parameters.chunkLength());

        if (idx >= chunkOffsets.size)
            throw new CorruptSSTableException(new EOFException(), indexFilePath);

        long chunkOffset = chunkOffsets.get(idx);
        long nextChunkOffset = (idx + 1 == chunkOffsets.size)
                                ? compressedFileLength
                                : chunkOffsets.get(idx + 1);

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
            for (int i = startIndex; i <= endIndex; i++)
            {
                long chunkOffset = chunkOffsets.get(i);
                long nextChunkOffset = (i + 1 == chunkOffsets.size)
                                               ? compressedFileLength
                                               : chunkOffsets.get(i + 1);
                offsets.add(new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4))); // "4" bytes reserved for checksum
            }
        }
        return offsets.toArray(new Chunk[offsets.size()]);
    }

    public static class Writer extends RandomAccessFile
    {
        // place for uncompressed data length in the index file
        private long dataLengthOffset = -1;
        // path to the file
        private final String filePath;

        private Writer(String path) throws FileNotFoundException
        {
            super(path, "rw");
            filePath = path;
        }

        public static Writer open(String path)
        {
            try
            {
                return new Writer(path);
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void writeHeader(CompressionParameters parameters)
        {
            try
            {
                writeUTF(parameters.sstableCompressor.getClass().getSimpleName());
                writeInt(parameters.otherOptions.size());
                for (Map.Entry<String, String> entry : parameters.otherOptions.entrySet())
                {
                    writeUTF(entry.getKey());
                    writeUTF(entry.getValue());
                }

                // store the length of the chunk
                writeInt(parameters.chunkLength());
                // store position and reserve a place for uncompressed data length and chunks count
                dataLengthOffset = getFilePointer();
                writeLong(-1);
                writeInt(-1);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, filePath);
            }
        }

        public void finalizeHeader(long dataLength, int chunks)
        {
            assert dataLengthOffset != -1 : "writeHeader wasn't called";

            long currentPosition;
            try
            {
                currentPosition = getFilePointer();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, filePath);
            }

            try
            {

                // seek back to the data length position
                seek(dataLengthOffset);

                // write uncompressed data length and chunks count
                writeLong(dataLength);
                writeInt(chunks);

                // seek forward to the previous position
                seek(currentPosition);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, filePath);
            }
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
            if (dataLengthOffset == -1)
                throw new IllegalStateException("writeHeader wasn't called");

            try
            {
                long position = getFilePointer();

                // seek to the position of the given chunk
                seek(dataLengthOffset
                     + 8 // size reserved for uncompressed data length
                     + 4 // size reserved for chunk count
                     + (chunkIndex * 8L));

                try
                {
                    return readLong();
                }
                finally
                {
                    // back to the original position
                    seek(position);
                }
            }
            catch (IOException e)
            {
                throw new FSReadError(e, filePath);
            }
        }

        /**
         * Reset the writer so that the next chunk offset written will be the
         * one of {@code chunkIndex}.
         */
        public void resetAndTruncate(int chunkIndex)
        {
            try
            {
                seek(dataLengthOffset
                     + 8 // size reserved for uncompressed data length
                     + 4 // size reserved for chunk count
                     + (chunkIndex * 8L));
                getChannel().truncate(getFilePointer());
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, filePath);
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
        public void serialize(Chunk chunk, DataOutput out, int version) throws IOException
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
