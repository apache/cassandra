/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.compress;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Holds metadata about compressed file
 */
public class CompressionMetadata
{
    public final long dataLength;
    public final long compressedFileLength;
    public final long[] chunkOffsets;
    public final String indexFilePath;
    public final CompressionParameters parameters;

    public CompressionMetadata(String indexFilePath, long compressedLength) throws IOException
    {
        this.indexFilePath = indexFilePath;

        DataInputStream stream = new DataInputStream(new FileInputStream(indexFilePath));

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

        FileUtils.closeQuietly(stream);
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
     *
     * @throws java.io.IOException on any I/O error (except EOF).
     */
    private long[] readChunkOffsets(DataInput input) throws IOException
    {
        int chunkCount = input.readInt();
        long[] offsets = new long[chunkCount];

        for (int i = 0; i < offsets.length; i++)
        {
            try
            {
                offsets[i] = input.readLong();
            }
            catch (EOFException e)
            {
                throw new EOFException(String.format("Corrupted Index File %s: read %d but expected %d chunks.",
                                                     indexFilePath,
                                                     i,
                                                     chunkCount));
            }
        }

        return offsets;
    }

    /**
     * Get a chunk of compressed data (offset, length) corresponding to given position
     *
     * @param position Position in the file.
     * @return pair of chunk offset and length.
     * @throws java.io.IOException on any I/O error.
     */
    public Chunk chunkFor(long position) throws IOException
    {
        // position of the chunk
        int idx = (int) (position / parameters.chunkLength());

        if (idx >= chunkOffsets.length)
            throw new EOFException();

        long chunkOffset = chunkOffsets[idx];
        long nextChunkOffset = (idx + 1 == chunkOffsets.length)
                                ? compressedFileLength
                                : chunkOffsets[idx + 1];

        return new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4)); // "4" bytes reserved for checksum
    }

    public static class Writer extends RandomAccessFile
    {
        // place for uncompressed data length in the index file
        private long dataLengthOffset = -1;

        public Writer(String path) throws IOException
        {
            super(path, "rw");
        }

        public void writeHeader(CompressionParameters parameters) throws IOException
        {
            // algorithm
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

        public void finalizeHeader(long dataLength, int chunks) throws IOException
        {
            assert dataLengthOffset != -1 : "writeHeader wasn't called";

            long currentPosition = getFilePointer();

            // seek back to the data length position
            seek(dataLengthOffset);

            // write uncompressed data length and chunks count
            writeLong(dataLength);
            writeInt(chunks);

            // seek forward to the previous position
            seek(currentPosition);
        }

        /**
         * Get a chunk offset by it's index.
         *
         * @param chunkIndex Index of the chunk.
         *
         * @return offset of the chunk in the compressed file.
         *
         * @throws IOException any I/O error.
         */
        public long chunkOffsetBy(int chunkIndex) throws IOException
        {
            if (dataLengthOffset == -1)
                throw new IllegalStateException("writeHeader wasn't called");

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

        /**
         * Reset the writer so that the next chunk offset written will be the
         * one of {@code chunkIndex}.
         */
        public void resetAndTruncate(int chunkIndex) throws IOException
        {
            seek(dataLengthOffset
                 + 8 // size reserved for uncompressed data length
                 + 4 // size reserved for chunk count
                 + (chunkIndex * 8L));
            getChannel().truncate(getFilePointer());
        }
    }

    /**
     * Holds offset and length of the file chunk
     */
    public class Chunk
    {
        public final long offset;
        public final int length;

        public Chunk(long offset, int length)
        {
            this.offset = offset;
            this.length = length;
        }

        public String toString()
        {
            return String.format("Chunk<offset: %d, length: %d>", offset, length);
        }
    }
}
