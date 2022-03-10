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

package org.apache.cassandra.streaming.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;

import static org.apache.cassandra.net.MessagingService.current_version;

/**
 * A serialiazer for stream compressed files (see package-level documentation). Much like a typical compressed
 * output stream, this class operates on buffers or chunks of the data at a a time. The format for each compressed
 * chunk is as follows:
 *
 * - int - compressed payload length
 * - int - uncompressed payload length
 * - bytes - compressed payload
 */
public class StreamCompressionSerializer
{
    private final ByteBufAllocator allocator;

    public StreamCompressionSerializer(ByteBufAllocator allocator)
    {
        this.allocator = allocator;
    }

    /**
     * Length of heaer data, which includes compressed length, uncompressed length.
     */
    private static final int HEADER_LENGTH = 8;

    public static StreamingDataOutputPlus.Write serialize(LZ4Compressor compressor, ByteBuffer in, int version)
    {
        assert version == current_version;
        return bufferSupplier -> {
            int uncompressedLength = in.remaining();
            int maxLength = compressor.maxCompressedLength(uncompressedLength);
            ByteBuffer out = bufferSupplier.get(maxLength);
            out.position(HEADER_LENGTH);
            compressor.compress(in, out);
            int compressedLength = out.position() - HEADER_LENGTH;
            out.putInt(0, compressedLength);
            out.putInt(4, uncompressedLength);
            out.flip();
        };
    }

    /**
     * @return A buffer with decompressed data.
     */
    public ByteBuf deserialize(LZ4SafeDecompressor decompressor, DataInputPlus in, int version) throws IOException
    {
        final int compressedLength = in.readInt();
        final int uncompressedLength = in.readInt();

        // there's no guarantee the next compressed block is contained within one buffer in the input,
        // so hence we need a 'staging' buffer to get all the bytes into one contiguous buffer for the decompressor
        ByteBuf compressed = null;
        ByteBuf uncompressed = null;
        try
        {
            final ByteBuffer compressedNioBuffer;

            // ReadableByteChannel allows us to keep the bytes off-heap because we pass a ByteBuffer to RBC.read(BB),
            // DataInputPlus.read() takes a byte array (thus, an on-heap array).
            if (in instanceof ReadableByteChannel)
            {
                compressed = allocator.directBuffer(compressedLength);
                compressedNioBuffer = compressed.nioBuffer(0, compressedLength);
                int readLength = ((ReadableByteChannel) in).read(compressedNioBuffer);
                assert readLength == compressedNioBuffer.position();
                compressedNioBuffer.flip();
            }
            else
            {
                byte[] compressedBytes = new byte[compressedLength];
                in.readFully(compressedBytes);
                compressedNioBuffer = ByteBuffer.wrap(compressedBytes);
            }

            uncompressed = allocator.directBuffer(uncompressedLength);
            ByteBuffer uncompressedNioBuffer = uncompressed.nioBuffer(0, uncompressedLength);
            decompressor.decompress(compressedNioBuffer, uncompressedNioBuffer);
            uncompressed.writerIndex(uncompressedLength);
            return uncompressed;
        }
        catch (Exception e)
        {
            if (uncompressed != null)
                uncompressed.release();

            if (e instanceof IOException)
                throw e;
            throw new IOException(e);
        }
        finally
        {
            if (compressed != null)
                compressed.release();
        }
    }
}
