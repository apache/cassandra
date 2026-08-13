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
package org.apache.cassandra.transport;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * If a client sends a COMPRESSED frame whose declared uncompressed length exceeds
 * {@code native_transport_max_frame_size} (or is negative), {@link Compressor.LZ4Compressor#decompress} (and the
 * Snappy variant) must reject it before that length is used to size the destination buffer.
 *
 * <p>This prevents a misbehaving client to declare a large length on a small frame, ~2 GiB throws
 * OutOfMemoryError; and instead of attempting to allocate, the client receives a {@link ProtocolException}
 * while the server keeps running avoiding the large allocation. The V3/V4 (pre-V5) decompression path is exercised.
 */
public class CompressorDecompressBoundsTest
{
    // Declared uncompressed length = 0x7FFFFFF0 (~2 GiB), big-endian.
    private static final int LARGE_UNCOMPRESSED_LENGTH = 0x7FFFFFF0;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Envelope frameWithBody(ByteBuf body)
    {
        // decompress() never inspects header.type, so any type is fine.
        return Envelope.create(Message.Type.OPTIONS, 0, ProtocolVersion.V4, EnumSet.noneOf(Envelope.Header.Flag.class), body);
    }

    @Test
    public void lz4DecompressRejectsOversizedDeclaredLength()
    {
        ByteBuf body = Unpooled.buffer(8);
        body.writeInt(LARGE_UNCOMPRESSED_LENGTH);
        body.writeBytes(new byte[]{ 0x10, 0x00, 0x00, 0x00 }); // stand-in for a tiny LZ4 block; never reached

        Envelope frame = frameWithBody(body);
        Assertions.assertThatThrownBy(() -> Compressor.LZ4Compressor.instance.decompress(frame))
                  .as("An oversized declared uncompressed length must be rejected before allocation, not OOM")
                  .isInstanceOf(ProtocolException.class)
                  .hasMessageContaining("Invalid uncompressed frame length")
                  .hasMessageContaining("native_transport_max_frame_size");
    }

    @Test
    public void lz4DecompressRejectsNegativeDeclaredLength()
    {
        // 0x80000000 decodes to Integer.MIN_VALUE via the big-endian shift in decompress().
        ByteBuf body = Unpooled.buffer(8);
        body.writeInt(0x80000000);
        body.writeBytes(new byte[]{ 0x00, 0x00, 0x00, 0x00 });

        Envelope frame = frameWithBody(body);
        Assertions.assertThatThrownBy(() -> Compressor.LZ4Compressor.instance.decompress(frame))
                  .isInstanceOf(ProtocolException.class)
                  .hasMessageContaining("Invalid uncompressed frame length");
    }

    @Test
    public void validateUncompressedLengthBoundary()
    {
        int maxFrameSize = DatabaseDescriptor.getNativeTransportMaxFrameSize();

        // At the limit is allowed.
        Compressor.validateUncompressedLength(0);
        Compressor.validateUncompressedLength(maxFrameSize);

        // Just over the limit and negative are rejected.
        Assertions.assertThatThrownBy(() -> Compressor.validateUncompressedLength(maxFrameSize + 1))
                  .isInstanceOf(ProtocolException.class)
                  .hasMessageContaining("native_transport_max_frame_size");
        Assertions.assertThatThrownBy(() -> Compressor.validateUncompressedLength(-1))
                  .isInstanceOf(ProtocolException.class);
    }

    @Test
    public void lz4RoundTripStillWorks() throws Exception
    {
        byte[] payload = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);

        Envelope compressed = Compressor.LZ4Compressor.instance.compress(frameWithBody(Unpooled.wrappedBuffer(payload)));
        Envelope decompressed = Compressor.LZ4Compressor.instance.decompress(compressed);
        try
        {
            byte[] out = new byte[decompressed.body.readableBytes()];
            decompressed.body.getBytes(decompressed.body.readerIndex(), out);
            Assertions.assertThat(out).isEqualTo(payload);
        }
        finally
        {
            decompressed.release();
        }
    }

    @Test
    public void snappyRoundTripStillWorks() throws Exception
    {
        Assume.assumeTrue("Snappy native library not available", Compressor.SnappyCompressor.instance != null);

        byte[] payload = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);

        Envelope compressed = Compressor.SnappyCompressor.instance.compress(frameWithBody(Unpooled.wrappedBuffer(payload)));
        Envelope decompressed = Compressor.SnappyCompressor.instance.decompress(compressed);
        try
        {
            byte[] out = new byte[decompressed.body.readableBytes()];
            decompressed.body.getBytes(decompressed.body.readerIndex(), out);
            Assertions.assertThat(out).isEqualTo(payload);
        }
        finally
        {
            decompressed.release();
        }
    }

    @Test
    public void lz4DecompressReleasesInputWhenRejected()
    {
        ByteBuf body = Unpooled.buffer(8);
        body.writeInt(LARGE_UNCOMPRESSED_LENGTH);
        body.writeBytes(new byte[]{ 0x10, 0x00, 0x00, 0x00 });

        Envelope frame = frameWithBody(body);
        Assertions.assertThatThrownBy(() -> Compressor.LZ4Compressor.instance.decompress(frame))
                  .isInstanceOf(ProtocolException.class);
        Assertions.assertThat(body.refCnt())
                  .as("Rejecting an oversized frame must still release the compressed input buffer")
                  .isZero();
    }

    @Test
    public void lz4DecompressReleasesInputWhenTooShort()
    {
        ByteBuf body = Unpooled.buffer(2);
        body.writeShort(0);

        Envelope frame = frameWithBody(body);
        Assertions.assertThatThrownBy(() -> Compressor.LZ4Compressor.instance.decompress(frame))
                  .isInstanceOf(ProtocolException.class)
                  .hasMessageContaining("LZ4 compressed");
        Assertions.assertThat(body.refCnt())
                  .as("Rejecting a too-short frame must still release the compressed input buffer")
                  .isZero();
    }
}
