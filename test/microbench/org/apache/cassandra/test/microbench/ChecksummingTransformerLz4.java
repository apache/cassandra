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

package org.apache.cassandra.test.microbench;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.frame.checksum.ChecksummingTransformer;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
import org.apache.cassandra.utils.ChecksumType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 8, time = 2)
@Fork(value = 2)
@State(Scope.Benchmark)
public class ChecksummingTransformerLz4
{
    private static final EnumSet<Frame.Header.Flag> FLAGS = EnumSet.of(Frame.Header.Flag.COMPRESSED, Frame.Header.Flag.CHECKSUMMED);

    static {
        DatabaseDescriptor.clientInitialization();
    }

    private final ChecksummingTransformer transformer = ChecksummingTransformer.getTransformer(ChecksumType.CRC32, LZ4Compressor.INSTANCE);
    private ByteBuf smallEnglishASCIICompressed;
    private ByteBuf smallEnglishUtf8Compressed;
    private ByteBuf largeBlobCompressed;

    @Setup
    public void setup() throws IOException
    {
        byte[] smallEnglishASCII = "this is small".getBytes(StandardCharsets.US_ASCII);
        this.smallEnglishASCIICompressed = transformer.transformOutbound(Unpooled.wrappedBuffer(smallEnglishASCII));
        byte[] smallEnglishUtf8 = "this is small".getBytes(StandardCharsets.UTF_8);
        this.smallEnglishUtf8Compressed = transformer.transformOutbound(Unpooled.wrappedBuffer(smallEnglishUtf8));

        String failureHex;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("test/data/CASSANDRA-15313/lz4-jvm-crash-failure.txt"), StandardCharsets.UTF_8))) {
            failureHex = reader.readLine().trim();
        }
        byte[] failure = ByteBufUtil.decodeHexDump(failureHex);
        this.largeBlobCompressed = transformer.transformOutbound(Unpooled.wrappedBuffer(failure));
    }

    @Benchmark
    public ByteBuf decompresSsmallEnglishASCII() {
        smallEnglishASCIICompressed.readerIndex(0);
        return transformer.transformInbound(smallEnglishASCIICompressed, FLAGS);
    }

    @Benchmark
    public ByteBuf decompresSsmallEnglishUtf8() {
        smallEnglishUtf8Compressed.readerIndex(0);
        return transformer.transformInbound(smallEnglishUtf8Compressed, FLAGS);
    }

    @Benchmark
    public ByteBuf decompresLargeBlob() {
        largeBlobCompressed.readerIndex(0);
        return transformer.transformInbound(largeBlobCompressed, FLAGS);
    }
}
