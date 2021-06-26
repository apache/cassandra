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

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.transport.CBUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 6, time = 20)
@Fork(value = 1, jvmArgsAppend = { "-Xmx256M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@State(Scope.Benchmark)
public class StringsEncodeBench
{
    private String shortText = "abcdefghijk";
    private String longText = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    private int shortTextEncodeSize = CBUtil.sizeOfString(shortText);
    private int longTextEncodeSize = CBUtil.sizeOfString(longText);

    @Benchmark
    public int writeShortText() // expecting resize
    {
        ByteBuf cb = Unpooled.buffer(shortTextEncodeSize);
        cb.writeShort(0); // field for str length
        return ByteBufUtil.writeUtf8(cb, shortText);
    }

    @Benchmark
    public int writeShortTextWithExactSize() // no resize
    {
        ByteBuf cb = Unpooled.buffer(shortTextEncodeSize);
        int size = TypeSizes.encodedUTF8Length(shortText);
        cb.writeShort(size); // field for str length
        return ByteBufUtil.reserveAndWriteUtf8(cb, shortText, size);
    }

    @Benchmark
    public int writeShortTextWithExactSizeSkipCalc() // no resize; from the encodeSize, we already know the amount of bytes required.
    {
        ByteBuf cb = Unpooled.buffer(shortTextEncodeSize);
        cb.writeShort(0); // field for str length
        int size = cb.capacity() - cb.writerIndex(); // leverage the pre-calculated encodeSize
        return ByteBufUtil.reserveAndWriteUtf8(cb, shortText, size);
    }

    @Benchmark
    public void writeShortTextAsASCII()
    {
        ByteBuf cb = Unpooled.buffer(shortTextEncodeSize);
        CBUtil.writeAsciiString(shortText, cb);
    }

    @Benchmark
    public int writeLongText() // expecting resize
    {
        ByteBuf cb = Unpooled.buffer(longTextEncodeSize);
        cb.writeShort(0); // field for str length
        return ByteBufUtil.writeUtf8(cb, longText);
    }

    @Benchmark
    public int writeLongTextWithExactSize() // no resize
    {
        ByteBuf cb = Unpooled.buffer(longTextEncodeSize);
        int size = TypeSizes.encodedUTF8Length(longText);
        cb.writeShort(size); // field for str length
        return ByteBufUtil.reserveAndWriteUtf8(cb, longText, size);
    }

    @Benchmark
    public int writeLongTextWithExactSizeSkipCalc() // no resize
    {
        ByteBuf cb = Unpooled.buffer(longTextEncodeSize);
        cb.writeShort(0); // field for str length
        int size = cb.capacity() - cb.writerIndex(); // leverage the pre-calculated encodeSize
        return ByteBufUtil.reserveAndWriteUtf8(cb, longText, size);
    }

    @Benchmark
    public void writeLongTextAsASCII()
    {
        ByteBuf cb = Unpooled.buffer(longTextEncodeSize);
        CBUtil.writeAsciiString(longText, cb);
    }

    @Benchmark
    public int sizeOfString()
    {
        return CBUtil.sizeOfString(longText);
    }

    @Benchmark
    public int sizeOfAsciiString()
    {
        return CBUtil.sizeOfAsciiString(longText);
    }
}
