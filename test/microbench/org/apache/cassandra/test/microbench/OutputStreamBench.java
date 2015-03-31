/**
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

import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.BufferedDataOutputStreamTest;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.openjdk.jmh.annotations.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3,jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class OutputStreamBench
{

    BufferedOutputStream hole = new BufferedOutputStream(new OutputStream() {

        @Override
        public void write(int b) throws IOException
        {

        }

        @Override
        public void write(byte b[]) throws IOException {

        }

        @Override
        public void write(byte b[], int a, int c) throws IOException {

        }
        });

    WrappedDataOutputStreamPlus streamA = new WrappedDataOutputStreamPlus(hole);

    BufferedDataOutputStreamPlus streamB = new BufferedDataOutputStreamPlus(new WritableByteChannel() {

        @Override
        public boolean isOpen()
        {
            // TODO Auto-generated method stub
            return true;
        }

        @Override
        public void close() throws IOException
        {
            // TODO Auto-generated method stub

        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            int remaining = src.remaining();
            src.position(src.limit());
            return remaining;
        }

    }, 8192);

    public static byte foo;

    public static int foo1;

    public static long foo2;

    public static double foo3;

    public static float foo4;

    public static short foo5;

    public static char foo6;

    @Benchmark
    public void testBOSByte() throws IOException
    {
        streamA.write(foo);
    }

    @Benchmark
    public void testBDOSPByte() throws IOException
    {
        streamB.write(foo);
    }

    @Benchmark
    public void testBOSInt() throws IOException
    {
        streamA.writeInt(foo1);
    }

    @Benchmark
    public void testBDOSPInt() throws IOException
    {
        streamB.writeInt(foo1);
    }

    @Benchmark
    public void testBOSLong() throws IOException
    {
        streamA.writeLong(foo2);
    }

    @Benchmark
    public void testBDOSPLong() throws IOException
    {
        streamB.writeLong(foo2);
    }

    @Benchmark
    public void testBOSMixed() throws IOException
    {
        streamA.write(foo);
        streamA.writeInt(foo1);
        streamA.writeLong(foo2);
        streamA.writeDouble(foo3);
        streamA.writeFloat(foo4);
        streamA.writeShort(foo5);
        streamA.writeChar(foo6);
    }

    @Benchmark
    public void testBDOSPMixed() throws IOException
    {
        streamB.write(foo);
        streamB.writeInt(foo1);
        streamB.writeLong(foo2);
        streamB.writeDouble(foo3);
        streamB.writeFloat(foo4);
        streamB.writeShort(foo5);
        streamB.writeChar(foo6);
    }

    public static String tinyM = "𠝹";
    public static String smallM = "𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ𠝹㒨ƀ";
    public static String largeM;
    public static String tiny = "a";
    public static String small = "adsjglhnafsjk;gujfakyhgukafshgjkahfsgjkhafs;jkhausjkgaksfj;gafskdghajfsk;g";
    public static String large;

    static {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 1024 * 12) {
            sb.append(small);
        }
        large = sb.toString();

        sb = new StringBuilder();
        while (sb.length() < 1024 * 12) {
            sb.append(smallM);
        }
        largeM = sb.toString();
    }

    @Benchmark
    public void testMTinyStringBOS() throws IOException {
        streamA.writeUTF(tinyM);
    }

    @Benchmark
    public void testMTinyStringBDOSP() throws IOException {
        streamB.writeUTF(tinyM);
    }

    @Benchmark
    public void testMTinyLegacyWriteUTF() throws IOException {
        BufferedDataOutputStreamTest.writeUTFLegacy(tinyM, hole);
    }

    @Benchmark
    public void testMSmallStringBOS() throws IOException {
        streamA.writeUTF(smallM);
    }

    @Benchmark
    public void testMSmallStringBDOSP() throws IOException {
        streamB.writeUTF(smallM);
    }

    @Benchmark
    public void testMSmallLegacyWriteUTF() throws IOException {
        BufferedDataOutputStreamTest.writeUTFLegacy(smallM, hole);
    }

    @Benchmark
    public void testMLargeStringBOS() throws IOException {
        streamA.writeUTF(largeM);
    }

    @Benchmark
    public void testMLargeStringBDOSP() throws IOException {
        streamB.writeUTF(largeM);
    }

    @Benchmark
    public void testMLargeLegacyWriteUTF() throws IOException {
        BufferedDataOutputStreamTest.writeUTFLegacy(largeM, hole);
    }

    @Benchmark
    public void testTinyStringBOS() throws IOException {
        streamA.writeUTF(tiny);
    }

    @Benchmark
    public void testTinyStringBDOSP() throws IOException {
        streamB.writeUTF(tiny);
    }

    @Benchmark
    public void testTinyLegacyWriteUTF() throws IOException {
        BufferedDataOutputStreamTest.writeUTFLegacy(tiny, hole);
    }

    @Benchmark
    public void testSmallStringBOS() throws IOException {
        streamA.writeUTF(small);
    }

    @Benchmark
    public void testSmallStringBDOSP() throws IOException {
        streamB.writeUTF(small);
    }

    @Benchmark
    public void testSmallLegacyWriteUTF() throws IOException {
        BufferedDataOutputStreamTest.writeUTFLegacy(small, hole);
    }

    @Benchmark
    public void testRLargeStringBOS() throws IOException {
        streamA.writeUTF(large);
    }

    @Benchmark
    public void testRLargeStringBDOSP() throws IOException {
        streamB.writeUTF(large);
    }

    @Benchmark
    public void testRLargeLegacyWriteUTF() throws IOException {
        BufferedDataOutputStreamTest.writeUTFLegacy(large, hole);
    }
}