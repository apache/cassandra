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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.serializers.UTF8Serializer;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class UTF8ValidatorBench
{

    @Param({ "short ASCII", "long ASCII", "short ASCII prefix non-ASCII", "short non-ASCII", "long non-ASCII"})
    private String stringType;

    byte[] arrayValue;
    ByteBuffer heapByteBufferValue;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        switch (stringType)
        {
            case "short ASCII":
                arrayValue = "ASCII string".getBytes(StandardCharsets.UTF_8);
                break;
            case "long ASCII":
                arrayValue = ("ASCII is an acronym for American Standard Code for Information Interchange, " +
                              "is a character encoding standard for representing a particular set of 95 " +
                              "(English language focused) printable and 33 control characters – a total of 128 code points. " +
                              "The set of available punctuation had significant impact on the syntax of computer languages " +
                              "and text markup. ASCII hugely influenced the design of character sets used by modern computers; " +
                              "for example, the first 128 code points of Unicode are the same as ASCII.").getBytes(StandardCharsets.UTF_8);
                break;
            case "short ASCII prefix non-ASCII":
                arrayValue = "a hierarchy of number systems: ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ".getBytes(StandardCharsets.UTF_8);
                break;
            case "short non-ASCII":
                arrayValue = "ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ".getBytes(StandardCharsets.UTF_8);
                break;
            case "long non-ASCII": // https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
                arrayValue = ("⡍⠜⠇⠑⠹ ⠺⠁⠎ ⠙⠑⠁⠙⠒ ⠞⠕ ⠃⠑⠛⠔ ⠺⠊⠹⠲ ⡹⠻⠑ ⠊⠎ ⠝⠕ ⠙⠳⠃⠞\n" +
                             "  ⠱⠁⠞⠑⠧⠻ ⠁⠃⠳⠞ ⠹⠁⠞⠲ ⡹⠑ ⠗⠑⠛⠊⠌⠻ ⠕⠋ ⠙⠊⠎ ⠃⠥⠗⠊⠁⠇ ⠺⠁⠎\n" +
                             "  ⠎⠊⠛⠝⠫ ⠃⠹ ⠹⠑ ⠊⠇⠻⠛⠹⠍⠁⠝⠂ ⠹⠑ ⠊⠇⠻⠅⠂ ⠹⠑ ⠥⠝⠙⠻⠞⠁⠅⠻⠂\n" +
                             "  ⠁⠝⠙ ⠹⠑ ⠡⠊⠑⠋ ⠍⠳⠗⠝⠻⠲ ⡎⠊⠗⠕⠕⠛⠑ ⠎⠊⠛⠝⠫ ⠊⠞⠲ ⡁⠝⠙\n" +
                             "  ⡎⠊⠗⠕⠕⠛⠑⠰⠎ ⠝⠁⠍⠑ ⠺⠁⠎ ⠛⠕⠕⠙ ⠥⠏⠕⠝ ⠰⡡⠁⠝⠛⠑⠂ ⠋⠕⠗ ⠁⠝⠹⠹⠔⠛ ⠙⠑ \n" +
                             "  ⠡⠕⠎⠑ ⠞⠕ ⠏⠥⠞ ⠙⠊⠎ ⠙⠁⠝⠙ ⠞⠕⠲").getBytes(StandardCharsets.UTF_8);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        heapByteBufferValue = ByteBuffer.allocate(arrayValue.length);
        heapByteBufferValue.put(arrayValue).rewind();
    }


    @Benchmark
    public void testBimorphic()
    {
        UTF8Serializer.instance.validate(heapByteBufferValue, ByteBufferAccessor.instance);
        UTF8Serializer.instance.validate(arrayValue, ByteArrayAccessor.instance);
    }


    @Benchmark
    public void testMonomorphicArray()
    {
        UTF8Serializer.instance.validate(arrayValue, ByteArrayAccessor.instance);
        UTF8Serializer.instance.validate(arrayValue, ByteArrayAccessor.instance);
    }

    @Benchmark
    public void testMonomorphicHeapByteBuffer()
    {
        UTF8Serializer.instance.validate(heapByteBufferValue, ByteBufferAccessor.instance);
        UTF8Serializer.instance.validate(heapByteBufferValue, ByteBufferAccessor.instance);
    }
}
