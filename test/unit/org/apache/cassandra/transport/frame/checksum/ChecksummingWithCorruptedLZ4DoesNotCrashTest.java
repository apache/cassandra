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

package org.apache.cassandra.transport.frame.checksum;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBufUtil;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Pair;

/**
 * When we use LZ4 with "fast" functions its unsafe in the case the stream is corrupt; for networking checksuming is
 * after lz4 decompresses (see CASSANDRA-15299) which means that lz4 can crash the process.
 *
 * This test is stand alone for the reason that this test is known to cause the JVM to crash.  Given the way we run tests
 * in CI this will kill the runner which means the file will be marked as failed; if this test was embedded into another
 * test file then all the other tests would be ignored if this crashes.
 */
public class ChecksummingWithCorruptedLZ4DoesNotCrashTest
{
    @BeforeClass
    public static void init()
    {
        // required as static ChecksummingTransformer instances read default block size from config
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void shouldNotCrash() throws IOException
    {
        // We found lz4 caused the JVM to crash, so used the input (bytes and byteToCorrupt) to the test which crashed
        // to reproduce.
        // It was found that the same input does not cause lz4 to crash by it self but needed repeated calls with this
        // input produce such a failure.
        String failureHex;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("test/data/CASSANDRA-15313/lz4-jvm-crash-failure.txt"), StandardCharsets.UTF_8))) {
            failureHex = reader.readLine().trim();
        }
        byte[] failure = ByteBufUtil.decodeHexDump(failureHex);
        ReusableBuffer buffer = new ReusableBuffer(failure);
        int byteToCorrupt = 52997;
        // corrupting these values causes the exception.
        byte[] corruptionValues = new byte[] { 21, 57, 79, (byte) 179 };
        // 5k was chosen as the largest number of iterations seen needed to crash.
        for (int i = 0; i < 5_000 ; i++) {
            for (byte corruptionValue : corruptionValues) {
                try {
                    ChecksummingTransformerTest.roundTripWithCorruption(Pair.create(buffer, byteToCorrupt), corruptionValue, LZ4Compressor.INSTANCE, ChecksumType.ADLER32);
                } catch (AssertionError e) {
                    // ignore
                }
            }
        }
    }
}
