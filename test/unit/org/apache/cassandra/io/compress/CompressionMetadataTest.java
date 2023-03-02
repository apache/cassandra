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


import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.schema.CompressionParams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class CompressionMetadataTest
{
    File chunksIndexFile = new File("/path/to/metadata");
    CompressionParams params = CompressionParams.zstd();
    long dataLength = 1000;
    long compressedFileLength = 100;

    private CompressionMetadata newCompressionMetadata(Memory memory)
    {
        return new CompressionMetadata(chunksIndexFile,
                                       params,
                                       memory,
                                       memory.size(),
                                       dataLength,
                                       compressedFileLength);
    }

    @Test
    public void testMemoryIsFreed()
    {
        Memory memory = Memory.allocate(10);
        CompressionMetadata cm = newCompressionMetadata(memory);

        cm.close();
        assertThat(cm.isCleanedUp()).isTrue();
        assertThatExceptionOfType(AssertionError.class).isThrownBy(memory::size);
    }

    @Test
    public void testMemoryIsShared()
    {
        Memory memory = Memory.allocate(10);
        CompressionMetadata cm = newCompressionMetadata(memory);

        CompressionMetadata copy = cm.sharedCopy();
        assertThat(copy).isNotSameAs(cm);

        cm.close();
        assertThat(cm.isCleanedUp()).isFalse();
        assertThat(copy.isCleanedUp()).isFalse();
        assertThat(memory.size()).isEqualTo(10); // expected that no expection is thrown since memory should not be released yet

        copy.close();
        assertThat(cm.isCleanedUp()).isTrue();
        assertThat(copy.isCleanedUp()).isTrue();
        assertThatExceptionOfType(AssertionError.class).isThrownBy(memory::size);
    }
}