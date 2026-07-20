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

package org.apache.cassandra.schema;

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.utils.ClassLoadingTestNonAssignable;
import org.apache.cassandra.utils.ClassLoadingTestSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionParamsTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testIsDictionaryCompressionEnabled()
    {
        CompressionParams noCompression = CompressionParams.noCompression();
        assertThat(noCompression.isDictionaryCompressionEnabled())
        .as("No compression should not enable dictionary compression")
        .isFalse();

        CompressionParams regularZstd = CompressionParams.zstd();
        assertThat(regularZstd.isDictionaryCompressionEnabled())
        .as("Regular Zstd compression should not enable dictionary compression")
        .isFalse();

        CompressionParams zstdDictionary = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true);
        assertThat(zstdDictionary.isDictionaryCompressionEnabled())
        .as("Zstd dictionary compression should enable dictionary compression")
        .isTrue();

        CompressionParams lz4 = CompressionParams.lz4();
        assertThat(lz4.isDictionaryCompressionEnabled())
        .as("LZ4 compression should not enable dictionary compression")
        .isFalse();

        CompressionParams snappy = CompressionParams.snappy();
        assertThat(snappy.isDictionaryCompressionEnabled())
        .as("Snappy compression should not enable dictionary compression")
        .isFalse();

        CompressionParams deflate = CompressionParams.deflate();
        assertThat(deflate.isDictionaryCompressionEnabled())
        .as("Deflate compression should not enable dictionary compression")
        .isFalse();

        CompressionParams noop = CompressionParams.noop();
        assertThat(noop.isDictionaryCompressionEnabled())
        .as("Noop compression should not enable dictionary compression")
        .isFalse();
    }

    @Test
    public void testRejectsNonCompressorWithoutInitializing()
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);

        assertThatThrownBy(() -> CompressionParams.fromMap(Collections.singletonMap(CompressionParams.CLASS,
                                                                                    ClassLoadingTestNonAssignable.class.getName())))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("must extend or implement " + ICompressor.class.getName());

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }
}
