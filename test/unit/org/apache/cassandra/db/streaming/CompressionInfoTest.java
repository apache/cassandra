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

package org.apache.cassandra.db.streaming;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.serializers.SerializationUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionInfoTest
{
    private static final Chunk[] CHUNKS = { new Chunk(0, 100), new Chunk(104, 80) };

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void dictionary()
    {
        CompressionDictionary dictionary = new ZstdCompressionDictionary(new DictId(Kind.ZSTD, 42), new byte[]{ 1, 2, 3 });

        CompressionDictionary cycled = cycle(CompressionInfo.newInstance(CHUNKS, CompressionParams.zstd(4096, true), dictionary)).dictionary();

        assertThat(cycled.dictId()).isEqualTo(dictionary.dictId());
        assertThat(cycled.rawDictionary()).isEqualTo(dictionary.rawDictionary());
        assertThat(cycled.checksum()).isEqualTo(dictionary.checksum());
    }

    @Test
    public void noDictionary()
    {
        assertThat(cycle(CompressionInfo.newInstance(CHUNKS, CompressionParams.zstd(4096, true), null)).dictionary()).isNull();
    }

    @Test
    public void unchangedWithoutDictionaryCompressor()
    {
        CompressionParams parameters = CompressionParams.lz4();
        CompressionInfo info = CompressionInfo.newInstance(CHUNKS, parameters, null);
        int version = MessagingService.current_version;

        long chunks = TypeSizes.INT_SIZE + CHUNKS.length * Chunk.serializer.serializedSize(CHUNKS[0], version);
        assertThat(CompressionInfo.serializer.serializedSize(info, version))
        .isEqualTo(chunks + CompressionParams.serializer.serializedSize(parameters, version));
        assertThat(cycle(info).dictionary()).isNull();
    }

    private static CompressionInfo cycle(CompressionInfo info)
    {
        SerializationUtils.assertSerializationCycle(info, CompressionInfo.serializer);
        return SerializationUtils.cycleSerialization(info, CompressionInfo.serializer, MessagingService.current_version);
    }
}
