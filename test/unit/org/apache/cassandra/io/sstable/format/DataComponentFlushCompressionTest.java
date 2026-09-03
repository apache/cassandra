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

package org.apache.cassandra.io.sstable.format;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.FlushCompression;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.NoopCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.FlushCompressionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Resolution of {@link CompressionParams} for the data component from the table-level
 * {@code flush_compression} option and the {@code flush_compression} yaml setting.
 */
public class DataComponentFlushCompressionTest
{
    private static final String KS = "ks_flush_compression";
    private static final String CF = "cf_flush_compression";

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // option 'auto': yaml setting applies

    @Test
    public void autoFollowsYamlNone()
    {
        for (CompressionParams table : compressedParams())
            assertCompressor(NoopCompressor.class, resolve(table, "auto", FlushCompression.none));
    }

    @Test
    public void autoFollowsYamlTable()
    {
        for (CompressionParams table : compressedParams())
            assertThat(resolve(table, "auto", FlushCompression.table)).isSameAs(table);
    }

    @Test
    public void autoFollowsYamlFast()
    {
        CompressionParams lz4 = CompressionParams.lz4();
        assertThat(resolve(lz4, "auto", FlushCompression.fast)).isSameAs(lz4);
        assertThat(resolve(CompressionParams.zstd(), "auto", FlushCompression.fast)).isSameAs(CompressionParams.DEFAULT);
        assertThat(resolve(CompressionParams.deflate(), "auto", FlushCompression.fast)).isSameAs(CompressionParams.DEFAULT);
    }

    // explicit options: yaml setting ignored

    @Test
    public void noneOverridesYaml()
    {
        for (FlushCompression yaml : FlushCompression.values())
            for (CompressionParams table : compressedParams())
                assertCompressor(NoopCompressor.class, resolve(table, "none", yaml));
    }

    @Test
    public void tableOverridesYaml()
    {
        for (FlushCompression yaml : FlushCompression.values())
            for (CompressionParams table : compressedParams())
                assertThat(resolve(table, "table", yaml)).isSameAs(table);
    }

    @Test
    public void fastOverridesYaml()
    {
        CompressionParams lz4 = CompressionParams.lz4();
        for (FlushCompression yaml : FlushCompression.values())
        {
            assertThat(resolve(lz4, "fast", yaml)).isSameAs(lz4);
            assertCompressor(LZ4Compressor.class, resolve(lz4, "fast", yaml));
            assertThat(resolve(CompressionParams.zstd(), "fast", yaml)).isSameAs(CompressionParams.DEFAULT);
            assertThat(resolve(CompressionParams.deflate(), "fast", yaml)).isSameAs(CompressionParams.DEFAULT);
        }
    }

    @Test
    public void fastKeepsFastTableCompressorInstance()
    {
        CompressionParams lz4WithLargeChunks = CompressionParams.lz4(64 * 1024);
        for (FlushCompression yaml : FlushCompression.values())
            assertThat(resolve(lz4WithLargeChunks, "fast", yaml)).isSameAs(lz4WithLargeChunks);
    }

    // FLUSH only

    @Test
    public void nonFlushOperationsAlwaysUseTableCompression()
    {
        for (OperationType op : OperationType.values())
        {
            if (op == OperationType.FLUSH)
                continue;

            for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
                for (FlushCompression yaml : FlushCompression.values())
                    for (CompressionParams table : compressedParams())
                        assertThat(DataComponent.buildCompressionParams(metadata(table, option.name()), op, yaml))
                        .describedAs("%s with table option %s and yaml %s", op, option, yaml)
                        .isSameAs(table);
        }
    }

    @Test
    public void uncompressedTableUnaffected()
    {
        CompressionParams uncompressed = CompressionParams.noCompression();
        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
            for (FlushCompression yaml : FlushCompression.values())
                assertThat(resolve(uncompressed, option.name(), yaml)).isSameAs(uncompressed);
    }

    @Test
    public void defaultParamsResolveAsAuto()
    {
        TableMetadata metadata = TableMetadata.builder(KS, CF)
                                              .addPartitionKeyColumn("k", BytesType.instance)
                                              .compression(CompressionParams.zstd())
                                              .build();
        assertThat(metadata.params.flushCompression).isEqualTo(FlushCompressionParams.DEFAULT);

        assertCompressor(NoopCompressor.class, DataComponent.buildCompressionParams(metadata, OperationType.FLUSH, FlushCompression.none));
        assertCompressor(LZ4Compressor.class, DataComponent.buildCompressionParams(metadata, OperationType.FLUSH, FlushCompression.fast));
        assertCompressor(ZstdCompressor.class, DataComponent.buildCompressionParams(metadata, OperationType.FLUSH, FlushCompression.table));
    }

    private static CompressionParams resolve(CompressionParams table, String tableOption, FlushCompression yaml)
    {
        return DataComponent.buildCompressionParams(metadata(table, tableOption), OperationType.FLUSH, yaml);
    }

    private static TableMetadata metadata(CompressionParams compression, String flushCompression)
    {
        TableParams params = TableParams.builder()
                                        .compression(compression)
                                        .flushCompression(FlushCompressionParams.fromString(flushCompression))
                                        .build();
        return TableMetadata.builder(KS, CF)
                            .addPartitionKeyColumn("k", BytesType.instance)
                            .params(params)
                            .build();
    }

    private static CompressionParams[] compressedParams()
    {
        return new CompressionParams[]{ CompressionParams.lz4(), CompressionParams.zstd(), CompressionParams.deflate() };
    }

    private static void assertCompressor(Class<? extends ICompressor> expected, CompressionParams actual)
    {
        assertThat(actual.getSstableCompressor()).isInstanceOf(expected);
    }

    @Test
    public void fixtureRecommendedUses()
    {
        assertThat(CompressionParams.lz4().getSstableCompressor().recommendedUses()).contains(ICompressor.Uses.FAST_COMPRESSION);
        assertThat(CompressionParams.zstd().getSstableCompressor().recommendedUses()).doesNotContain(ICompressor.Uses.FAST_COMPRESSION);
        assertThat(CompressionParams.deflate().getSstableCompressor().recommendedUses()).doesNotContain(ICompressor.Uses.FAST_COMPRESSION);
        assertThat(CompressionParams.deflate().getSstableCompressor()).isInstanceOf(DeflateCompressor.class);
    }
}
