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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.NoopCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.FlushCompressionParams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-table {@code FlushCompression} attribute of the table MBean, a node local override in the same style as
 * {@code CompressionParameters}.
 */
public class ColumnFamilyStoreFlushCompressionTest extends CQLTester
{
    @Test
    public void everyValueRoundTrips()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getFlushCompression()).isEqualTo("auto");

        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
        {
            cfs.setFlushCompression(option.name());
            assertThat(cfs.getFlushCompression()).isEqualTo(option.name());
            // the override lives in the local metadata only; cfs.metadata() deliberately returns the schema view,
            // and the flush path reads getTableMetadataRef().getLocal()
            assertThat(cfs.metadata.getLocal().params.flushCompression.configurationKey).isEqualTo(option);
            assertThat(cfs.metadata().params.flushCompression.configurationKey)
            .describedAs("schema view is untouched by a local override")
            .isEqualTo(FlushCompressionParams.Option.auto);
        }
    }

    @Test
    public void unknownValueIsRejectedAndPreviousIsKept()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.setFlushCompression("table");

        assertThatThrownBy(() -> cfs.setFlushCompression("bogus"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid value used for flush compression parameter: bogus");

        assertThat(cfs.getFlushCompression()).isEqualTo("table");
    }

    @Test
    public void overrideAppliesToTheNextFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'} " +
                    "AND flush_compression = 'fast'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, "a");
        flush();
        assertThat(compressorOfNewest(cfs)).isEqualTo(LZ4Compressor.class);

        cfs.setFlushCompression("table");
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 2, "b");
        flush();
        assertThat(compressorOfNewest(cfs)).isEqualTo(ZstdCompressor.class);

        cfs.setFlushCompression("none");
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 3, "c");
        flush();
        assertThat(compressorOfNewest(cfs)).isEqualTo(NoopCompressor.class);
    }

    /**
     * The override is local metadata only, so a schema change rebuilds the table metadata and drops it. This is the
     * same behaviour as a {@code CompressionParameters} override.
     */
    @Test
    public void schemaChangeDiscardsTheOverride() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = 'fast'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        cfs.setFlushCompression("none");
        assertThat(cfs.getFlushCompression()).isEqualTo("none");

        alterTable("ALTER TABLE %s WITH comment = 'unrelated change'");

        assertThat(getCurrentColumnFamilyStore().getFlushCompression())
        .describedAs("a schema change drops the local override and restores the schema value")
        .isEqualTo("fast");
    }

    private static Class<?> compressorOfNewest(ColumnFamilyStore cfs)
    {
        SSTableReader newest = null;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (newest == null || sstable.descriptor.id.toString().compareTo(newest.descriptor.id.toString()) > 0)
                newest = sstable;

        assertThat(newest).isNotNull();
        return newest.getCompressionMetadata().parameters.getSstableCompressor().getClass();
    }
}
