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

package org.apache.cassandra.db.guardrails;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ZstdCompressorBase;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GuardrailZstdCompressionLevelTest extends GuardrailTester
{
    public GuardrailZstdCompressionLevelTest()
    {
        super(Guardrails.zstdCompressionLevelThreshold);
    }

    @After
    public void disableThresholds()
    {
        guardrails().setZstdCompressionLevelThreshold(-1, -1);
    }

    @Test
    public void rejectsNegativeOtherThanDisabled()
    {
        assertThatThrownBy(() -> guardrails().setZstdCompressionLevelThreshold(-2, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("zstd_compression_level_warn_threshold");

        assertThatThrownBy(() -> guardrails().setZstdCompressionLevelThreshold(1, -2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("zstd_compression_level_fail_threshold");
    }

    @Test
    public void rejectsWarnAboveFail()
    {
        assertThatThrownBy(() -> guardrails().setZstdCompressionLevelThreshold(5, 3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("should be lower than the fail threshold");
    }

    @Test
    public void skipsOrderingWhenEitherIsDisabled()
    {
        guardrails().setZstdCompressionLevelThreshold(-1, 3);
        guardrails().setZstdCompressionLevelThreshold(5, -1);
        guardrails().setZstdCompressionLevelThreshold(-1, -1);
    }

    @Test
    public void warnsOnCreateTable() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(5, 10);
        assertWarns(createTable(6), "Value of Zstd compression level is '6'");
        assertValid(createTable(5));
    }

    @Test
    public void failsOnCreateTable() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(5, 10);
        assertFails(createTable(11), "Value of Zstd compression level is '11'");
    }

    @Test
    public void warnsAndFailsOnAlterTable() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(-1, -1);
        createTable("CREATE TABLE %s (k int PRIMARY KEY) WITH compression = " +
                    "{'class': 'ZstdCompressor', 'compression_level': '3'}");

        guardrails().setZstdCompressionLevelThreshold(5, 10);
        assertWarns(alterTable(6), "Value of Zstd compression level is '6'");
        assertFails(alterTable(11), "Value of Zstd compression level is '11'");
        assertValid(alterTable(4));
    }

    @Test
    public void appliesToDictionaryCompressorToo() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(5, 10);
        assertFails(format("CREATE TABLE %s.%s (k int PRIMARY KEY) WITH compression = " +
                           "{'class': 'ZstdDictionaryCompressor', 'compression_level': '11'}",
                           keyspace(), createTableName()),
                    "Value of Zstd compression level is '11'");
    }

    @Test
    public void ignoresNonZstdCompressor() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(0, 0);
        assertValid(format("CREATE TABLE %s.%s (k int PRIMARY KEY) WITH compression = {'class': 'LZ4Compressor'}",
                           keyspace(), createTableName()));
    }

    @Test
    public void defaultLevelIsGuardedToo() throws Throwable
    {
        // no compression_level given, so the compressor's default applies and is still subject to the guardrail
        guardrails().setZstdCompressionLevelThreshold(1, 2);
        assertFails(format("CREATE TABLE %s.%s (k int PRIMARY KEY) WITH compression = {'class': 'ZstdCompressor'}",
                           keyspace(), createTableName()),
                    "Value of Zstd compression level is '3'");
    }

    @Test
    public void createsTableAndKeepsTheLevel() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(10, 20);

        String table = createTableName();
        assertValid(format("CREATE TABLE %s.%s (k int PRIMARY KEY) WITH compression = " +
                           "{'class': 'ZstdCompressor', 'compression_level': '9'}", keyspace(), table));

        assertThat(levelOf(table)).isEqualTo(9);
    }

    @Test
    public void disabledThresholdsAllowAnyLevel() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(-1, -1);

        String table = createTableName();
        assertValid(format("CREATE TABLE %s.%s (k int PRIMARY KEY) WITH compression = " +
                           "{'class': 'ZstdCompressor', 'compression_level': '-22'}", keyspace(), table));

        assertThat(levelOf(table)).isEqualTo(-22);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        guardrails().setZstdCompressionLevelThreshold(5, 10);
        testExcludedUsers(() -> createTable(6), () -> createTable(11));
    }

    private int levelOf(String table)
    {
        ICompressor compressor = getColumnFamilyStore(keyspace(), table).metadata().params.compression.getSstableCompressor();
        assertThat(compressor).isInstanceOf(ZstdCompressorBase.class);
        return ((ZstdCompressorBase) compressor).compressionLevel();
    }

    private String createTable(int level)
    {
        return format("CREATE TABLE %s.%s (k int PRIMARY KEY) WITH compression = " +
                      "{'class': 'ZstdCompressor', 'compression_level': '%d'}",
                      keyspace(), createTableName(), level);
    }

    private String alterTable(int level)
    {
        return format("ALTER TABLE %s.%s WITH compression = " +
                      "{'class': 'ZstdCompressor', 'compression_level': '%d'}",
                      keyspace(), currentTable(), level);
    }
}
