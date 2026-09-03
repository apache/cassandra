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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.FlushCompressionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code flush_compression} table option: CREATE TABLE, ALTER TABLE, validation, CQL string round trip.
 */
public class FlushCompressionTableOptionTest extends CQLTester
{
    @Test
    public void optionIsValidKeyword()
    {
        assertThat(TableAttributes.validKeywords()).contains("flush_compression");
        assertThat(TableParams.Option.FLUSH_COMPRESSION.toString()).isEqualTo("flush_compression");
    }

    @Test
    public void defaultIsAuto()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        assertFlushCompression(FlushCompressionParams.Option.auto);
        assertThat(currentParams().flushCompression).isEqualTo(FlushCompressionParams.DEFAULT);
    }

    @Test
    public void createTableWithEveryOption()
    {
        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = '" + option + '\'');
            assertFlushCompression(option);
        }
    }

    @Test
    public void createWithOtherOptions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) " +
                    "WITH compression = {'class': 'ZstdCompressor'} " +
                    "AND flush_compression = 'table' " +
                    "AND comment = 'flush compression test' " +
                    "AND gc_grace_seconds = 100");
        assertFlushCompression(FlushCompressionParams.Option.table);
        assertThat(currentParams().comment).isEqualTo("flush compression test");
        assertThat(currentParams().gcGraceSeconds).isEqualTo(100);
        assertThat(currentParams().compression.getSstableCompressor().getClass().getSimpleName()).isEqualTo("ZstdCompressor");
    }

    @Test
    public void alterChangesOption()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        assertFlushCompression(FlushCompressionParams.Option.auto);

        alterTable("ALTER TABLE %s WITH flush_compression = 'none'");
        assertFlushCompression(FlushCompressionParams.Option.none);

        alterTable("ALTER TABLE %s WITH flush_compression = 'fast'");
        assertFlushCompression(FlushCompressionParams.Option.fast);

        alterTable("ALTER TABLE %s WITH flush_compression = 'table'");
        assertFlushCompression(FlushCompressionParams.Option.table);

        alterTable("ALTER TABLE %s WITH flush_compression = 'auto'");
        assertFlushCompression(FlushCompressionParams.Option.auto);
    }

    @Test
    public void alterOtherOptionPreservesFlushCompression()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = 'none'");
        assertFlushCompression(FlushCompressionParams.Option.none);

        alterTable("ALTER TABLE %s WITH comment = 'unrelated change'");
        assertFlushCompression(FlushCompressionParams.Option.none);
        assertThat(currentParams().comment).isEqualTo("unrelated change");

        alterTable("ALTER TABLE %s WITH compression = {'class': 'ZstdCompressor'}");
        assertFlushCompression(FlushCompressionParams.Option.none);

        alterTable("ALTER TABLE %s ADD v2 int");
        assertFlushCompression(FlushCompressionParams.Option.none);
    }

    @Test
    public void createRejectsUnknownValue() throws Throwable
    {
        assertInvalidThrowMessage("Invalid value used for flush compression parameter: bogus",
                                  ConfigurationException.class,
                                  "CREATE TABLE " + KEYSPACE + ".flush_compression_invalid (k int PRIMARY KEY, v text) WITH flush_compression = 'bogus'");
    }

    @Test
    public void createRejectsEmptyValue() throws Throwable
    {
        assertInvalidThrow(ConfigurationException.class,
                           "CREATE TABLE " + KEYSPACE + ".flush_compression_empty (k int PRIMARY KEY, v text) WITH flush_compression = ''");
    }

    @Test
    public void createRejectsMapValue() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class,
                           "CREATE TABLE " + KEYSPACE + ".flush_compression_map (k int PRIMARY KEY, v text) WITH flush_compression = {'class': 'none'}");
    }

    @Test
    public void createRejectsUnquotedValue() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class,
                           "CREATE TABLE " + KEYSPACE + ".flush_compression_unquoted (k int PRIMARY KEY, v text) WITH flush_compression = none");
    }

    @Test
    public void alterRejectsUnknownValueAndKeepsPrevious() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = 'fast'");
        assertInvalidThrowMessage("Invalid value used for flush compression parameter: bogus",
                                  ConfigurationException.class,
                                  "ALTER TABLE %s WITH flush_compression = 'bogus'");
        assertFlushCompression(FlushCompressionParams.Option.fast);
    }

    @Test
    public void cqlStringContainsQuotedOption()
    {
        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = '" + option + '\'');
            String cql = currentMetadata().toCqlString(false, false, false);
            assertThat(cql).contains("AND flush_compression = '" + option + "'\n");
            assertThat(cql).contains("AND extensions = {}\n    AND flush_compression = '" + option + "'\n    AND gc_grace_seconds");
        }
    }

    @Test
    public void cqlStringRoundTrip()
    {
        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
        {
            String source = createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = '" + option + '\'');
            String cql = currentMetadata().toCqlString(false, false, false);

            String copy = createTable(cql.replace(KEYSPACE + '.' + source, "%s").replace(source, "%s"));
            assertThat(copy).isNotEqualTo(source);
            assertFlushCompression(option);
        }
    }

    @Test
    public void tableParamsToStringContainsOption()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = 'none'");
        assertThat(currentParams().toString()).contains("flush_compression=none");
    }

    @Test
    public void unbuildPreservesOption()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH flush_compression = 'table'");
        TableParams rebuilt = currentParams().unbuild().build();
        assertThat(rebuilt.flushCompression.configurationKey).isEqualTo(FlushCompressionParams.Option.table);
        assertThat(rebuilt).isEqualTo(currentParams());
    }

    private void assertFlushCompression(FlushCompressionParams.Option expected)
    {
        assertThat(currentParams().flushCompression.configurationKey).isEqualTo(expected);
    }

    private TableMetadata currentMetadata()
    {
        return getCurrentColumnFamilyStore().metadata();
    }

    private TableParams currentParams()
    {
        return currentMetadata().params;
    }
}
