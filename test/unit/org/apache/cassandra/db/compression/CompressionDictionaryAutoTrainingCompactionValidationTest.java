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

package org.apache.cassandra.db.compression;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the schema-level guard that compression dictionary auto-training ({@code auto_training_enabled = true})
 * is only permitted on tables using {@code TimeWindowCompactionStrategy}: both CREATE TABLE and ALTER TABLE accept
 * it on a TWCS table and reject it on any other compaction strategy.
 */
public class CompressionDictionaryAutoTrainingCompactionValidationTest extends CQLTester
{
    private static final String DICT_AUTO = "{'class':'ZstdDictionaryCompressor','auto_training_enabled':'true'}";
    private static final String DICT_PLAIN = "{'class':'ZstdDictionaryCompressor'}";
    private static final String TWCS = "{'class':'TimeWindowCompactionStrategy'}";
    private static final String STCS = "{'class':'SizeTieredCompactionStrategy'}";
    private static final String EXPECTED = "only supported on tables using TimeWindowCompactionStrategy";

    @Test
    public void createAllowsAutoTrainingOnTwcs()
    {
        // must not throw
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);
    }

    @Test
    public void createRejectsAutoTrainingOnNonTwcs()
    {
        assertThatThrownBy(() ->
            createTableMayThrow("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                                "WITH compression = " + DICT_AUTO + " AND compaction = " + STCS))
            .hasMessageContaining(EXPECTED);
    }

    @Test
    public void alterAllowsEnablingAutoTrainingOnTwcs()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_PLAIN + " AND compaction = " + TWCS);
        // enabling auto-training on an existing TWCS table must not throw
        alterTable("ALTER TABLE %s WITH compression = " + DICT_AUTO);
    }

    @Test
    public void alterRejectsEnablingAutoTrainingOnNonTwcs()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_PLAIN + " AND compaction = " + STCS);
        assertThatThrownBy(() ->
            alterTableMayThrow("ALTER TABLE %s WITH compression = " + DICT_AUTO))
            .hasMessageContaining(EXPECTED);
    }

    @Test
    public void createLikeAllowsAutoTrainingInheritedOnTwcs()
    {
        String source = createTable(KEYSPACE, "CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                                              "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);
        createTableLike("CREATE TABLE %s LIKE %s", source, KEYSPACE, KEYSPACE);
    }

    /**
     * Compression carrying auto-training is inherited from the source while the WITH clause overrides compaction,
     * so neither the source params nor the WITH clause alone reveals the unsupported combination.
     */
    @Test
    public void createLikeRejectsInheritedAutoTrainingWhenCompactionOverriddenToNonTwcs()
    {
        String source = createTable(KEYSPACE, "CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                                              "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);
        assertThatThrownBy(() ->
            createTableLike("CREATE TABLE %s LIKE %s WITH compaction = " + STCS, source, KEYSPACE, KEYSPACE))
            .hasStackTraceContaining(EXPECTED);
    }

    /**
     * The mirror image: compaction is inherited and non-TWCS, auto-training arrives via the WITH clause.
     */
    @Test
    public void createLikeRejectsAutoTrainingAddedOnInheritedNonTwcs()
    {
        String source = createTable(KEYSPACE, "CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                                              "WITH compression = " + DICT_PLAIN + " AND compaction = " + STCS);
        assertThatThrownBy(() ->
            createTableLike("CREATE TABLE %s LIKE %s WITH compression = " + DICT_AUTO, source, KEYSPACE, KEYSPACE))
            .hasStackTraceContaining(EXPECTED);
    }
}
