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
package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * This test is primarily handling edge conditions, error conditions
 * and basic functionality. Comprehensive type testing of collections
 * is in the cql/types/collections package
 */
public class CollectionIndexingTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void indexMap()
    {
        createPopulatedMap(createIndexDDL("value"));
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS 'v1'").size());
    }

    @Test
    public void indexMapKeys()
    {
        createPopulatedMap(createIndexDDL("KEYS(value)"));
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS KEY 1").size());
    }

    @Test
    public void indexMapValues()
    {
        createPopulatedMap(createIndexDDL("VALUES(value)"));
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS 'v1'").size());
    }

    @Test
    public void indexMapEntries()
    {
        createPopulatedMap(createIndexDDL("ENTRIES(value)"));
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] = 'v1'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE value[1] = 'v1' AND value[2] = 'v2'").size());
    }

    @Test
    public void indexFrozenList() throws Throwable
    {
        createPopulatedFrozenList(createIndexDDL("FULL(value)"));
        beforeAndAfterFlush(() -> {
            assertEquals(2, execute("SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2, 3)).size());
        });
    }

    @Test
    public void indexFrozenMap() throws Throwable
    {
        createPopulatedFrozenMap(createIndexDDL("FULL(value)"));
        beforeAndAfterFlush(() -> {
            assertEquals(1, execute("SELECT * FROM %s WHERE value = ?", new HashMap<Integer, String>() {{
                put(1, "v1");
                put(2, "v2");
            }}).size());
        });
    }

    @Test
    public void indexFrozenMapQueryKeys() throws Throwable
    {
        createPopulatedFrozenMap(createIndexDDL("FULL(value)"));
        beforeAndAfterFlush(() -> {
            assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value contains key 1");
        });
    }

    @Test
    public void indexFrozenMapQueryValues() throws Throwable
    {
        createPopulatedFrozenMap(createIndexDDL("FULL(value)"));
        beforeAndAfterFlush(() -> {
            assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value contains 'v1'");
        });
    }

    @Test
    public void indexFrozenMapQueryEntries() throws Throwable
    {
        createPopulatedFrozenMap(createIndexDDL("FULL(value)"));
        beforeAndAfterFlush(() -> {
            assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value[1] = 'v1'");
        });
    }

    @Test
    public void indexMapEntriesQueryEq() throws Throwable
    {
        createPopulatedMap(createIndexDDL("ENTRIES(value)"));
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapEntriesQueryKeys() throws Throwable
    {
        createPopulatedMap(createIndexDDL("ENTRIES(value)"));
        assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value contains key 1");
    }

    @Test
    public void indexMapEntriesQueryValues() throws Throwable
    {
        createPopulatedMap(createIndexDDL("ENTRIES(value)"));
        assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value contains 'v1'");
    }

    @Test
    public void indexMapKeysQueryEq() throws Throwable
    {
        createPopulatedMap(createIndexDDL("KEYS(value)"));
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapKeysQueryValues() throws Throwable
    {
        createPopulatedMap(createIndexDDL("KEYS(value)"));
        assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value contains 'v1'");
    }

    @Test
    public void indexMapKeysQueryEntries() throws Throwable
    {
        createPopulatedMap(createIndexDDL("KEYS(value)"));
        assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value[1] = 'v1'");
    }

    @Test
    public void indexMapValuesQueryEq() throws Throwable
    {
        createPopulatedMap(createIndexDDL("VALUES(value)"));
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapValuesQueryKeys() throws Throwable
    {
        createPopulatedMap(createIndexDDL("VALUES(value)"));
        assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value contains key 1");
    }

    @Test
    public void indexMapValuesQueryEntries() throws Throwable
    {
        createPopulatedMap(createIndexDDL("VALUES(value)"));
        assertUnsupportedIndexOperator(2, "SELECT * FROM %s WHERE value[1] = 'v1'");
    }

    @Test
    public void unindexedContainsExpressions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, m map<int, int>)");
        createIndex("CREATE INDEX ON %s(v) USING 'SAI'"); // just to make sure that SAI is involved

        Object[] row = row(0, 1, map(2, 3));
        execute("INSERT INTO %s (k, v, m) VALUES (?, ?, ?)", row);
        execute("INSERT INTO %s (k, v, m) VALUES (?, ?, ?)", 1, 1, map(12, 13));

        // try without any indexes on the map
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS 3 ALLOW FILTERING"), row);
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS KEY 2 ALLOW FILTERING"), row);
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS KEY 2 AND m CONTAINS 3 ALLOW FILTERING"), row);

        // try with index on map values
        createIndex("CREATE INDEX ON %s(m) USING 'SAI'");
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS 3"), row);
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS KEY 2 ALLOW FILTERING"), row);
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS KEY 2 AND m CONTAINS 3 ALLOW FILTERING"), row);

        // try with index on map keys
        createIndex("CREATE INDEX ON %s(KEYS(m)) USING 'SAI'");
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS 3"), row);
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS KEY 2"), row);
        assertRows(execute("SELECT k, v, m FROM %s WHERE v = 1 AND m CONTAINS KEY 2 AND m CONTAINS 3"), row);
    }

    @Test
    public void testFrozenListFullIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_list frozen<list<int>>)");

        execute("INSERT INTO %s (pk, frozen_list) VALUES (1, [1, 2, 3])");
        execute("INSERT INTO %s (pk, frozen_list) VALUES (2, [1, 2, 3])");
        execute("INSERT INTO %s (pk, frozen_list) VALUES (3, [4, 5, 6])");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list = ? ALLOW FILTERING", Arrays.asList(1, 2, 3)).size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_list)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list = ?", Arrays.asList(1, 2, 3));
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListValuesIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_list frozen<list<int>>)");

        execute("INSERT INTO %s (pk, frozen_list) VALUES (1, [1, 2, 3])");
        execute("INSERT INTO %s (pk, frozen_list) VALUES (2, [3, 4, 5])");
        execute("INSERT INTO %s (pk, frozen_list) VALUES (3, [4, 5, 6])");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list CONTAINS 3 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(frozen_list)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list CONTAINS 3");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenSetFullIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_set frozen<set<text>>)");

        execute("INSERT INTO %s (pk, frozen_set) VALUES (1, {'apple', 'banana'})");
        execute("INSERT INTO %s (pk, frozen_set) VALUES (2, {'apple', 'banana'})");
        execute("INSERT INTO %s (pk, frozen_set) VALUES (3, {'cherry', 'date'})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_set = ? ALLOW FILTERING", Arrays.asList("apple", "banana")).size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_set)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_set = ?", Arrays.asList("apple", "banana"));
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenSetValuesIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_set frozen<set<text>>)");

        execute("INSERT INTO %s (pk, frozen_set) VALUES (1, {'apple', 'banana'})");
        execute("INSERT INTO %s (pk, frozen_set) VALUES (2, {'banana', 'cherry'})");
        execute("INSERT INTO %s (pk, frozen_set) VALUES (3, {'cherry', 'date'})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_set CONTAINS 'banana' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(frozen_set)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_set CONTAINS 'banana'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapFullIndexEquality() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<text, text>>)");


        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, {'k1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, {'k1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, {'k3': 'v3', 'k4': 'v4'})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_map = ? ALLOW FILTERING", ImmutableMap.of("k1", "v1", "k2", "v2")).size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_map)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map = ?", ImmutableMap.of("k1", "v1", "k2", "v2"));
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapFullIndexMapEntryRequiresFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<int, text>>)");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of(1, "v1", 2, "v2"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, ?)", ImmutableMap.of(1, "v1", 3, "v3"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, ?)", ImmutableMap.of(3, "v3", 4, "v4"));

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_map[1] = 'v1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_map)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT pk FROM %s WHERE frozen_map[1] = 'v1'");

            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map[1] = 'v1' ALLOW FILTERING");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapEntriesIndexMapEntry() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<int, text>>)");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of(1, "v1", 2, "v2"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, ?)", ImmutableMap.of(1, "v1", 3, "v3"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, ?)", ImmutableMap.of(3, "v3", 4, "v4"));

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_map[1] = 'v1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map[1] = 'v1'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapValuesIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<text, text>>)");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, {'k1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, {'k3': 'v1', 'k4': 'v3'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, {'k5': 'v3', 'k6': 'v4'})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_map CONTAINS 'v1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(frozen_map)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map CONTAINS 'v1'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapKeysIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<text, text>>)");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, {'k1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, {'k1': 'v3', 'k4': 'v4'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, {'k5': 'v5', 'k6': 'v6'})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_map CONTAINS KEY 'k1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(KEYS(frozen_map)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map CONTAINS KEY 'k1'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testDifferentIndexTypesOnDifferentColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, " +
                    "map_full frozen<map<text, text>>, " +
                    "map_values frozen<map<text, text>>, " +
                    "map_keys frozen<map<text, text>>, " +
                    "map_entries frozen<map<text, text>>)");


        execute("INSERT INTO %s (pk, map_full, map_values, map_keys, map_entries) " +
                "VALUES (1, {'k1': 'v1', 'k2': 'v2'}, {'k1': 'v1', 'k2': 'v2'}, {'k1': 'v1', 'k2': 'v2'}, {'k1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (pk, map_full, map_values, map_keys, map_entries) " +
                "VALUES (2, {'k1': 'v1', 'k2': 'v2'}, {'k3': 'v1', 'k4': 'v3'}, {'k1': 'v3', 'k5': 'v5'}, {'k1': 'v1', 'k6': 'v6'})");
        execute("INSERT INTO %s (pk, map_full, map_values, map_keys, map_entries) " +
                "VALUES (3, {'k3': 'v3', 'k4': 'v4'}, {'k5': 'v5', 'k6': 'v6'}, {'k7': 'v7', 'k8': 'v8'}, {'k9': 'v9', 'k10': 'v10'})");

        assertEquals(2,execute("SELECT pk FROM %s WHERE map_full = ? ALLOW FILTERING", ImmutableMap.of("k1", "v1", "k2", "v2")).size());
        assertEquals(2,execute("SELECT pk FROM %s WHERE map_values CONTAINS 'v1' ALLOW FILTERING").size());
        assertEquals(2,execute("SELECT pk FROM %s WHERE map_keys CONTAINS KEY 'k1' ALLOW FILTERING").size());
        assertEquals(2,execute("SELECT pk FROM %s WHERE map_entries['k1'] = 'v1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX idx_full ON %s(FULL(map_full)) USING 'sai'");
        createIndex("CREATE INDEX idx_values ON %s(VALUES(map_values)) USING 'sai'");
        createIndex("CREATE INDEX idx_keys ON %s(KEYS(map_keys)) USING 'sai'");
        createIndex("CREATE INDEX idx_entries ON %s(ENTRIES(map_entries)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE map_full = ?", ImmutableMap.of("k1", "v1", "k2", "v2"));
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE map_values CONTAINS 'v1'");
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE map_keys CONTAINS KEY 'k1'");
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE map_entries['k1'] = 'v1'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFullIndexDoesNotSupportContainsOperations() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_list frozen<list<int>>)");

        execute("INSERT INTO %s (pk, frozen_list) VALUES (1, [1, 2, 3])");

        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_list CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_list)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT pk FROM %s WHERE frozen_list CONTAINS 1");
        });
    }

    @Test
    public void testValuesIndexDoesNotSupportEquality() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_list frozen<list<int>>)");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_list)) USING 'sai'");

        execute("INSERT INTO %s (pk, frozen_list) VALUES (1, [1, 2, 3])");

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT pk FROM %s WHERE frozen_list = ?", Arrays.asList(1, 2, 3));
        });
    }

    @Test
    public void testMapEntryWithAllowFilteringDifferentKeyTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<int, text>>)");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of(1, "v1", 2, "v2"));

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map[1] = 'v1' ALLOW FILTERING");
            assertEquals(1, rows.all().size());
        });

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<text, text>>)");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of("k1", "v1", "k2", "v2"));

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map['k1'] = 'v1' ALLOW FILTERING");
            assertEquals(1, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapValuesIndexWithMapEntryQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<int, text>>)");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_map)) USING 'sai'");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of(1, "v1", 2, "v2"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, ?)", ImmutableMap.of(1, "v1", 3, "v3"));

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT pk FROM %s WHERE frozen_map[1] = 'v1'");

            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map[1] = 'v1' ALLOW FILTERING");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapKeysIndexWithMapEntryQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<int, text>>)");
        createIndex("CREATE INDEX ON %s(KEYS(frozen_map)) USING 'sai'");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of(1, "v1", 2, "v2"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, ?)", ImmutableMap.of(1, "v1", 3, "v3"));

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT pk FROM %s WHERE frozen_map[1] = 'v1'");

            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map[1] = 'v1' ALLOW FILTERING");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapDefaultIndexType() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<int, text>>)");
        createIndex("CREATE INDEX ON %s(frozen_map) USING 'sai'");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of(1, "v1", 2, "v2"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, ?)", ImmutableMap.of(1, "v1", 3, "v3"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, ?)", ImmutableMap.of(4, "v4", 5, "v5"));

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map CONTAINS 'v1'");
            assertEquals(2, rows.all().size());

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT pk FROM %s WHERE frozen_map = ?", ImmutableMap.of(1, "v1", 2, "v2"));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT pk FROM %s WHERE frozen_map[1] = 'v1'");

            rows = executeNet("SELECT pk FROM %s WHERE frozen_map[1] = 'v1' ALLOW FILTERING");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testMapEntryQueryWithNullAndEmptyCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<int, text>>)");
        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map)) USING 'sai'");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, ?)", ImmutableMap.of(1, "v1", 2, "v2"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, ?)", ImmutableMap.of(3, "v3", 4, "v4"));
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, NULL)");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (4, ?)", ImmutableMap.of());
        execute("INSERT INTO %s (pk, frozen_map) VALUES (5, ?)", ImmutableMap.of(1, "different_value"));

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map[1] = 'v1'");
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s");
            assertEquals(5, rows.all().size());
        });
    }

    @Test
    public void testFullIndexWithNullAndEmptyCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_list frozen<list<int>>)");
        createIndex("CREATE INDEX ON %s(FULL(frozen_list)) USING 'sai'");

        execute("INSERT INTO %s (pk, frozen_list) VALUES (1, ?)", Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, frozen_list) VALUES (2, ?)", Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, frozen_list) VALUES (3, ?)", Arrays.asList(4, 5, 6));
        execute("INSERT INTO %s (pk, frozen_list) VALUES (4, NULL)");
        execute("INSERT INTO %s (pk, frozen_list) VALUES (5, ?)", List.of());

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list = ?", Arrays.asList(1, 2, 3));
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_list = ?", List.of());
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s");
            assertEquals(5, rows.all().size());
        });
    }

    @Test
    public void testFrozenCollectionsWithNumericTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, " +
                    "frozen_list_bigint frozen<list<bigint>>, " +
                    "frozen_set_smallint frozen<set<smallint>>, " +
                    "frozen_map_tinyint frozen<map<tinyint, text>>, " +
                    "frozen_list_varint frozen<list<varint>>)");

        execute("INSERT INTO %s (pk, frozen_list_bigint, frozen_set_smallint, frozen_map_tinyint, frozen_list_varint) " +
                "VALUES (1, [1, 2, 3], {10, 20}, {1: 'v1', 2: 'v2'}, [100, 200])");
        execute("INSERT INTO %s (pk, frozen_list_bigint, frozen_set_smallint, frozen_map_tinyint, frozen_list_varint) " +
                "VALUES (2, [1, 2, 3], {30, 40}, {3: 'v3'}, [300])");
        execute("INSERT INTO %s (pk, frozen_list_bigint, frozen_set_smallint, frozen_map_tinyint, frozen_list_varint) " +
                "VALUES (3, [], {}, {}, [])");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list_bigint = ? ALLOW FILTERING", Arrays.asList(1L, 2L, 3L)).size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_set_smallint CONTAINS ? ALLOW FILTERING", (short)10).size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map_tinyint[?] = 'v1' ALLOW FILTERING", (byte)1).size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_list_varint CONTAINS 100 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_list_bigint)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_set_smallint)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map_tinyint)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_list_varint)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list_bigint = ?", Arrays.asList(1L, 2L, 3L));
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_set_smallint CONTAINS ?", (short)10);
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_map_tinyint[?] = 'v1'", (byte)1);
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_list_varint CONTAINS 100");
            assertEquals(1, rows.all().size());
        });
    }

    @Test
    public void testFrozenCollectionsWithDecimalTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, " +
                    "frozen_list_float frozen<list<float>>, " +
                    "frozen_set_double frozen<set<double>>, " +
                    "frozen_map_decimal frozen<map<decimal, text>>)");

        execute("INSERT INTO %s (pk, frozen_list_float, frozen_set_double, frozen_map_decimal) " +
                "VALUES (1, [1.1, 2.2], {10.5, 20.5}, {1.0: 'v1', 2.0: 'v2'})");
        execute("INSERT INTO %s (pk, frozen_list_float, frozen_set_double, frozen_map_decimal) " +
                "VALUES (2, [1.1, 2.2], {30.5}, {3.0: 'v3'})");
        execute("INSERT INTO %s (pk, frozen_list_float, frozen_set_double, frozen_map_decimal) " +
                "VALUES (3, [], {}, {})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list_float = ? ALLOW FILTERING", Arrays.asList(1.1f, 2.2f)).size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_set_double CONTAINS 10.5 ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map_decimal[1.0] = 'v1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_list_float)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_set_double)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map_decimal)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list_float = ?", Arrays.asList(1.1f, 2.2f));
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_set_double CONTAINS 10.5");
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_map_decimal[1.0] = 'v1'");
            assertEquals(1, rows.all().size());
        });
    }

    @Test
    public void testFrozenCollectionsWithTextVariants() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, " +
                    "frozen_list_varchar frozen<list<varchar>>, " +
                    "frozen_set_ascii frozen<set<ascii>>, " +
                    "frozen_map_text frozen<map<text, varchar>>)");

        execute("INSERT INTO %s (pk, frozen_list_varchar, frozen_set_ascii, frozen_map_text) " +
                "VALUES (1, ['apple', 'banana'], {'hello', 'world'}, {'key1': 'value1', 'key2': 'value2'})");
        execute("INSERT INTO %s (pk, frozen_list_varchar, frozen_set_ascii, frozen_map_text) " +
                "VALUES (2, ['apple', 'banana'], {'test'}, {'key3': 'value3'})");
        execute("INSERT INTO %s (pk, frozen_list_varchar, frozen_set_ascii, frozen_map_text) " +
                "VALUES (3, [], {}, {})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list_varchar = ? ALLOW FILTERING", Arrays.asList("apple", "banana")).size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_set_ascii CONTAINS 'hello' ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map_text['key1'] = 'value1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_list_varchar)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_set_ascii)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map_text)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list_varchar = ?", Arrays.asList("apple", "banana"));
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_set_ascii CONTAINS 'hello'");
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_map_text['key1'] = 'value1'");
            assertEquals(1, rows.all().size());
        });
    }

    @Test
    public void testFrozenCollectionsWithTimeTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, " +
                    "frozen_list_timestamp frozen<list<timestamp>>, " +
                    "frozen_set_date frozen<set<date>>, " +
                    "frozen_map_time frozen<map<time, text>>, " +
                    "frozen_list_timeuuid frozen<list<timeuuid>>)");

        execute("INSERT INTO %s (pk, frozen_list_timestamp, frozen_set_date, frozen_map_time, frozen_list_timeuuid) " +
                "VALUES (1, ['2023-01-01 00:00:00+0000', '2023-01-02 00:00:00+0000'], {'2023-01-01', '2023-01-02'}, " +
                "{'12:00:00': 'noon', '18:00:00': 'evening'}, [50554d6e-29bb-11e5-b345-feff819cdc9f])");
        execute("INSERT INTO %s (pk, frozen_list_timestamp, frozen_set_date, frozen_map_time, frozen_list_timeuuid) " +
                "VALUES (2, ['2023-01-01 00:00:00+0000', '2023-01-02 00:00:00+0000'], {'2023-01-03'}, " +
                "{'06:00:00': 'morning'}, [50554d6e-29bb-11e5-b345-feff819cdc9f])");
        execute("INSERT INTO %s (pk, frozen_list_timestamp, frozen_set_date, frozen_map_time, frozen_list_timeuuid) " +
                "VALUES (3, [], {}, {}, [])");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list_timestamp = ['2023-01-01 00:00:00+0000', '2023-01-02 00:00:00+0000'] ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_set_date CONTAINS '2023-01-01' ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map_time['12:00:00'] = 'noon' ALLOW FILTERING").size());
        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list_timeuuid CONTAINS 50554d6e-29bb-11e5-b345-feff819cdc9f ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_list_timestamp)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_set_date)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map_time)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_list_timeuuid)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list_timestamp = ['2023-01-01 00:00:00+0000', '2023-01-02 00:00:00+0000']");
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_set_date CONTAINS '2023-01-01'");
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_map_time['12:00:00'] = 'noon'");
            assertEquals(1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_list_timeuuid CONTAINS 50554d6e-29bb-11e5-b345-feff819cdc9f");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenCollectionsWithOtherTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, " +
                    "frozen_list_uuid frozen<list<uuid>>, " +
                    "frozen_set_boolean frozen<set<boolean>>, " +
                    "frozen_map_inet frozen<map<inet, text>>)");

        execute("INSERT INTO %s (pk, frozen_list_uuid, frozen_set_boolean, frozen_map_inet) " +
                "VALUES (1, [550e8400-e29b-41d4-a716-446655440000, 550e8400-e29b-41d4-a716-446655440001], " +
                "{true, false}, {'127.0.0.1': 'localhost', '192.168.1.1': 'router'})");
        execute("INSERT INTO %s (pk, frozen_list_uuid, frozen_set_boolean, frozen_map_inet) " +
                "VALUES (2, [550e8400-e29b-41d4-a716-446655440000, 550e8400-e29b-41d4-a716-446655440001], " +
                "{true}, {'10.0.0.1': 'server'})");
        execute("INSERT INTO %s (pk, frozen_list_uuid, frozen_map_inet) " +
                "VALUES (3, [], {})");

        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_list_uuid = ? ALLOW FILTERING",
                                Arrays.asList(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                                              UUID.fromString("550e8400-e29b-41d4-a716-446655440001"))).size());
        assertEquals(2, execute("SELECT pk FROM %s WHERE frozen_set_boolean CONTAINS true ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map_inet['127.0.0.1'] = 'localhost' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(FULL(frozen_list_uuid)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(frozen_set_boolean)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map_inet)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list_uuid = ?",
                                        Arrays.asList(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                                                      UUID.fromString("550e8400-e29b-41d4-a716-446655440001")));
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_set_boolean CONTAINS true");
            assertEquals(2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE frozen_map_inet['127.0.0.1'] = 'localhost'");
            assertEquals(1, rows.all().size());
        });
    }

    @Test
    public void testNonFrozenCollectionsIndexes()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, list_col list<text>)");
        createIndex("CREATE INDEX ON %s(VALUES(list_col)) USING 'sai'");

        execute("INSERT INTO %s (id, list_col) VALUES (1, ['apple', 'banana'])");
        execute("INSERT INTO %s (id, list_col) VALUES (2, ['cherry', 'date'])");
        execute("INSERT INTO %s (id, list_col) VALUES (3, ['apple', 'banana', 'cherry'])");
        flush();

        ResultSet rows = executeNet("SELECT id FROM %s WHERE list_col CONTAINS 'apple'");
        assertEquals(2, rows.all().size());

        createTable("CREATE TABLE %s (id int PRIMARY KEY, set_col set<text>)");
        createIndex("CREATE INDEX ON %s(VALUES(set_col)) USING 'sai'");

        execute("INSERT INTO %s (id, set_col) VALUES (1, {'apple', 'banana'})");
        execute("INSERT INTO %s (id, set_col) VALUES (2, {'cherry', 'date'})");
        execute("INSERT INTO %s (id, set_col) VALUES (3, {'apple', 'banana', 'cherry'})");
        flush();

        rows = executeNet("SELECT id FROM %s WHERE set_col CONTAINS 'apple'");
        assertEquals(2, rows.all().size());

        createTable("CREATE TABLE %s (id int PRIMARY KEY, map_col map<text, text>)");
        createIndex("CREATE INDEX ON %s(VALUES(map_col)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(KEYS(map_col)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(ENTRIES(map_col)) USING 'sai'");

        execute("INSERT INTO %s (id, map_col) VALUES (1, {'k1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (id, map_col) VALUES (2, {'k3': 'v3', 'k4': 'v4'})");
        execute("INSERT INTO %s (id, map_col) VALUES (3, {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})");
        flush();

        rows = executeNet("SELECT id FROM %s WHERE map_col CONTAINS 'v1'");
        assertEquals(2, rows.all().size());

        rows = executeNet("SELECT id FROM %s WHERE map_col CONTAINS key 'k1'");
        assertEquals(2, rows.all().size());

        rows = executeNet("SELECT id FROM %s WHERE map_col['k1'] = 'v1'");
        assertEquals(2, rows.all().size());

        execute("UPDATE %s SET map_col = map_col + {'k1': 'v1_updated'} WHERE id = 1");
        flush();

        rows = executeNet("SELECT id FROM %s WHERE map_col['k1'] = 'v1_updated'");
        assertEquals(1, rows.all().size());

        rows = executeNet("SELECT id FROM %s WHERE map_col['k1'] = 'v1'");
        assertEquals(1, rows.all().size());

        execute("DELETE map_col['k1'] FROM %s WHERE id = 3");
        flush();

        rows = executeNet("SELECT id FROM %s WHERE map_col CONTAINS key 'k1'");
        assertEquals(1, rows.all().size());

        assertThatThrownBy(() -> executeNet("SELECT id FROM %s WHERE map_col = {'k1': 'v1'} ALLOW FILTERING"))
        .isInstanceOf(Exception.class);
    }

    @Test
    public void testSaiNonFrozenMap()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, frozen_map map<text, text>)");
        createIndex("CREATE INDEX ON %s(ENTRIES(frozen_map)) USING 'sai'");

        execute("INSERT INTO %s (id, frozen_map) VALUES (1, {'k1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (id, frozen_map) VALUES (2, {'k3': 'v3', 'k4': 'v4'})");
        execute("INSERT INTO %s (id, frozen_map) VALUES (3, {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})");
        execute("INSERT INTO %s (id, frozen_map) VALUES (4, {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})");
        flush();

        ResultSet rows = executeNet("SELECT id FROM %s WHERE frozen_map['k1']='v1';");
        assertEquals(3, rows.all().size());
    }

    @Test
    public void testFrozenListClusteringKeyWithValuesIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, [1, 2, 3], 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, [4, 5, 6], 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, [1, 7, 8], 300)");

        assertEquals("Should find 2 rows containing value 1", 2,
                     execute("SELECT pk, v FROM %s WHERE ck CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk, v FROM %s WHERE ck CONTAINS 1");
            assertEquals("Should find 2 rows containing value 1", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenSetClusteringKeyWithValuesIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<set<text>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {'a', 'b'}, 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {'c', 'd'}, 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, {'a', 'e'}, 300)");

        assertEquals("Should find 2 rows containing 'a'", 2,
                     execute("SELECT pk, v FROM %s WHERE ck CONTAINS 'a' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk, v FROM %s WHERE ck CONTAINS 'a'");
            assertEquals("Should find 2 rows containing 'a'", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapClusteringKeyWithKeysIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<map<text, int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {'x': 1, 'y': 2}, 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {'z': 3}, 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, {'x': 4}, 300)");

        assertEquals("Should find 2 rows with key 'x'", 2,
                     execute("SELECT pk, v FROM %s WHERE ck CONTAINS KEY 'x' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(KEYS(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk, v FROM %s WHERE ck CONTAINS KEY 'x'");
            assertEquals("Should find 2 rows with key 'x'", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapClusteringKeyWithValuesIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<map<text, int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {'x': 1, 'y': 2}, 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {'z': 3}, 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, {'x': 1}, 300)");

        assertEquals("Should find 2 rows with value 1", 2,
                     execute("SELECT pk, v FROM %s WHERE ck CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk, v FROM %s WHERE ck CONTAINS 1");
            assertEquals("Should find 2 rows with value 1", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapClusteringKeyWithEntriesIndexNotAllowed() throws Throwable
    {
        // ENTRIES index on frozen map clustering column should not be allowed because
        // map entry predicates (ck[key] = value) are not supported on clustering columns,
        // making the index unqueryable.
        createTable("CREATE TABLE %s (pk int, ck frozen<map<int, text>>, v int, PRIMARY KEY (pk, ck))");
        assertInvalidMessage("Cannot create ENTRIES index on frozen map clustering column",
                             "CREATE INDEX ON %s(ENTRIES(ck)) USING 'sai'");
    }

    @Test
    public void testFrozenListAsSecondClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 frozen<list<int>>, v int, PRIMARY KEY (pk, ck1, ck2))");

        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 1, [1, 2, 3], 100)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, [4, 5, 6], 200)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (2, 1, [1, 7, 8], 300)");

        assertEquals("Should find 2 rows containing value 1", 2,
                     execute("SELECT pk, ck1, v FROM %s WHERE ck2 CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck2)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk, ck1, v FROM %s WHERE ck2 CONTAINS 1");
            assertEquals("Should find 2 rows containing value 1", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyWithEmptyCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, [], 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, [1, 2], 200)");

        assertEquals("Should find 1 row containing value 1", 1,
                     execute("SELECT pk FROM %s WHERE ck CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck CONTAINS 1");
            assertEquals("Should find 1 row containing value 1", 1, rows.all().size());

            rows = executeNet("SELECT pk FROM %s");
            assertEquals("Should have 2 rows total", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyWithFullIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, [1, 2, 3], 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, [1, 2, 3], 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (3, [4, 5, 6], 300)");

        // Equality on clustering column works without index
        assertEquals("Should find 2 rows with list [1, 2, 3]", 2,
                     execute("SELECT pk FROM %s WHERE ck = ? ALLOW FILTERING", Arrays.asList(1, 2, 3)).size());

        createIndex("CREATE INDEX ON %s(FULL(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck = ?", Arrays.asList(1, 2, 3));
            assertEquals("Should find 2 rows with list [1, 2, 3]", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenSetClusteringKeyWithFullIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<set<int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {1, 2, 3}, 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, {1, 2, 3}, 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (3, {4, 5}, 300)");

        // Equality on clustering column works without index
        assertEquals("Should find 2 rows with set {1, 2, 3}", 2,
                     execute("SELECT pk FROM %s WHERE ck = ? ALLOW FILTERING", ImmutableSet.of(1, 2, 3)).size());

        createIndex("CREATE INDEX ON %s(FULL(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck = ?", ImmutableSet.of(1, 2, 3));
            assertEquals("Should find 2 rows with set {1, 2, 3}", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapClusteringKeyWithFullIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<map<text, int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, {'a': 1, 'b': 2}, 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, {'a': 1, 'b': 2}, 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (3, {'c': 3}, 300)");

        // Equality on clustering column works without index
        assertEquals("Should find 2 rows with map {'a': 1, 'b': 2}", 2,
                     execute("SELECT pk FROM %s WHERE ck = ? ALLOW FILTERING", ImmutableMap.of("a", 1, "b", 2)).size());

        createIndex("CREATE INDEX ON %s(FULL(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck = ?", ImmutableMap.of("a", 1, "b", 2));
            assertEquals("Should find 2 rows with map {'a': 1, 'b': 2}", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, [1, 2, 3], 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, [1, 4, 5], 200)");

        assertEquals("Should have 2 rows containing value 1", 2,
                     execute("SELECT pk FROM %s WHERE ck CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck CONTAINS 1");
        assertEquals("Should have 2 rows containing value 1", 2, rows.all().size());

        execute("DELETE FROM %s WHERE pk = 1 AND ck = [1, 2, 3]");

        beforeAndAfterFlush(() -> {
            ResultSet rows2 = executeNet("SELECT pk FROM %s WHERE ck CONTAINS 1");
            assertEquals("After delete, should have 1 row containing value 1", 1, rows2.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyMultiplePartitions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, [1, 2], 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, [1, 3], 200)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (3, [1, 4], 300)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (4, [5, 6], 400)");

        assertEquals("Should have 3 rows containing value 1 across different partitions", 3,
                     execute("SELECT pk FROM %s WHERE ck CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck CONTAINS 1");
            assertEquals("Should have 3 rows containing value 1 across different partitions", 3, rows.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyWithDuplicateValues() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<int>>, v int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, [1, 1, 1, 2], 100)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, [1, 3], 200)");

        assertEquals("Should find 2 rows containing value 1 (even with duplicates)", 2,
                     execute("SELECT pk FROM %s WHERE ck CONTAINS 1 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck CONTAINS 1");
            assertEquals("Should find 2 rows containing value 1 (even with duplicates)", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyWithLargeCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<int>>, v int, PRIMARY KEY (pk, ck))");

        List<Integer> largeList = new ArrayList<>();
        for (int i = 0; i < 100; i++)
        {
            largeList.add(i);
        }

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, 100)", largeList);
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, [99, 200], 200)");

        assertEquals("Should find 2 rows containing value 99", 2,
                     execute("SELECT pk FROM %s WHERE ck CONTAINS 99 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck CONTAINS 99");
            assertEquals("Should find 2 rows containing value 99", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyWithNumericTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, " +
                    "ck1 frozen<list<bigint>>, " +
                    "ck2 frozen<list<smallint>>, " +
                    "ck3 frozen<list<varint>>, " +
                    "v int, " +
                    "PRIMARY KEY (pk, ck1, ck2, ck3))");

        execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (1, [1, 2], [10, 20], [100, 200], 100)");
        execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (2, [1, 3], [10, 30], [100, 300], 200)");

        assertEquals("Should find 2 rows with bigint value 1", 2,
                     execute("SELECT pk FROM %s WHERE ck1 CONTAINS ? ALLOW FILTERING", 1L).size());
        assertEquals("Should find 2 rows with smallint value 10", 2,
                     execute("SELECT pk FROM %s WHERE ck2 CONTAINS ? ALLOW FILTERING", (short)10).size());
        assertEquals("Should find 2 rows with varint value 100", 2,
                     execute("SELECT pk FROM %s WHERE ck3 CONTAINS 100 ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(ck1)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(ck2)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(ck3)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck1 CONTAINS ?", 1L);
            assertEquals("Should find 2 rows with bigint value 1", 2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE ck2 CONTAINS ?", (short)10);
            assertEquals("Should find 2 rows with smallint value 10", 2, rows.all().size());

            rows = executeNet("SELECT pk FROM %s WHERE ck3 CONTAINS 100");
            assertEquals("Should find 2 rows with varint value 100", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListClusteringKeyWithUUIDType() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<uuid>>, v int, PRIMARY KEY (pk, ck))");

        UUID uuid1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID uuid2 = UUID.fromString("550e8400-e29b-41d4-a716-446655440001");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, 100)", Arrays.asList(uuid1, uuid2));
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, ?, 200)", Arrays.asList(uuid1));

        assertEquals("Should find 2 rows containing UUID", 2,
                     execute("SELECT pk FROM %s WHERE ck CONTAINS ? ALLOW FILTERING", uuid1).size());

        createIndex("CREATE INDEX ON %s(VALUES(ck)) USING 'sai'");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE ck CONTAINS ?", uuid1);
            assertEquals("Should find 2 rows containing UUID", 2, rows.all().size());
        });
    }

    @Test
    public void testFrozenListValuesIndexCaseInsensitive() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_list frozen<list<text>>)");

        execute("INSERT INTO %s (pk, frozen_list) VALUES (1, ['Apple', 'Banana'])");
        execute("INSERT INTO %s (pk, frozen_list) VALUES (2, ['banana', 'cherry'])");
        execute("INSERT INTO %s (pk, frozen_list) VALUES (3, ['cherry', 'date'])");

        // Without index, case-sensitive CONTAINS finds only exact matches
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_list CONTAINS 'Banana' ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_list CONTAINS 'banana' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(frozen_list)) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        beforeAndAfterFlush(() -> {
            // With case-insensitive index, 'banana' matches both 'Banana' and 'banana'
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_list CONTAINS 'banana'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenSetValuesIndexCaseInsensitive() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_set frozen<set<text>>)");

        execute("INSERT INTO %s (pk, frozen_set) VALUES (1, {'apple', 'Banana'})");
        execute("INSERT INTO %s (pk, frozen_set) VALUES (2, {'banana', 'cherry'})");
        execute("INSERT INTO %s (pk, frozen_set) VALUES (3, {'cherry', 'date'})");

        // Without index, case-sensitive CONTAINS finds only exact matches
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_set CONTAINS 'Banana' ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_set CONTAINS 'banana' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(frozen_set)) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        beforeAndAfterFlush(() -> {
            // With case-insensitive index, 'banana' matches both 'Banana' and 'banana'
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_set CONTAINS 'banana'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapValuesIndexCaseInsensitive() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<text, text>>)");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, {'k1': 'Value1', 'k2': 'v2'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, {'k3': 'value1', 'k4': 'v3'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, {'k5': 'v3', 'k6': 'v4'})");

        // Without index, case-sensitive CONTAINS finds only exact matches
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map CONTAINS 'Value1' ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map CONTAINS 'value1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(VALUES(frozen_map)) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        beforeAndAfterFlush(() -> {
            // With case-insensitive index, 'value1' matches both 'Value1' and 'value1'
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map CONTAINS 'value1'");
            assertEquals(2, rows.all().size());
        });
    }

    @Test
    public void testFrozenMapKeysIndexCaseInsensitive() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, frozen_map frozen<map<text, text>>)");

        execute("INSERT INTO %s (pk, frozen_map) VALUES (1, {'Key1': 'v1', 'k2': 'v2'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (2, {'key1': 'v3', 'k4': 'v4'})");
        execute("INSERT INTO %s (pk, frozen_map) VALUES (3, {'k5': 'v5', 'k6': 'v6'})");

        // Without index, case-sensitive CONTAINS KEY finds only exact matches
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map CONTAINS KEY 'Key1' ALLOW FILTERING").size());
        assertEquals(1, execute("SELECT pk FROM %s WHERE frozen_map CONTAINS KEY 'key1' ALLOW FILTERING").size());

        createIndex("CREATE INDEX ON %s(KEYS(frozen_map)) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        beforeAndAfterFlush(() -> {
            // With case-insensitive index, 'key1' matches both 'Key1' and 'key1'
            ResultSet rows = executeNet("SELECT pk FROM %s WHERE frozen_map CONTAINS KEY 'key1'");
            assertEquals(2, rows.all().size());
        });
    }

    private void createPopulatedMap(String createIndex)
    {
        createTable("CREATE TABLE %s (pk int primary key, value map<int, text>)");
        createIndex(createIndex);
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }});
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v3");
        }});
    }

    @SuppressWarnings("SameParameterValue")
    private void createPopulatedFrozenMap(String createIndex)
    {
        createTable("CREATE TABLE %s (pk int primary key, value frozen<map<int, text>>)");
        createIndex(createIndex);
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }});
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v3");
        }});
    }

    @SuppressWarnings("SameParameterValue")
    private void createPopulatedFrozenList(String createIndex)
    {
        createTable("CREATE TABLE %s (pk int primary key, value frozen<list<int>>)");
        createIndex(createIndex);
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 3, Arrays.asList(4, 5, 6));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 4, Arrays.asList(1, 2, 7));
    }

    @SuppressWarnings("SameParameterValue")
    private void assertUnsupportedIndexOperator(int expectedSize, String query, Object... values) throws Throwable
    {
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query, values);
        assertEquals(expectedSize, execute(query + " ALLOW FILTERING").size());
    }

    private static String createIndexDDL(String target)
    {
        return "CREATE INDEX ON %s(" + target + ") USING 'sai'";
    }
}
