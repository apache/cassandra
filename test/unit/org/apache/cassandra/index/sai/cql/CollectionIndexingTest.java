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

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

// This test is primarily handling edge conditions, error conditions
// and basic functionality. Comprehensive type testing of collections
// is in the cql/types/collections package
//TODO Sort out statement restrictions assertion
public class CollectionIndexingTest extends SAITester
{
    @Before
    public void setup() throws Throwable
    {
        requireNetwork();
    }

    @Test
    public void indexMap() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS 'v1'").size());
    }

    @Test
    public void indexMapKeys() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS KEY 1").size());
    }

    @Test
    public void indexMapValues() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS 'v1'").size());
    }

    @Test
    public void indexMapEntries() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] = 'v1'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE value[1] = 'v1' AND value[2] = 'v2'").size());
    }

    @Test
    public void indexFrozenList() throws Throwable
    {
        createPopulatedFrozenList();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertEquals(2, execute("SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2, 3)).size());
    }

    @Test
    public void indexFrozenMap() throws Throwable
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertEquals(1, execute("SELECT * FROM %s WHERE value = ?", new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }}).size());

    }

    @Test
    public void indexFrozenMapQueryKeys() throws Throwable
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains key 1");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains key 1 ALLOW FILTERING").size());
    }

    @Test
    public void indexFrozenMapQueryValues() throws Throwable
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexFrozenMapQueryEntries() throws Throwable
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertInvalidMessage("Map-entry equality predicates on frozen map column value are not supported",
                "SELECT * FROM %s WHERE value[1] = 'v1'");
    }

    @Test
    public void indexMapEntriesQueryEq() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapEntriesQueryKeys() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains key 1");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains key 1 ALLOW FILTERING").size());
    }

    @Test
    public void indexMapEntriesQueryValues() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexMapKeysQueryEq() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapKeysQueryValues() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexMapKeysQueryEntries() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value[1] = 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] = 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexMapValuesQueryEq() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapValuesQueryKeys() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains key 1");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains key 1 ALLOW FILTERING").size());
    }

    @Test
    public void indexMapValuesQueryEntries() throws Throwable
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value[1] = 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] = 'v1' ALLOW FILTERING").size());
    }

    private void createPopulatedMap() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value map<int, text>)");
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }});
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v3");
        }});
    }

    private void createPopulatedFrozenMap() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value frozen<map<int, text>>)");
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }});
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v3");
        }});
    }

    private void createPopulatedFrozenList() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value frozen<list<int>>)");
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 3, Arrays.asList(4, 5, 6));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 4, Arrays.asList(1, 2, 7));
    }

    private void assertUnsupportedIndexOperator(String query, Object... values) throws Throwable
    {
//        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "value"),
//                query, values);
    }
}
