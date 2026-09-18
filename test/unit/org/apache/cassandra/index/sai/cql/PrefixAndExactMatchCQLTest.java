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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.StorageService;

/**
 * End-to-end CQL test for SAI prefix (LIKE 'prefix%') and exact match (= 'value') queries.
 *
 * Inserts rows with shared-prefix string values, flushes the memtable to SSTable,
 * then validates that EQ queries return only exact matches while LIKE prefix queries
 * return all rows whose indexed column starts with the given prefix.
 *
 * This exercises the multi-type postings path (exactMatch + prefix) that is unit-tested
 * at the low level in {@link org.apache.cassandra.index.sai.disk.v1.postings.MultiTypePostingsTest}.
 */
public class PrefixAndExactMatchCQLTest extends SAITester
{
    @BeforeClass
    public static void setup()
    {
        StorageService.instance.unsafeSetInitialized();
    }

    /**
     * Test exact match (=) and prefix (LIKE 'x%') queries both before and after flush.
     *
     * Data:
     *   pk | name
     *   ---+-------------
     *    1 | apple
     *    2 | application
     *    3 | apply
     *    4 | ape
     *    5 | banana
     *    6 | band
     *    7 | bank
     *    8 | bat
     *    9 | cat
     *   10 | car
     *   11 | card
     *   12 | cart
     *   13 | dog
     *   14 | dove
     *   15 | door
     */
    @Test
    public void testPrefixAndExactMatchBeforeAndAfterFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, name text)");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX ON %s(name) USING 'sai'");

        // Insert 15 rows with shared-prefix string values
        execute("INSERT INTO %s (pk, name) VALUES (1,  'apple')");
        execute("INSERT INTO %s (pk, name) VALUES (2,  'application')");
        execute("INSERT INTO %s (pk, name) VALUES (3,  'apply')");
        execute("INSERT INTO %s (pk, name) VALUES (4,  'ape')");
        execute("INSERT INTO %s (pk, name) VALUES (5,  'banana')");
        execute("INSERT INTO %s (pk, name) VALUES (6,  'band')");
        execute("INSERT INTO %s (pk, name) VALUES (7,  'bank')");
        execute("INSERT INTO %s (pk, name) VALUES (8,  'bat')");
        execute("INSERT INTO %s (pk, name) VALUES (9,  'cat')");
        execute("INSERT INTO %s (pk, name) VALUES (10, 'car')");
        execute("INSERT INTO %s (pk, name) VALUES (11, 'card')");
        execute("INSERT INTO %s (pk, name) VALUES (12, 'cart')");
        execute("INSERT INTO %s (pk, name) VALUES (13, 'dog')");
        execute("INSERT INTO %s (pk, name) VALUES (14, 'dove')");
        execute("INSERT INTO %s (pk, name) VALUES (15, 'door')");

        // Run assertions before and after flush (memtable path vs SSTable path)
        beforeAndAfterFlush(() -> {
            // === Exact match (EQ) queries ===
            assertRows(execute("SELECT pk, name FROM %s WHERE name = 'apple'"),
                       row(1, "apple"));

            assertRows(execute("SELECT pk, name FROM %s WHERE name = 'bank'"),
                       row(7, "bank"));

            assertRows(execute("SELECT pk, name FROM %s WHERE name = 'dove'"),
                       row(14, "dove"));

            // EQ with no match
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name = 'nonexistent'"), 0);
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name = 'app'"), 0); // 'app' is a prefix but not a value

            // === Prefix (LIKE 'x%') queries ===
            // 'app%' -> apple, application, apply  (3 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'app%%'"), 3);

            // 'ap%' -> apple, application, apply, ape  (4 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ap%%'"), 4);

            // 'ban%' -> banana, band, bank  (3 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ban%%'"), 3);

            // 'ba%' -> banana, band, bank, bat  (4 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ba%%'"), 4);

            // 'ca%' -> cat, car, card, cart  (4 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ca%%'"), 4);

            // 'car%' -> car, card, cart  (3 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'car%%'"), 3);

            // 'do%' -> dog, dove, door  (3 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'do%%'"), 3);

            // Single-char prefix: 'a%' -> apple, application, apply, ape  (4 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'a%%'"), 4);

            // Single-char prefix: 'b%' -> banana, band, bank, bat  (4 rows)
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'b%%'"), 4);

            // Prefix with no match
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'zzz%%'"), 0);
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'x%%'"), 0);

            // Full value as prefix should match exactly one row
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'apple%%'"), 1);
            assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'application%%'"), 1);
        });
    }

    /**
     * Test with multiple flushes creating multiple SSTables, then verify queries still work.
     */
    @Test
    public void testPrefixAndExactMatchAcrossMultipleSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, name text)");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX ON %s(name) USING 'sai'");

        // First batch -> flush to SSTable 1
        execute("INSERT INTO %s (pk, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (pk, name) VALUES (2, 'application')");
        execute("INSERT INTO %s (pk, name) VALUES (3, 'apply')");
        execute("INSERT INTO %s (pk, name) VALUES (4, 'ape')");
        execute("INSERT INTO %s (pk, name) VALUES (5, 'banana')");
        flush();

        // Second batch -> flush to SSTable 2
        execute("INSERT INTO %s (pk, name) VALUES (6, 'band')");
        execute("INSERT INTO %s (pk, name) VALUES (7, 'bank')");
        execute("INSERT INTO %s (pk, name) VALUES (8, 'bat')");
        execute("INSERT INTO %s (pk, name) VALUES (9, 'cat')");
        execute("INSERT INTO %s (pk, name) VALUES (10, 'car')");
        flush();

        // Third batch -> stays in memtable (mixed SSTable + memtable)
        execute("INSERT INTO %s (pk, name) VALUES (11, 'card')");
        execute("INSERT INTO %s (pk, name) VALUES (12, 'cart')");
        execute("INSERT INTO %s (pk, name) VALUES (13, 'dog')");

        // EQ across SSTables
        assertRows(execute("SELECT pk, name FROM %s WHERE name = 'apple'"),
                   row(1, "apple"));
        assertRows(execute("SELECT pk, name FROM %s WHERE name = 'band'"),  // SSTable 2
                   row(6, "band"));
        assertRows(execute("SELECT pk, name FROM %s WHERE name = 'dog'"),   // memtable
                   row(13, "dog"));

        // Prefix across SSTables + memtable
        // 'app%' -> apple(SST1), application(SST1), apply(SST1)
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'app%%'"), 3);

        // 'ba%' -> banana(SST1), band(SST2), bank(SST2), bat(SST2)
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ba%%'"), 4);

        // 'ca%' -> cat(SST2), car(SST2), card(mem), cart(mem)
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ca%%'"), 4);

        // Now flush the remaining memtable and verify again
        flush();

        assertRows(execute("SELECT pk, name FROM %s WHERE name = 'dog'"),
                   row(13, "dog"));
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ca%%'"), 4);
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'app%%'"), 3);
    }

    /**
     * Test after compaction merges SSTables - prefix and exact match should still work.
     */
    @Test
    public void testPrefixAndExactMatchAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, name text)");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX ON %s(name) USING 'sai'");

        execute("INSERT INTO %s (pk, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (pk, name) VALUES (2, 'application')");
        execute("INSERT INTO %s (pk, name) VALUES (3, 'apply')");
        flush();

        execute("INSERT INTO %s (pk, name) VALUES (4, 'banana')");
        execute("INSERT INTO %s (pk, name) VALUES (5, 'band')");
        execute("INSERT INTO %s (pk, name) VALUES (6, 'bank')");
        flush();

        // Compact all SSTables into one
        compact();

        // EQ after compaction
        assertRows(execute("SELECT pk, name FROM %s WHERE name = 'apple'"),
                   row(1, "apple"));
        assertRows(execute("SELECT pk, name FROM %s WHERE name = 'band'"),
                   row(5, "band"));
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name = 'nonexistent'"), 0);

        // Prefix after compaction
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'app%%'"), 3);
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'ban%%'"), 3);
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'a%%'"), 3);
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'b%%'"), 3);
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'z%%'"), 0);
    }

    /**
     * Test that updating a value correctly reflects in both EQ and prefix queries after flush.
     */
    @Test
    public void testPrefixAndExactMatchWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, name text)");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX ON %s(name) USING 'sai'");

        execute("INSERT INTO %s (pk, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (pk, name) VALUES (2, 'application')");
        flush();

        // Update pk=1 from 'apple' to 'avocado'
        execute("INSERT INTO %s (pk, name) VALUES (1, 'avocado')");
        flush();

        // 'apple' should no longer match pk=1
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name = 'apple'"), 0);
        // 'avocado' should match pk=1
        assertRows(execute("SELECT pk, name FROM %s WHERE name = 'avocado'"),
                   row(1, "avocado"));

        // 'app%' should now only match pk=2 (application)
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'app%%'"), 1);
        // 'av%' should match pk=1 (avocado)
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'av%%'"), 1);
        // 'a%' should match pk=1 (avocado), pk=2 (application)
        assertRowCount(execute("SELECT pk, name FROM %s WHERE name LIKE 'a%%'"), 2);
    }
}
