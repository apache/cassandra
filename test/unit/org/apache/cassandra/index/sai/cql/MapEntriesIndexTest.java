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

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

public class MapEntriesIndexTest extends SAITester
{
    @Test
    public void createEntriesIndexEqualityTest()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': -2})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 1, 'orange': 3})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 10, 'orange': -7})");

        // Test equality over both sstable and memtable
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] = 1"), row(1), row(3));
        // Test sstable read
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] = -2"), row(1));
        // Test memtable read
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] = -7"), row(4));
        // Test miss
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] = -3"));
    }

    @Test
    public void basicIntegerEntriesIndexRangeTest() throws Throwable
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // We intentionally use apple, banana, and orange to deal with multiple keys in the trie.
        // We then search against banana to show that we only get results for banana
        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 100, 'banana': 1, 'orange': 2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': -10, 'banana': 3, 'orange': 2})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 50, 'banana': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 10, 'banana': 1, 'orange': 3})");

        // Test range over both sstable and memtable, then over two sstables
        beforeAndAfterFlush(this::assertIntRangeQueries);
    }

    private void assertIntRangeQueries() {
        // GT cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] > " + Integer.MIN_VALUE),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] > -1"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] > 0"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] > 1"), row(2), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] > 2"), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] > 3"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] > " + Integer.MAX_VALUE));

        // GTE cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] >= " + Integer.MIN_VALUE),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] >= -1"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] >= 0"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] >= 1"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] >= 2"), row(2), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] >= 3"),
                   row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] >= " + Integer.MAX_VALUE));

        // LT cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] < " + Integer.MAX_VALUE),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] < 4"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] < 2"),
                   row(1), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] < 1"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] < 0"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] < " + Integer.MIN_VALUE));

        // LTE cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] <= " + Integer.MAX_VALUE),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] <= 4"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] <= 2"),
                   row(1), row(2), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] <= 1"),
                   row(1), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] <= 0"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['banana'] <= " + Integer.MIN_VALUE));
    }

    @Test
    public void queryMissingKeyTest()
    {
        createTable("CREATE TABLE %s (partition int primary key, coordinates map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(coordinates)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // No rows in table yet, so definitely no results
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] > 10"));

        // Insert row with keys surrounding x but not including it so that we test the empty iterator case
        execute("INSERT INTO %s (partition, coordinates) VALUES (1, {'w': 100, 'y': 2})");

        // Make sure we still get no results
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] > 10"));
        flush();
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] > 10"));
    }

    @Test
    public void entriesIndexRangeNestedPredicatesTest()
    {
        createTable("CREATE TABLE %s (partition int primary key, coordinates map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(coordinates)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, coordinates) VALUES (1, {'x': -1000000, 'y': 1000000})");
        execute("INSERT INTO %s (partition, coordinates) VALUES (4, {'x': -100, 'y': 2})");

        entriesIndexRangeNestedPredicatesTestAssertions();
        flush();
        entriesIndexRangeNestedPredicatesTestAssertions();
    }

    private void entriesIndexRangeNestedPredicatesTestAssertions()
    {
        // Intersections for x and y
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] <= 0 AND coordinates['y'] > 0"),
                   row(1), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] < -100 AND coordinates['y'] > 0"),
                   row(1));
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] < -100 AND coordinates['y'] > 1000000"));

        // Intersections for x (setting upper and lower bounds)
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] < -100 AND coordinates['x'] > -1000000"));
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] <= -100 AND coordinates['x'] >= -1000000"),
                   row(1), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] < -99 AND coordinates['x'] >= -101"),
                   row(4));

        // Unions
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] > -101 OR coordinates['y'] > 2"),
                   row(1), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] > 0 OR coordinates['y'] > 0"),
                   row(1), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] < -100 OR coordinates['y'] < 0"),
                   row(1));
        assertRows(execute("SELECT partition FROM %s WHERE coordinates['x'] < -1000000 OR coordinates['y'] > 1000000"));
    }

    @Test
    public void basicDateEntriesIndexRangeTest()
    {
        createTable("CREATE TABLE %s (partition int primary key, dates map<text, date>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(dates)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, dates) VALUES (1, {'a': '2000-02-03'})");
        execute("INSERT INTO %s (partition, dates) VALUES (4, {'a': '2001-02-03'})");
        flush();
        execute("INSERT INTO %s (partition, dates) VALUES (2, {'a': '1999-02-03'})");
        execute("INSERT INTO %s (partition, dates) VALUES (3, {'a': '2000-01-01'})");

        // Test range over both sstable and memtable
        assertDateRangeQueries();
        // Make two sstables
        flush();
        assertDateRangeQueries();
    }

    private void assertDateRangeQueries()
    {
        assertRows(execute("SELECT partition FROM %s WHERE dates['a'] > '2000-01-01'"),
                   row(1), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE dates['a'] >= '2000-01-01'"),
                   row(1), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE dates['a'] < '2000-02-03'"),
                   row(2), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE dates['a'] <= '2000-02-03'"),
                   row(1), row(2), row(3));
    }

    @Test
    public void basicTimestampEntriesIndexRangeTest()
    {
        createTable("CREATE TABLE %s (partition int primary key, timestamps map<text, timestamp>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(timestamps)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // 2011-02-03 04:05+0000 is 1296705900000
        execute("INSERT INTO %s (partition, timestamps) VALUES (1, {'a': '2011-02-03 04:05+0000'})");
        // 1299038900000 is on 2011-03-02
        execute("INSERT INTO %s (partition, timestamps) VALUES (4, {'a': '1299038900000'})");
        flush();
        execute("INSERT INTO %s (partition, timestamps) VALUES (2, {'a': '1000000000000'})");
        execute("INSERT INTO %s (partition, timestamps) VALUES (3, {'a': '1999999900000'})");

        // Test range over both sstable and memtable
        assertTimestampRangeQueries();
        // Make two sstables
        flush();
        assertTimestampRangeQueries();
    }

    private void assertTimestampRangeQueries()
    {
        assertRows(execute("SELECT partition FROM %s WHERE timestamps['a'] > '0'"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE timestamps['a'] > '1000000000000'"),
                   row(1), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE timestamps['a'] > '1296705900000'"),
                   row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE timestamps['a'] >= '1296705900000'"),
                   row(1), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE timestamps['a'] < '2011-02-03 04:05+0000'"),
                   row(2));
        assertRows(execute("SELECT partition FROM %s WHERE timestamps['a'] <= '2011-02-03 04:05+0000'"),
                   row(1), row(2));
    }

    // This test requires the ability to reverse lookup multiple rows from a single trie node
    // The indexing works by having a trie map that maps from a term to an ordinal in the posting list
    // and the posting list's ordinal maps to a list of primary keys.
    @Test
    public void testDifferentEntryWithSameValueInSameSSTable()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': 2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 1, 'orange': 3})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 3, 'orange': 2})");
        flush();

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 0"),
                   row(1), row(2), row(4), row(3));
    }

    @Test
    public void testUpdateInvalidatesRowInResultSet()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': 2})");

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 2"), row(1));

        // Push row from memtable to sstable and expect the same result
        flush();

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 2"), row(1));

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 2, 'orange': 1})");

        // Expect no rows then make sure we can still get the row
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 2"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 3"), row(1));

        // Push row from memtable to sstable and expect the same result
        flush();

        // Expect no rows then make sure we can still get the row
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 2"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 3"), row(1));

        // Now remove the key from the result
        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'orange': 1})");

        // Don't get anything
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 2"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 3"));

        // Push row from memtable to sstable and expect the same result
        flush();

        // Don't get anything
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 2"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 3"));
    }

    @Test
    public void queryLargeEntriesWithZeroes()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 101, 'orange': 2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 302, 'orange': 2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': -1000000, 'orange': 2})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 1000000, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (5, {'apple': -1000001, 'orange': 3})");

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 2"),
                   row(1), row(2), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 0"), row(5), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= 0"), row(5), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < -100"), row(5), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= -100"), row(5), row(3));
    }

    @Test
    public void queryTextEntriesWithZeroes()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 'ab0', 'orange': '2'})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 'a', 'orange': '2'})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 'abv', 'orange': '1'})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 'z', 'orange': '3'})");

        assertRowsIgnoringOrder(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 'a'"),
                                row(1), row(2), row(3));
    }

    @Test
    public void testUpdatesAndDeletes()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': -2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 1, 'orange': 3})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 10, 'orange': -7})");
        flush();

        // Set a baseline to make sure data is as expected
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 1"),
                                row(2), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] < 1"),
                   row(1), row(4));

        // Delete and update some rows
        execute("DELETE FROM %s WHERE partition = 2");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 1, 'orange': 3})");

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 1"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] < 1"), row(1));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] >= 3"), row(4), row(3));

        flush();

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 1"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] < 1"), row(1));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] >= 3"), row(4), row(3));
    }

    // NOTE: this works without touching the SAI code. This is worth testing to make sure we don't need to reject
    // these queries.
    @Test
    public void testLWTConditionalDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': -2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 10000, 'orange': 1})");
        flush();

        // Attempt to delete rows, but the conditional is not satisfied
        execute("DELETE FROM %s WHERE partition = 2 IF item_cost['apple'] > 2");

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 0"), row(1), row(2), row(3));

        // Actually delete row 2 this time
        execute("DELETE FROM %s WHERE partition = 2 IF item_cost['apple'] > 1");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 0"), row(1), row(3));
            assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 1"), row(3));
            // Show that it also works for the other map entry
            assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] > 0"), row(3));
        });
    }

    // NOTE: this works without touching the SAI code. This is worth testing to make sure we don't need to reject
    // these queries.
    @Test
    public void testLWTConditionalUpdate()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': -2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 10000, 'orange': 1})");
        flush();

        var row3 = execute("SELECT item_cost FROM %s WHERE partition = 3");
        assertEquals("{apple=10000, orange=1}", row3.one().getMap("item_cost", UTF8Type.instance, Int32Type.instance).toString());

        // Attempt to update rows, but only one of the updates is successful in updating the value of apple
        execute("UPDATE %s SET item_cost['apple'] = 3 WHERE partition = 2 IF item_cost['apple'] > 100");
        execute("UPDATE %s SET item_cost['apple'] = 3 WHERE partition = 3 IF item_cost['apple'] > 100");

        // observe the change
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] = 3"), row(3));
        row3 = execute("SELECT item_cost FROM %s WHERE partition = 3");
        assertEquals("{apple=3, orange=1}", row3.one().getMap("item_cost", UTF8Type.instance, Int32Type.instance).toString());
    }

    @Test
    public void testRangeQueriesCombinedWithQueryAgainstOtherIndexes()
    {
        createTable("CREATE TABLE %s (partition int primary key, store_name text, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(store_name) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // We intentionally use apple, banana, and orange to deal with multiple keys in the trie.
        // We then search against banana to show that we only get results for banana
        execute("INSERT INTO %s (partition, store_name, item_cost) VALUES (0, 'Partial Foods', {'apple': 5, 'orange': 7})");
        flush();
        execute("INSERT INTO %s (partition, store_name, item_cost) VALUES (1, 'Stale Market', {'apple': 6, 'orange': 4})");

        // Test basic ranges
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 6"), row(0));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] < 7"), row(1));

        // Combine ranges with query on other index
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= 6 AND store_name = 'Partial Foods'"), row(0));
    }

    @Test
    public void testPreparedQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // We intentionally use apple, banana, and orange to deal with multiple keys in the trie.
        // We then search against banana to show that we only get results for banana
        execute("INSERT INTO %s (partition, item_cost) VALUES (0, {'apple': 5, 'orange': 7})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 6, 'orange': 4})");

        String query = "SELECT partition FROM %s WHERE item_cost[?] < ?";
        prepare(query);
        assertRows(execute(query, "apple", 6), row(0));
    }

    @Test
    public void testRangeQueriesOnClusteringColumn() throws Throwable
    {
        // If we ever support using non-frozen collections as clustering columns, we need to determine
        // if range queries should work when the column is a clustering column.
        assertInvalidThrow(InvalidRequestException.class,
                           String.format(
                           "CREATE TABLE %s.%s (partition int, item_cost map<text, int>, PRIMARY KEY (partition, item_cost))",
                           KEYSPACE,
                           createTableName()));
    }
}
