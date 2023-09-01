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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class LuceneUpdateDeleteTest extends SAITester
{
    @Test
    public void updateAndDeleteWithAnalyzerRestrictionQueryShouldFail() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES (0, 'a sad doG.')");

        // Prove we can get the row back
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());

        // DELETE fails
        assertThatThrownBy(() -> execute("DELETE FROM %s WHERE val : 'dog'"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid query. DELETE does not support use of secondary indices, but val : 'dog' restriction requires a secondary index.");

        // UPDATE fails
        assertThatThrownBy(() -> execute("UPDATE %s SET val = 'something new' WHERE val : 'dog'"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid query. UPDATE does not support use of secondary indices, but val : 'dog' restriction requires a secondary index.");

        // UPDATE with LWT fails (different error message because it fails at a different point)
        assertThatThrownBy(() -> execute("UPDATE %s SET val = 'something new' WHERE id = 0 IF val : 'dog'"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("LWT Conditions do not support the : operator");
    }

    // No flushes
    @Test
    public void removeUpdateAndDeleteTextInMemoryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        waitForIndexQueryable();

        // The analyzed text column will result in overlapping and non-overlapping tokens in the in memory trie map.
        // Note that capitalization is covered as well as tokenization.
        execute("INSERT INTO %s (id, val) VALUES (0, 'a sad doG.')");
        execute("INSERT INTO %s (id, val) VALUES (1, 'A Happy DOG.')");

        // Prove initial assumptions about data structures are correct.
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = null WHERE id = 0");

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = 'the dog' WHERE id = 0");

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("DELETE from %s WHERE id = 1");

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());
    }

    // Flush after every insert/update/delete
    @Test
    public void removeUpdateAndDeleteTextOnDiskTest() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        waitForIndexQueryable();

        // The analyzed text column will result in overlapping and non-overlapping tokens in the in memory trie map.
        // Note that capitalization is covered as well as tokenization.
        execute("INSERT INTO %s (id, val) VALUES (0, 'a sad doG.')");
        execute("INSERT INTO %s (id, val) VALUES (1, 'A Happy DOG.')");

        flush();

        // Prove initial assumptions about data structures are correct.
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = null WHERE id = 0");
        flush();

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = 'the dog' WHERE id = 0");
        flush();

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("DELETE from %s WHERE id = 1");
        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("INSERT INTO %s (id, val) VALUES (1, 'A Happy DOG.')");
        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());
    }

    // Insert entries, flush them, then perform updates without flushing.
    @Test
    public void removeUpdateAndDeleteTextMixInMemoryOnDiskTest() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        waitForIndexQueryable();

        // The analyzed text column will result in overlapping and non-overlapping tokens in the in memory trie map.
        // Note that capitalization is covered as well as tokenization.
        execute("INSERT INTO %s (id, val) VALUES (0, 'a sad doG.')");
        execute("INSERT INTO %s (id, val) VALUES (1, 'A Happy DOG.')");

        flush();

        // Prove initial assumptions about data structures are correct.
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = null WHERE id = 0");

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = 'the dog' WHERE id = 0");

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("DELETE from %s WHERE id = 1");

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("INSERT INTO %s (id, val) VALUES (1, 'A Happy DOG.')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());
    }

    // row delete will trigger UpdateTransaction#onUpdated
    @Test
    public void rowDeleteRowInMemoryAndFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, str_val text, val text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, ck, str_val, val) VALUES (0, 0, 'A', 'dog 0')");
        execute("INSERT INTO %s (pk, ck, str_val, val) VALUES (1, 1, 'B', 'dog 1')");
        execute("DELETE from %s WHERE pk = 1 and ck = 1");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'dog'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s WHERE val : 'dog'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    // range delete won't trigger UpdateTransaction#onUpdated
    @Test
    public void rangeDeleteRowInMemoryAndFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, ck2 int, str_val text, val text, PRIMARY KEY(pk, ck, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (0, 0, 0, 'A', 'first insert')");
        execute("INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (1, 1, 1, 'B', 'second insert')");
        execute("DELETE from %s WHERE pk = 1 and ck = 1");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    @Test
    public void updateRowInMemoryAndFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', 'first insert')");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', 'second insert')");
        execute("UPDATE %s SET val = null WHERE pk = 1");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    @Test
    public void deleteRowPostFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', 'first insert')");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', 'second insert')");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(2);
        flush();

        execute("UPDATE %s SET val = null WHERE pk = 0");
        result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).isEmpty();
        flush();

        result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).isEmpty();
    }

    @Test
    public void deletedInOtherSSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', 'first insert')");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', 'second insert')");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', 'third insert')");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'first'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");
        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void deletedInOtherSSTablesMultiIndexTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', 'first insert')");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', 'second insert')");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', 'third insert')");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = 'A' AND val : 'first'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");
        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s WHERE str_val = 'A' AND val : 'insert'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void rangeDeletedInOtherSSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, str_val text, val text, PRIMARY KEY(pk, ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', 'first insert')");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', 'second insert')");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 3, 'C', 'third insert')");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 4, 'D', 'fourth insert')");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(4);
        flush();

        execute("DELETE from %s WHERE pk = 0 and ck1 = 0");

        result = execute("SELECT * FROM %s WHERE val : 'insert'");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "ck2", 3);
        assertContainsInt(result, "ck2", 4);
    }

    @Test
    public void partitionDeletedInOtherSSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, str_val text, val text, PRIMARY KEY(pk, ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', 'some text')");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', 'updated text')");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 3, 'C', 'another text')");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 4, 'D', 'more text')");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'updated'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");

        result = execute("SELECT * FROM %s WHERE val : 'another'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);


        result = execute("SELECT * FROM %s WHERE val : 'text'");
        assertThat(result).hasSize(2);
    }

    @Test
    public void upsertTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, not_analyzed text, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (1, 'B', 'different tokenized text')");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val : 'tokenized'");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
        flush();

        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (0, 'A', 'this will be tokenized')");
        result = execute("SELECT * FROM %s WHERE val : 'tokenized'");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
        flush();

        result = execute("SELECT * FROM %s WHERE val : 'tokenized'");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
    }

    @Test
    public void updateOtherColumnsTest() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text, not_analyzed text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val, not_analyzed) VALUES (0, 'a sad doG.', 'more text')");
        execute("INSERT INTO %s (id, val, not_analyzed) VALUES (1, 'A Happy DOG.', 'different text')");
        execute("UPDATE %s SET not_analyzed='A' WHERE id=0");

        var result = execute("SELECT * FROM %s WHERE val : 'dog'");
        assertThat(result).hasSize(2);
    }

    @Test
    public void updateManySSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, val) VALUES (0, 'this is')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'a test')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'of the emergency')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'broadcast system')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'this is only')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'a test')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'if this were')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'a real emergency')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'you would be instructed')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'where to tune in your area')");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, 'for news and official information')");
        flush();

        var result = execute("SELECT * FROM %s WHERE val : 'news'");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s WHERE val : 'this'");
        assertThat(result).hasSize(0);
    }

    @Test
    public void shadowedPrimaryKeyInDifferentSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val text, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();
        disableCompaction(KEYSPACE);

        // flush a sstable with one vector
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', 'an indexed phrase')");
        flush();

        // flush another sstable to shadow the vector row
        execute("DELETE FROM %s where pk = 0");
        flush();

        // flush another sstable with one new vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', 'something different')");
        flush();

        // the shadow vector has the highest score
        var result = execute("SELECT * FROM %s WHERE val : 'something'");
        assertThat(result).hasSize(1);
    }

}