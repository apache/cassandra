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

import org.apache.cassandra.cql3.UntypedResultSet;

import org.junit.Test;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE;
import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.assertj.core.api.Assertions.assertThat;

public class VectorUpdateDeleteTest extends VectorTester
{

    // partition delete won't trigger UpdateTransaction#onUpdated
    @Test
    public void partitionDeleteVectorInMemoryTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        execute("UPDATE %s SET val = null WHERE pk = 0");

        result = execute("SELECT * FROM %s ORDER BY val ann of [1.1, 2.1, 3.1] LIMIT 1"); // closer to row 0
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1"); // closer to row 1
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1");  // closer to row 1
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    // row delete will trigger UpdateTransaction#onUpdated
    @Test
    public void rowDeleteVectorInMemoryAndFlushTest()
    {
        createTable("CREATE TABLE %s (pk int, ck int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, str_val, val) VALUES (0, 0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck, str_val, val) VALUES (1, 1, 'B', [2.0, 3.0, 4.0])");
        execute("DELETE from %s WHERE pk = 1 and ck = 1");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    // range delete won't trigger UpdateTransaction#onUpdated
    @Test
    public void rangeDeleteVectorInMemoryAndFlushTest()
    {
        createTable("CREATE TABLE %s (pk int, ck int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (0, 0, 0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (1, 1, 1, 'B', [2.0, 3.0, 4.0])");
        execute("DELETE from %s WHERE pk = 1 and ck = 1");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    @Test
    public void updateVectorInMemoryAndFlushTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("UPDATE %s SET val = null WHERE pk = 1");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    @Test
    public void deleteVectorPostFlushTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);
        flush();

        execute("UPDATE %s SET val = null WHERE pk = 0");
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).isEmpty();
        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).isEmpty();
    }

    @Test
    public void deletedInOtherSSTablesTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");
        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void deletedInOtherSSTablesMultiIndexTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [3.0, 4.0, 5.0])");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");
        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void rangeDeletedInOtherSSTablesTest()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 3, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 4, 'D', [3.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "ck1", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0 and ck1 = 0");

        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "ck1", 1);


        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void partitionDeletedInOtherSSTablesTest()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 3, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 4, 'D', [3.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");

        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);


        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void upsertTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
        flush();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
    }

    @Test
    public void updateTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // overwrite row A a bunch of times; also write row B with the same vector as a deleted A value
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [5.0, 6.0, 7.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        // check that queries near A and B get the right row
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        makeRowStrings(result).forEach(logger::info);
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        // flush, and re-check same queries
        flush();
        result = execute("SELECT * FROM %s ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        // overwite A more in the new memtable
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [6.0, 7.0, 8.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [7.0, 8.0, 9.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [8.0, 9.0, 10.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [9.0, 10.0, 11.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [10.0, 11.0, 12.0])");

        // query near A and B again
        result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        // flush, and re-check same queries
        flush();
        result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);
    }

    @Test
    public void updateOtherColumnsTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("UPDATE %s SET str_val='C' WHERE pk=0");

        var result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void updateManySSTablesTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [2.0, 3.0, 4.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [3.0, 4.0, 5.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [4.0, 5.0, 6.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [5.0, 6.0, 7.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [6.0, 7.0, 8.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [7.0, 8.0, 9.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [8.0, 9.0, 10.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [9.0, 10.0, 11.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [10.0, 11.0, 12.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        flush();

        var result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);
    }

    @Test
    public void shadowedPrimaryKeyInDifferentSSTable()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // flush a sstable with one vector
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        flush();

        // flush another sstable to shadow the vector row
        execute("DELETE FROM %s where pk = 0");
        flush();

        // flush another sstable with one new vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        flush();

        // the shadow vector has the highest score
        var result = execute("SELECT * FROM %s ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 1");
        assertThat(result).hasSize(1);
    }

    @Test
    public void testVectorRowWhereUpdateMakesRowMatchNonOrderingPredicates()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, val text, vec vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Split the row across 1 sstable and the memtable.
        execute("INSERT INTO %s (pk, vec) VALUES (1, [1,1])");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (1, 'match me')");

        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));
        // Push memtable to sstable. we should get same result
        flush();
        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));

        // Run the test again but instead inserting a full row and then overwrite val to match the predicate.
        // This covers a different case because when there is no data for a column, it doesn't get an index file.

        execute("INSERT INTO %s (pk, val, vec) VALUES (1, 'no match', [1,1])");
        flush();
        execute("INSERT INTO %s (pk, val) VALUES (1, 'match me')");

        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));
        // Push memtable to sstable. we should get same result
        flush();
        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1));
    }

    // We need to make sure that we search each vector index for all relevant primary keys. In this test, row 1
    // matches the query predicate, but has a low score. It is later updated to have a vector that is closer to the
    // searched vector. As such, we need to make sure that we get all possible primary keys that match the predicates
    // and use those to search for topk vectors.
    @Test
    public void testUpdateVectorWithSplitRow()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, val text, vec vector<float, 2>, PRIMARY KEY(pk))");
        // Use euclidean distance to more easily verify correctness of caching
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // We will search for [11,11]
        execute("INSERT INTO %s (pk, val, vec) VALUES (1, 'match me', [1,1])");
        execute("INSERT INTO %s (pk, val, vec) VALUES (2, 'match me', [10,10])");
        flush();
        execute("INSERT INTO %s (pk, val, vec) VALUES (3, 'match me', [12,12])");
        // Overwrite pk 1 with a vector that is closest to the search vector
        execute("INSERT INTO %s (pk, vec) VALUES (1, [11,11])");

        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [11,11] LIMIT 1"), row(1));
        // Push memtable to sstable. we should get same result
        flush();
        assertRows(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [11,11] LIMIT 1"), row(1));
    }

    @Test
    public void testUpdateNonVectorColumnWhereNoSingleSSTableRowMatchesAllPredicates()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, val1 text, val2 text, vec vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, val1, vec) VALUES (1, 'match me', [1,1])");
        execute("INSERT INTO %s (pk, val2, vec) VALUES (2, 'match me', [1,2])");
        flush();
        execute("INSERT INTO %s (pk, val2, vec) VALUES (1, 'match me', [1,1])");
        execute("INSERT INTO %s (pk, val1, vec) VALUES (2, 'match me', [1,2])");


        assertRows(execute("SELECT pk FROM %s WHERE val1 = 'match me' AND val2 = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 2"), row(1), row(2));
        // Push memtable to sstable. we should get same result
        flush();
        assertRows(execute("SELECT pk FROM %s WHERE val1 = 'match me' AND val2 = 'match me' ORDER BY vec ANN OF [11,11] LIMIT 2"), row(1), row(2));
    }

    @Test
    public void ensureVariableChunkSizeDoesNotLeadToIncorrectResults() throws Exception
    {
        // When adding the chunk size feature, there were issues related to leaked files.
        // This setting only matters for hybrid queries
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, vec vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");

        // Create many sstables to ensure chunk size matters
        // Start at 1 to prevent indexing zero vector.
        // Index every vector with A to match everything and because this test only makes sense for hybrid queries
        for (int i = 1; i <= 100; i++)
        {
            execute("INSERT INTO %s (pk, str_val, vec) VALUES (?, ?, ?)", i, "A", vector((float) i, (float) i));
            if (i % 10 == 0)
                flush();
            // Add some deletes in the next segment
            if (i % 3 == 0)
                execute("DELETE FROM %s WHERE pk = ?", i);
        }

        try
        {
            // We use a chunk size that is as low as possible (1) and goes up to the whole dataset (100).
            // We also query for different LIMITs
            for (int i = 1; i <= 100; i++)
            {
                SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE.setInt(i);
                var results = execute("SELECT pk FROM %s WHERE str_val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 1");
                assertRows(results, row(1));
                results = execute("SELECT pk FROM %s WHERE str_val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 3");
                // Note that we delete row 3
                assertRows(results, row(1), row(2), row(4));
                results = execute("SELECT pk FROM %s WHERE str_val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 10");
                // Note that we delete row 3, 6, 9, 12
                assertRows(results, row(1), row(2), row(4), row(5),
                           row(7), row(8), row(10), row(11), row(13), row(14));
            }
        }
        finally
        {
            // Revert to prevent interference with other tests. Note that a decreased chunk size can impact
            // whether we compute the topk with brute force because it determines how many vectors get sent to the
            // vector index.
            SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE.setInt(100000);
        }
    }
}
