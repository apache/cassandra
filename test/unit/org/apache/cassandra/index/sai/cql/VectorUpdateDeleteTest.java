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
import org.apache.cassandra.index.sai.utils.IndexIdentifier;

import org.junit.Test;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph.MIN_PQ_ROWS;
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

    @Test
    public void testFlushWithDeletedVectors()
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (0, [1.0, 2.0])");
        execute("INSERT INTO %s (pk, v) VALUES (0, null)");

        flush();

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY v ann of [2.5, 3.5] LIMIT 1");
        assertThat(result).hasSize(0);
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

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
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

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1");
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
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 1");
        assertThat(result).hasSize(1);
    }

    @Test
    public void shadowedPrimaryKeyWithSharedVectorAndOtherPredicates()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // flush a sstable with one vector that is shared by two rows
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [1.0, 2.0, 3.0])");
        flush();

        // flush another sstable to shadow row 0
        execute("DELETE FROM %s where pk = 0");
        flush();

        // flush another sstable with one new vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [2.0, 3.0, 4.0])");
        flush();

        // the shadowed vector has the highest score, but we shouldn't see it
        UntypedResultSet result = execute("SELECT pk FROM %s WHERE str_val = 'A' ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 2");
        assertRowsIgnoringOrder(result, row(2), row(1));
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
    public void shadowedPrimaryKeyWithUpdatedPredicateMatchingIntValue() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, num int, val vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // Same PK, different num, different vectors
        execute("INSERT INTO %s (pk, num, val) VALUES (0, 1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, num, val) VALUES (0, 2, [2.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, num, val) VALUES (0, 3, [3.0, 2.0, 3.0])");
        // Need PKs that wrap 0 when put in PK order
        execute("INSERT INTO %s (pk, num, val) VALUES (1, 1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, num, val) VALUES (2, 1, [1.0, 2.0, 3.0])");

        // the shadowed vector has the highest score, but we shouldn't see it
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE num < 3 ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 10"),
                       row(1), row(2));
        });
    }

    @Test
    public void rangeRestrictedTestWithDuplicateVectorsAndADelete()
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", 2));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, val) VALUES (0, [1.0, 2.0])"); // -3485513579396041028
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])"); // -4069959284402364209
        execute("INSERT INTO %s (pk, val) VALUES (2, [1.0, 2.0])"); // -3248873570005575792
        execute("INSERT INTO %s (pk, val) VALUES (3, [1.0, 2.0])"); // 9010454139840013625

        flush();

        // Show the result set is as expected
        assertRows(execute("SELECT pk FROM %s WHERE token(pk) <= -3248873570005575792 AND " +
                           "token(pk) >= -3485513579396041028 ORDER BY val ann of [1,2] LIMIT 1000"), row(0), row(2));

        // Delete one of the rows
        execute("DELETE FROM %s WHERE pk = 0");

        flush();
        assertRows(execute("SELECT pk FROM %s WHERE token(pk) <= -3248873570005575792 AND " +
                           "token(pk) >= -3485513579396041028 ORDER BY val ann of [1,2] LIMIT 1000"), row(2));
    }

    @Test
    public void rangeRestrictedTestWithDuplicateVectorsAndAddNullVector() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", 2));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");


        execute("INSERT INTO %s (pk, val) VALUES (0, [1.0, 2.0])");
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])");
        execute("INSERT INTO %s (pk, val) VALUES (2, [1.0, 2.0])");
        // Add a str_val to make sure pk has a row id in the sstable
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'a', null)");
        // Add another row to test a different part of the code
        execute("INSERT INTO %s (pk, val) VALUES (4, [1.0, 2.0])");
        execute("DELETE FROM %s WHERE pk = 2");
        flush();

        // Delete one of the rows to trigger a shadowed primary key
        execute("DELETE FROM %s WHERE pk = 0");
        execute("INSERT INTO %s (pk, val) VALUES (2, [2.0, 2.0])");
        flush();

        // Delete more rows.
        execute("DELETE FROM %s WHERE pk = 2");
        execute("DELETE FROM %s WHERE pk = 3");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1,2] LIMIT 1000"),
                       row(1), row(4));
        });
    }

    // This test intentionally has extra rows with primary keys that are above and below the
    // deleted primary key so that we do not short circuit certain parts of the shadowed key logic.
    @Test
    public void shadowedPrimaryKeyInDifferentSSTableEachWithMultipleRows()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // flush a sstable with one vector
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'A', [1.0, 2.0, 3.0])");
        flush();

        // flush another sstable to shadow the vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])");
        execute("DELETE FROM %s where pk = 2");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'A', [1.0, 2.0, 3.0])");
        flush();

        // flush another sstable with one new vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'B', [2.0, 3.0, 4.0])");
        flush();

        // the shadow vector has the highest score
        UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 4");
        assertRows(result, row(1), row(3), row(0), row(4));
    }

    @Test
    public void shadowedPrimaryKeysRequireDeeperSearch() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // Choose a row count that will essentially force us to re-query the index that still has more rows to search.
        int baseRowCount = 1000;
        // Create 1000 rows so that each row has a slightly less similar score.
        for (int i = 0; i < baseRowCount - 10; i++)
        {
            try
            {
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", i, vector(1f, (float) i));
            }
            catch (Error e)
            {
                logger.error("Failed to insert row {}", i, e);
                throw new RuntimeException(e);
            }
        }

        for (int i = baseRowCount -10; i < baseRowCount; i++)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", i, vector(1f, (float) -i));

        flush();

        // Create 10 rows with the worst scores, but they won't be shadowed.
        for (int i = baseRowCount; i < baseRowCount + 10; i++)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", i, vector(-1f, (float) baseRowCount * -1));

        // Delete all but the last 10 rows
        for (int i = 0; i < baseRowCount - 10; i++)
            execute("DELETE FROM %s WHERE pk = ?", i);

        beforeAndAfterFlush(() -> {
            // ANN Only
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 1.0] LIMIT 3"),
                       row(baseRowCount - 10), row(baseRowCount - 9), row(baseRowCount - 8));
            // Hyrbid
            assertRows(execute("SELECT pk FROM %s WHERE str_val = 'A' ORDER BY val ann of [1.0, 1.0] LIMIT 3"),
                       row(baseRowCount - 10), row(baseRowCount - 9), row(baseRowCount - 8));
        });
    }

    @Test
    public void testUpdateVectorToWorseAndBetterPositions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, val) VALUES (0, [1.0, 2.0])");
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 3.0])");

        flush();
        execute("INSERT INTO %s (pk, val) VALUES (0, [1.0, 4.0])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1"), row(1));
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 2"), row(1), row(0));
        });

        // And now update pk 1 to show that we can get 0 too
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 5.0])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1"), row(0));
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 2"), row(0), row(1));
        });

        // And now update both PKs so that the stream of ranked rows is PKs: 0, 1, [1], 0, 1, [0], where the numbers
        // wrapped in brackets are the "real" scores of the vectors. This test makes sure that we correctly remove
        // PrimaryKeys from the updatedKeys map so that we don't accidentally duplicate PKs.
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 3.5])");
        execute("INSERT INTO %s (pk, val) VALUES (0, [1.0, 6.0])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1"), row(1));
            assertRows(execute("SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 2"), row(1), row(0));
        });
    }

    @Test
    public void updatedPrimaryKeysRequireResumeSearch() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // This test is fairly contrived, but it covers a bug we hit due to prematurely closed iterators.
        // The general design for this test is to shadow the close vectors on a memtable/sstable index forcing the
        // index to resume search. We do that by overwriting the first 50 vectors in the initial sstable.
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", i, vector(1f, (float) i));

        // Add more rows to make sure we filter then sort
        for (int i = 100; i < 1000; i++)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'C', ?)", i, vector(1f, (float) i));

        flush();

        // Overwrite the most similar 50 rows
        for (int i = 0; i < 50; i++)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'B', ?)", i, vector(1f, (float) i));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE str_val = 'A' ORDER BY val ann of [1.0, 1.0] LIMIT 1"),
                       row(50));
        });
    }

    @Test
    public void testBruteForceRangeQueryWithUpdatedVectors1536D() throws Throwable
    {
        testBruteForceRangeQueryWithUpdatedVectors(1536);
    }

    @Test
    public void testBruteForceRangeQueryWithUpdatedVectors2D() throws Throwable
    {
        testBruteForceRangeQueryWithUpdatedVectors(2);
    }

    private void testBruteForceRangeQueryWithUpdatedVectors(int vectorDimension) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, " + vectorDimension + ">, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Insert 100 vectors
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, randomVectorBoxed(vectorDimension));

        // Update those vectors so some ordinals are changed
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, randomVectorBoxed(vectorDimension));

        // Delete the first 50 PKs.
        for (int i = 0; i < 50; i++)
            execute("DELETE FROM %s WHERE pk = ?", i);

        // All of the above inserts and deletes are performed on the same index to verify internal index behavior
        // for both memtables and sstables.
        beforeAndAfterFlush(() -> {
            // Query for the first 10 vectors, we don't care which.
            // Use a range query to hit the right brute force code path
            UntypedResultSet results = execute("SELECT pk FROM %s WHERE token(pk) < 0 ORDER BY val ann of ? LIMIT 10",
                                  randomVectorBoxed(vectorDimension));
            assertThat(results).hasSize(10);
            // Make sure we don't get any of the deleted PKs
            assertThat(results).allSatisfy(row -> assertThat(row.getInt("pk")).isGreaterThanOrEqualTo(50));
        });
    }

    @Test
    public void testVectorIndexWithAllOrdinalsDeletedAndSomeViaRangeDeletion()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, a int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, a))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // Insert two rows with different vectors to get different ordinals
        execute("INSERT INTO %s (pk, a, str_val, val) VALUES (1, 1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, a, str_val, val) VALUES (2, 1, 'A', [1.0, 2.0, 4.0])");

        // Range delete the first row
        execute("DELETE FROM %s WHERE pk = 1");
        // Specifically delete the vector column second to hit a different code path.
        execute("DELETE FROM %s WHERE pk = 2 AND a = 1");

        // Insert another row without a vector
        execute("INSERT INTO %s (pk, a, str_val) VALUES (2, 1, 'A')");
        flush();

        assertRows(execute("SELECT PK FROM %s WHERE str_val = 'A' ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 1"));
    }

    @Test
    public void ensureCompressedVectorsCanFlush()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 4>, PRIMARY KEY(pk))");
        IndexIdentifier indexIdentifier = createIndexIdentifier(createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'"));

        // insert enough vectors for pq plus 1 because we need quantization and we're deleting a row
        for (int i = 0; i < MIN_PQ_ROWS + 1; i++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, vector(randomVector(4)));

        // Delete a single vector to trigger the regression
        execute("DELETE from %s WHERE pk = 0");

        flush();

        verifySSTableIndexes(indexIdentifier, 1);
    }

    // This test mimics having rf > 1.
    @Test
    public void testSameRowInMultipleSSTablesWithSameTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        // We don't want compaction preventing us from hitting the intended code path.
        disableCompaction();

        // This test is fairly contrived, but covers the case where the first row we attempt to materialize in the
        // ScoreOrderedResultRetriever is shadowed by a row in a different sstable. And then, when we go to pull in
        // the next row, we find that the PK is already pulled in, so we need to skip it.
        execute("INSERT INTO %s (pk, ck, val) VALUES (0, 0, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck, val) VALUES (0, 1, [1.0, 2.0, 3.0]) USING TIMESTAMP 1");
        flush();
        // Now, delete row pk=0, ck=0 so that we can test that the shadowed row is not returned and that we need
        // to get the next row from the score ordered iterator.
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 0");
        execute("INSERT INTO %s (pk, ck, val) VALUES (0, 1, [1.0, 2.0, 3.0]) USING TIMESTAMP 1");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT ck FROM %s ORDER BY val ANN OF [1.0, 2.0, 3.0] LIMIT 2"), row(1));
        });
    }

    @Test
    public void testMemtableInsertSearchUpdateSearchHandling()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, embedding vector<float, 5>)");
        createIndex("CREATE CUSTOM INDEX ON %s(embedding) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'similarity_function': 'dot_product'}");

        // Insert initial data
        execute("INSERT INTO %s (id, embedding) VALUES ('row1', [0.1, 0.1, 0.1, 0.1, 0.1])");
        execute("INSERT INTO %s (id, embedding) VALUES ('row2', [0.9, 0.9, 0.9, 0.9, 0.9])");

        // Query 100 times to try to guarantee all graph searchers are initialized
        for (int i = 0; i < 100; i++)
        {
            // Initial vector search
            UntypedResultSet initialSearch = execute("SELECT * FROM %s ORDER BY embedding ANN OF [0.8, 0.8, 0.8, 0.8, 0.8] LIMIT 1");
            assertThat(initialSearch).hasSize(1);
        }

        // Update one of the rows (this update wasn't observed due to state leaked between queries previously)
        execute("UPDATE %s SET embedding = [0.7, 0.7, 0.7, 0.7, 0.7] WHERE id = 'row1'");

        // Query 100 times to make sure it works as expected
        for (int j = 0; j < 100; j++)
        {
            // Get all data to verify we have 2 rows
            UntypedResultSet allData = execute("SELECT * FROM %s ORDER BY embedding ANN OF [0.8, 0.8, 0.8, 0.8, 0.8] LIMIT 1000");
            assertThat(allData).hasSize(2);
        }
    }

    @Test
    public void testUpdatedVectorStaticVectorColumnIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 2> static, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // This counts as an update because the indexed column is static and is therefore operated on at the partition
        // level.
        execute("INSERT INTO %s (pk, ck, val) VALUES (0, 1, [0,2])");
        execute("INSERT INTO %s (pk, ck, val) VALUES (0, 2, [1,0])");
        execute("INSERT INTO %s (pk, ck, val) VALUES (1, 3, [0,1])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT ck FROM %s ORDER BY val ANN OF [1,0] LIMIT 2"), row(1), row(2));
        });
    }
}
