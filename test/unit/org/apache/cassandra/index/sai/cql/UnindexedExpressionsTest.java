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

import com.datastax.driver.core.ResultSet;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnindexedExpressionsTest extends SAITester
{
    @Test
    public void inOperatorIsNotHandledByIndexTest()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        execute("INSERT INTO %s (k, v1, v2) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (2, 2, 2)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (3, 3, 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (4, 4, 2)");

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT * FROM %s WHERE v1 IN (1, 2)");

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT * FROM %s WHERE v1 IN (1, 2) AND v2 = 1");

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE v1 IN (1, 2) AND v2 = 1 ALLOW FILTERING"), row(1, 1, 1));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE v1 IN (1, 2) AND v2 = 2 ALLOW FILTERING"), row(2, 2, 2));

        assertTrue(execute("SELECT * FROM %s WHERE v1 IN (1, 3) AND v2 = 2 ALLOW FILTERING").isEmpty());
    }

    @Test
    public void unindexedFrozenCollectionRegularColumn() throws Throwable
    {
        assertUnindexedCollections(true, false);
    }

    @Test
    public void unindexedFrozenCollectionClusteringColumn() throws Throwable
    {
        // Map-entry predicates (m[k] = v) are rejected by CQL on frozen clustering columns.
        assertUnindexedCollections(true, true);
    }

    @Test
    public void unindexedNonFrozenCollectionRegularColumn() throws Throwable
    {
        // Non-frozen (multi-cell) collections can't be part of the primary key, so there's no clustering variant.
        assertUnindexedCollections(false, false);
    }

    private void assertUnindexedCollections(boolean frozen, boolean clusteringColumn) throws Throwable
    {
        String typeFormat = frozen ? "frozen<%s>" : "%s";
        String primaryKey = clusteringColumn ? "PRIMARY KEY (k, x, m, s, l)" : "PRIMARY KEY (k, x)";

        createTable(String.format("CREATE TABLE %%s (k int, x int, m %s, s %s, l %s, v int, %s)",
                                   String.format(typeFormat, "map<int, int>"), String.format(typeFormat, "set<int>"), String.format(typeFormat, "list<int>"), primaryKey));
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, x, m, s, l, v) VALUES (0, 0, {1: 10}, {1}, [1], 1)");
        execute("INSERT INTO %s (k, x, m, s, l, v) VALUES (1, 0, {2: 20}, {2}, [2], 1)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS KEY 2 ALLOW FILTERING"), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS 20 ALLOW FILTERING"), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v = 1 AND s CONTAINS 2 ALLOW FILTERING"), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v = 1 AND l CONTAINS 2 ALLOW FILTERING"), row(1));

            // Whole-collection equality is only valid CQL for frozen collections.
            if (frozen)
            {
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m = {2: 20} ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND s = {2} ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND l = [2] ALLOW FILTERING"), row(1));
            }

            // Map entry predicates (m[k] = v) are rejected by CQL on frozen clustering columns.
            if (!(frozen && clusteringColumn))
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m[2] = 20 ALLOW FILTERING"), row(1));
        });
    }

    @Test
    public void unindexedFrozenInetSetContains() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<inet>>, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, s, v) VALUES (0, {'10.0.0.1'}, 1)");
        execute("INSERT INTO %s (k, s, v) VALUES (1, {'10.0.0.2'}, 1)");

        beforeAndAfterFlush(() -> assertRows(execute("SELECT k FROM %s WHERE v = 1 AND s CONTAINS '10.0.0.2' ALLOW FILTERING"), row(1)));
    }

    @Test
    public void unindexedFrozenVectorSetContains() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 int, ck0 int, s1 int static, v1 frozen<set<vector<blob, 1>>>, PRIMARY KEY (pk0, ck0))");

        createIndex("CREATE INDEX ON %s(s1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(FULL(v1)) USING 'sai' WITH OPTIONS = {'shards': 'auto'}");

        execute("INSERT INTO %s (pk0, ck0, s1, v1) VALUES (1, 1, 100, {[0x35a528da0a70b82c], [0x8e653cf84369e2fc], [0xb8]}) USING TIMESTAMP 1");
        execute("UPDATE %s USING TIMESTAMP 2 SET s1=200, v1={[0x2400b6], [0x4db24a], [0xbc865621fb2eff13c5]} WHERE pk0 = 1 AND ck0 = 2");

        beforeAndAfterFlush(() -> {
            ResultSet rows = executeNetWithPaging("SELECT * FROM %s WHERE v1 CONTAINS [0x2400b6] AND s1 = 200 PER PARTITION LIMIT 5 ALLOW FILTERING", 1);
            assertEquals(1, rows.all().size());
        });
    }

    @Test
    public void unindexedInetColumnWithEmptyValuePostFiltersCorrectly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i inet, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, i, v) VALUES (0, blobAsInet(0x), 1)");
        execute("INSERT INTO %s (k, i, v) VALUES (1, '127.0.0.1', 1)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s WHERE v = 1 AND i >= '127.0.0.1' ALLOW FILTERING"), row(1));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND i >= blobAsInet(0x) ALLOW FILTERING"), row(0), row(1));
        });
    }

    @Test
    public void unsupportedOperatorsAreHandledTest()
    {
        createTable("CREATE TABLE %s (pk int primary key, val1 int, val2 text)");
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(val2) USING 'sai'");

        execute("INSERT INTO %s (pk, val1, val2) VALUES (1, 1, '11')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (2, 2, '22')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (3, 3, '33')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (4, 4, '44')");

        // The LIKE operator is rejected because it needs to be handled by an index
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT pk FROM %s WHERE val1 = 1 AND val2 like '1%%'");

        // The IS NOT operator is only valid on materialized views
        assertInvalidMessage("Unsupported restriction:", "SELECT pk FROM %s WHERE val1 = 1 AND val2 is not null");

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 != '22'");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 < '22'");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 <= '11'");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 >= '11'");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 > '00'");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 in ('11', '22')");

        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2 < '22' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2 <= '11' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2 >= '11' AND val2 <= '22' ALLOW FILTERING"), row(1), row(2));
        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2 > '00' AND val2 <= '11' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2 in ('11', '22') ALLOW FILTERING"), row(1), row(2));
    }

    @Test
    public void unindexedMapColumnTest()
    {
        createTable("CREATE TABLE %s (pk int primary key, val1 int, val2 map<int, text>)");
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");

        execute("INSERT INTO %s (pk, val1, val2) VALUES (1, 1, {1 : '1', 2 : '2', 3 : '3'})");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (2, 2, {2 : '2', 3 : '3', 4 : '4'})");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (3, 3, {3 : '3', 4 : '4', 5 : '5'})");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (4, 4, {4 : '4', 5 : '5', 6 : '6'})");

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 CONTAINS KEY 1");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT pk FROM %s WHERE val1 = 1 AND val2 CONTAINS '2'");

        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2 CONTAINS KEY 2 ALLOW FILTERING"), row(1), row(2));
        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2 CONTAINS '2' ALLOW FILTERING"), row(1), row(2));
        assertRows(execute("SELECT pk FROM %s WHERE val1 >= 1 AND val2[2] = '2' ALLOW FILTERING"), row(1), row(2));
    }
}