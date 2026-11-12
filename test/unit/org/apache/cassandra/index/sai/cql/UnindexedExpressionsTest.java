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

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.Operation;

import static org.junit.Assert.assertTrue;

public class UnindexedExpressionsTest extends SAITester
{
    @Test
    public void inOnIndexedFixedLengthColumnIsPostFiltered() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 uuid, v2 int)");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        execute("INSERT INTO %s (k, v1, v2) VALUES (1, 00000000-0000-4000-8000-000000000001, 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (2, 00000000-0000-4000-8000-000000000002, 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (3, 00000000-0000-4000-8000-000000000003, 2)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v1 IN (00000000-0000-4000-8000-000000000001, 00000000-0000-4000-8000-000000000003) AND v2 = 1 ALLOW FILTERING"), row(1));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v1 IN (00000000-0000-4000-8000-000000000001, 00000000-0000-4000-8000-000000000002) AND v2 = 1 ALLOW FILTERING"), row(1), row(2));
            assertTrue(execute("SELECT k FROM %s WHERE v1 IN (00000000-0000-4000-8000-000000000003, 00000000-0000-4000-8000-000000000004) AND v2 = 1 ALLOW FILTERING").isEmpty());
        });
    }

    @Test
    public void inOnIndexedInetColumnIsPostFiltered() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 inet, v2 int)");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        execute("INSERT INTO %s (k, v1, v2) VALUES (1, '127.0.0.1', 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (2, '2001:db8::1', 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (3, '127.0.0.3', 2)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v1 IN ('127.0.0.1', '127.0.0.3') AND v2 = 1 ALLOW FILTERING"), row(1));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v1 IN ('127.0.0.1', '2001:db8::1') AND v2 = 1 ALLOW FILTERING"), row(1), row(2));
            assertTrue(execute("SELECT k FROM %s WHERE v1 IN ('127.0.0.3', '2001:db8::2') AND v2 = 1 ALLOW FILTERING").isEmpty());
        });
    }

    @Test
    public void inOperatorIsNotHandledByIndexTest() throws Throwable
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
            // Post-filtering CONTAINS/CONTAINS KEY against a frozen collection element-by-element (mixed with
            // another indexed predicate) isn't supported in 5.0.
            if (frozen)
            {
                assertInvalidMessage(String.format(Operation.FROZEN_COLLECTION_ELEMENT_FILTERING_NOT_SUPPORTED, "CONTAINS KEY", 'm'),
                                     "SELECT k FROM %s WHERE v = 1 AND m CONTAINS KEY 2 ALLOW FILTERING");
                assertInvalidMessage(String.format(Operation.FROZEN_COLLECTION_ELEMENT_FILTERING_NOT_SUPPORTED, "CONTAINS", 'm'),
                                     "SELECT k FROM %s WHERE v = 1 AND m CONTAINS 20 ALLOW FILTERING");
                assertInvalidMessage(String.format(Operation.FROZEN_COLLECTION_ELEMENT_FILTERING_NOT_SUPPORTED, "CONTAINS", 's'),
                                     "SELECT k FROM %s WHERE v = 1 AND s CONTAINS 2 ALLOW FILTERING");
                assertInvalidMessage(String.format(Operation.FROZEN_COLLECTION_ELEMENT_FILTERING_NOT_SUPPORTED, "CONTAINS", 'l'),
                                     "SELECT k FROM %s WHERE v = 1 AND l CONTAINS 2 ALLOW FILTERING");
            }
            else
            {
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS KEY 2 ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS 20 ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND s CONTAINS 2 ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND l CONTAINS 2 ALLOW FILTERING"), row(1));
            }

            // Whole-collection equality uses the FULL index target, which the restriction above doesn't apply to.
            if (frozen)
            {
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m = {2: 20} ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND s = {2} ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND l = [2] ALLOW FILTERING"), row(1));
            }

            // Map-entry predicates (m[k] = v) on frozen map columns aren't supported in 5.0.
            if (!frozen)
                assertRows(execute("SELECT k FROM %s WHERE v = 1 AND m[2] = 20 ALLOW FILTERING"), row(1));
        });
    }

    @Test
    public void unindexedFrozenSetContainsIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<inet>>, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, s, v) VALUES (0, {'10.0.0.1'}, 1)");
        execute("INSERT INTO %s (k, s, v) VALUES (1, {'10.0.0.2'}, 1)");

        // CONTAINS against frozen collection elements (mixed with another indexed predicate) isn't supported yet.
        beforeAndAfterFlush(() -> assertInvalidMessage(String.format(Operation.FROZEN_COLLECTION_ELEMENT_FILTERING_NOT_SUPPORTED, "CONTAINS", 's'),
                                                       "SELECT k FROM %s WHERE v = 1 AND s CONTAINS '10.0.0.2' ALLOW FILTERING"));
    }

    @Test
    public void unindexedFrozenMapRangeRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, m frozen<map<int, text>>, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {1: 'a'}, 7)");
        execute("INSERT INTO %s (pk, ck, m, v) VALUES (1, 2, {5: 'm'}, 7)");
        execute("INSERT INTO %s (pk, ck, m, v) VALUES (1, 3, {9: 'z'}, 7)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT ck FROM %s WHERE v = 7 AND m >= {5: 'm'} ALLOW FILTERING"), row(2), row(3));
            assertRows(execute("SELECT ck FROM %s WHERE v = 7 AND m < {5: 'm'} ALLOW FILTERING"), row(1));
            assertEmpty(execute("SELECT ck FROM %s WHERE v = 7 AND m > {9: 'z'} ALLOW FILTERING"));
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
    public void unsupportedOperatorsAreHandledTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, val1 int, val2 text)");
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(val2) USING 'sai'");

        execute("INSERT INTO %s (pk, val1, val2) VALUES (1, 1, '11')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (2, 2, '22')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (3, 3, '33')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (4, 4, '44')");

        // The LIKE operator is rejected because it needs to be handled by an index
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns",
                             "SELECT pk FROM %s WHERE val1 = 1 AND val2 like '1%%'");

        // The IS NOT operator is only valid on materialized views
        assertInvalidMessage("Unsupported restriction:", "SELECT pk FROM %s WHERE val1 = 1 AND val2 is not null");

        // The != operator is currently not supported at all
        assertInvalidMessage("Unsupported \"!=\" relation:", "SELECT pk FROM %s WHERE val1 = 1 AND val2 != '22'");

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
    public void unindexedMapColumnTest() throws Throwable
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