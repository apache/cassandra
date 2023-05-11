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
package org.apache.cassandra.cql3.validation.entities;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecondaryIndexOnMapEntriesTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @Test
    public void testShouldNotCreateIndexOnFrozenMaps() throws Throwable
    {
        createTable("CREATE TABLE %s (k TEXT PRIMARY KEY, v FROZEN<MAP<TEXT, TEXT>>)");
        assertIndexInvalidForColumn("v");
    }

    @Test
    public void testShouldNotCreateIndexOnNonMapTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (k TEXT PRIMARY KEY, i INT, t TEXT, b BLOB, s SET<TEXT>, l LIST<TEXT>, tu TUPLE<TEXT>)");
        assertIndexInvalidForColumn("i");
        assertIndexInvalidForColumn("t");
        assertIndexInvalidForColumn("b");
        assertIndexInvalidForColumn("s");
        assertIndexInvalidForColumn("l");
        assertIndexInvalidForColumn("tu");
    }

    @Test
    public void testShouldValidateMapKeyAndValueTypes() throws Throwable
    {
        createSimpleTableAndIndex();

        String query = "SELECT * FROM %s WHERE v[?] = ?";
        Object validKey = "valid key";
        Object invalidKey = 31415;
        Object validValue = 31415;
        Object invalidValue = "invalid value";
        assertInvalid(query, invalidKey, invalidValue);
        assertInvalid(query, invalidKey, validValue);
        assertInvalid(query, validKey, invalidValue);
        assertReturnsNoRows(query, validKey, validValue);
    }

    @Test
    public void testShouldFindRowsMatchingSingleEqualityRestriction() throws Throwable
    {
        createSimpleTableAndIndex();
        Object[] foo = insertIntoSimpleTable("foo", map("a", 1,
                                                        "c", 3));
        Object[] bar = insertIntoSimpleTable("bar", map("a", 1,
                                                        "b", 2));
        Object[] baz = insertIntoSimpleTable("baz", map("b", 2,
                                                        "c", 5,
                                                        "d", 4));
        Object[] qux = insertIntoSimpleTable("qux", map("b", 2,
                                                        "d", 4));

        assertRowsForConditions(entry("a", 1), bar, foo);
        assertRowsForConditions(entry("b", 2), bar, baz, qux);
        assertRowsForConditions(entry("c", 3), foo);
        assertRowsForConditions(entry("c", 5), baz);
        assertRowsForConditions(entry("d", 4), baz, qux);
    }

    @Test
    public void testRequireFilteringDirectiveIfMultipleRestrictionsSpecified() throws Throwable
    {
        createSimpleTableAndIndex();
        String baseQuery = "SELECT * FROM %s WHERE v['foo'] = 31415 AND v['baz'] = 31416";
        assertInvalid(baseQuery);
        assertReturnsNoRows(baseQuery + " ALLOW FILTERING");
    }

    @Test
    public void testShouldFindRowsMatchingMultipleEqualityRestrictions() throws Throwable
    {
        createSimpleTableAndIndex();

        Object[] foo = insertIntoSimpleTable("foo", map("k1", 1));
        Object[] bar = insertIntoSimpleTable("bar", map("k1", 1,
                                                        "k2", 2));
        Object[] baz = insertIntoSimpleTable("baz", map("k2", 2,
                                                        "k3", 3));
        Object[] qux = insertIntoSimpleTable("qux", map("k2", 2,
                                                        "k3", 3,
                                                        "k4", 4));

        assertRowsForConditions(entry("k1", 1),
                                bar, foo);
        assertRowsForConditions(entry("k1", 1).entry("k2", 2),
                                bar);
        assertNoRowsForConditions(entry("k1", 1).entry("k2", 2).entry("k3", 3));
        assertRowsForConditions(entry("k2", 2).entry("k3", 3),
                                baz, qux);
        assertRowsForConditions(entry("k2", 2).entry("k3", 3).entry("k4", 4),
                                qux);
        assertRowsForConditions(entry("k3", 3).entry("k4", 4),
                                qux);
        assertNoRowsForConditions(entry("k3", 3).entry("k4", 4).entry("k5", 5));
    }

    @Test
    public void testShouldFindRowsMatchingEqualityAndContainsRestrictions() throws Throwable
    {
        createSimpleTableAndIndex();

        Object[] foo = insertIntoSimpleTable("foo", map("common", 31415,
                                                        "k1", 1,
                                                        "k2", 2,
                                                        "k3", 3));
        Object[] bar = insertIntoSimpleTable("bar", map("common", 31415,
                                                        "k3", 3,
                                                        "k4", 4,
                                                        "k5", 5));
        Object[] baz = insertIntoSimpleTable("baz", map("common", 31415,
                                                        "k5", 5,
                                                        "k6", 6,
                                                        "k7", 7));

        assertRowsForConditions(entry("common", 31415),
                                bar, baz, foo);
        assertRowsForConditions(entry("common", 31415).key("k1"),
                                foo);
        assertRowsForConditions(entry("common", 31415).key("k2"),
                                foo);
        assertRowsForConditions(entry("common", 31415).key("k3"),
                                bar, foo);
        assertRowsForConditions(entry("common", 31415).key("k3").value(2),
                                foo);
        assertRowsForConditions(entry("common", 31415).key("k3").value(3),
                                bar, foo);
        assertRowsForConditions(entry("common", 31415).key("k3").value(4),
                                bar);
        assertRowsForConditions(entry("common", 31415).key("k3").key("k5"),
                                bar);
        assertRowsForConditions(entry("common", 31415).key("k5"),
                                bar, baz);
        assertRowsForConditions(entry("common", 31415).key("k5").value(4),
                                bar);
        assertRowsForConditions(entry("common", 31415).key("k5").value(5),
                                bar, baz);
        assertRowsForConditions(entry("common", 31415).key("k5").value(6),
                                baz);
        assertNoRowsForConditions(entry("common", 31415).key("k5").value(8));
    }

    @Test
    public void testShouldNotAcceptUnsupportedRelationsOnEntries() throws Throwable
    {
        createSimpleTableAndIndex();
        assertInvalidRelation("< 31415");
        assertInvalidRelation("<= 31415");
        assertInvalidRelation("> 31415");
        assertInvalidRelation(">= 31415");
        assertInvalidRelation("IN (31415, 31416, 31417)");
        assertInvalidRelation("CONTAINS 31415");
        assertInvalidRelation("CONTAINS KEY 'foo'");
    }

    @Test
    public void testShouldRecognizeAlteredOrDeletedMapEntries() throws Throwable
    {
        createSimpleTableAndIndex();
        Object[] foo = insertIntoSimpleTable("foo", map("common", 31415,
                                                        "target", 8192));
        Object[] bar = insertIntoSimpleTable("bar", map("common", 31415,
                                                        "target", 8192));
        Object[] baz = insertIntoSimpleTable("baz", map("common", 31415,
                                                        "target", 8192));

        assertRowsForConditions(entry("target", 8192),
                                bar, baz, foo);
        baz = updateMapInSimpleTable(baz, "target", 4096);
        assertRowsForConditions(entry("target", 8192),
                                bar, foo);
        bar = updateMapInSimpleTable(bar, "target", null);
        assertRowsForConditions(entry("target", 8192),
                                foo);
        execute("DELETE FROM %s WHERE k = 'foo'");
        assertNoRowsForConditions(entry("target", 8192));
        assertRowsForConditions(entry("common", 31415),
                                bar, baz);
        assertRowsForConditions(entry("target", 4096),
                                baz);
    }

    @Test
    public void testShouldRejectQueriesForNullEntries() throws Throwable
    {
        createSimpleTableAndIndex();
        assertInvalid("SELECT * FROM %s WHERE v['somekey'] = null");
    }

    @Test
    public void testShouldTreatQueriesAgainstFrozenMapIndexesAsInvalid() throws Throwable
    {
        createTable("CREATE TABLE %s (k TEXT PRIMARY KEY, v FROZEN<MAP<TEXT, TEXT>>)");
        createIndex("CREATE INDEX ON %s(FULL(V))");

        try
        {
            execute("SELECT * FROM %s WHERE v['somekey'] = 'somevalue'");
            fail("Expected index query to fail");
        }
        catch (InvalidRequestException e)
        {
            String expectedMessage = "Map-entry equality predicates on frozen map column v are not supported";
            assertTrue("Expected error message to contain '" + expectedMessage + "' but got '" +
                       e.getMessage() + "'", e.getMessage().contains(expectedMessage));
        }
    }

    private void assertIndexInvalidForColumn(String colname) throws Throwable
    {
        String query = String.format("CREATE INDEX ON %%s(ENTRIES(%s))", colname);
        assertInvalid(query);
    }

    private void assertReturnsNoRows(String query, Object... params) throws Throwable
    {
        assertRows(execute(query, params));
    }

    private void createSimpleTableAndIndex()
    {
        createTable("CREATE TABLE %s (k TEXT PRIMARY KEY, v MAP<TEXT, INT>)");
        createIndex("CREATE INDEX ON %s(ENTRIES(v))");
    }

    private Object[] insertIntoSimpleTable(String key, Object value) throws Throwable
    {
        String query = "INSERT INTO %s (k, v) VALUES (?, ?)";
        execute(query, key, value);
        return row(key, value);
    }

    private void assertRowsForConditions(IndexWhereClause whereClause, Object[]... rows) throws Throwable
    {
        assertRows(execute("SELECT * FROM %s WHERE " + whereClause.text(), whereClause.params()), rows);
    }

    private void assertNoRowsForConditions(IndexWhereClause whereClause) throws Throwable
    {
        assertRowsForConditions(whereClause);
    }

    private void assertInvalidRelation(String rel) throws Throwable
    {
        String query = "SELECT * FROM %s WHERE v " + rel;
        assertInvalid(query);
    }

    private Object[] updateMapInSimpleTable(Object[] row, String mapKey, Integer mapValue) throws Throwable
    {
        execute("UPDATE %s SET v[?] = ? WHERE k = ?", mapKey, mapValue, row[0]);
        UntypedResultSet rawResults = execute("SELECT * FROM %s WHERE k = ?", row[0]);
        Map<Object, Object> value = (Map<Object, Object>)row[1];
        if (mapValue == null)
        {
            value.remove(mapKey);
        }
        else
        {
            value.put(mapKey, mapValue);
        }
        return row;
    }

    private IndexWhereClause entry(Object key, Object value)
    {
        return (new IndexWhereClause()).entry(key, value);
    }

    private static final class IndexWhereClause
    {
        private final List<String> preds = new ArrayList<>();
        private final List<Object> params = new ArrayList<>();

        public IndexWhereClause entry(Object key, Object value)
        {
            preds.add("v[?] = ?");
            params.add(key);
            params.add(value);
            return this;
        }

        public IndexWhereClause key(Object key)
        {
            preds.add("v CONTAINS KEY ?");
            params.add(key);
            return this;
        }

        public IndexWhereClause value(Object value)
        {
            preds.add("v CONTAINS ?");
            params.add(value);
            return this;
        }

        public String text()
        {
            if (preds.size() == 1)
                return preds.get(0);
            return StringUtils.join(preds, " AND ") + " ALLOW FILTERING";
        }

        public Object[] params()
        {
            return params.toArray();
        }
    }
}
