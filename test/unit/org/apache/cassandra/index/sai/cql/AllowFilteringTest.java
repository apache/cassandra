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
import org.apache.cassandra.index.sai.StorageAttachedIndex;

import static org.junit.Assert.assertNotNull;

/**
 * Tests that {@code ALLOW FILTERING} is required only if needed.
 */
public class AllowFilteringTest extends SAITester
{
    @Test
    public void testAllowFilteringOnFirstClusteringKeyColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, v1 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c1) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();

        // with only index restrictions
        test("SELECT * FROM %s WHERE c1=0", false);
        test("SELECT * FROM %s WHERE c1>0", false);
        test("SELECT * FROM %s WHERE c1>0 AND c1<1", false);
        
        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c1=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND v1=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c1=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c1=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c1=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND v1=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c3=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND v1=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3>0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3>0 AND v1=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3=0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3=0 AND v1=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3) = (0, 0, 0)", false);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3) = (0, 0, 0) AND v1=0", true);
    }

    @Test
    public void testAllowFilteringOnNotFirstClusteringKeyColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, c4 int, v1 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3, c4))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c3) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();

        // with only index restrictions
        test("SELECT * FROM %s WHERE c3=0", false);
        test("SELECT * FROM %s WHERE c3>0", false);
        test("SELECT * FROM %s WHERE c3>0 AND c3<1", false);
        
        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c3=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND c4=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND v1=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c3=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c3=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c3=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND v1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c4=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0 AND v1=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND c4=0", false);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND c4=0 AND v1=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0)", false);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0) AND v1=0", true);
    }

    @Test
    public void testAllowFilteringOnMultipleClusteringKeyColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, c4 int, v1 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3, c4))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c2) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c4) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();
        
        // with only index restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4>0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4>0 AND c4<1", false);
        test("SELECT * FROM %s WHERE c2>0 AND c4=0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c2<1 AND c4=0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c4>0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c2<1 AND c4>0 AND c4<1", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND v1=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE k1=2 AND k2=3 AND c1=4 AND c2=0 AND c4=1", true);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND c1=0 AND c2=0 AND c4=0 AND v1=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0 AND v1=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0)", false);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0) AND v1=0", true);
    }

    @Test
    public void testAllowFilteringOnSingleRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY ((k1, k2), c1, c2))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();

        // with only index restrictions
        test("SELECT * FROM %s WHERE v1=0", false);
        test("SELECT * FROM %s WHERE v1>0", false);
        test("SELECT * FROM %s WHERE v1>0 AND v1<1", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE v1=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE v1=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND v2=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0 AND v2=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND v2=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0) AND v2=0", true);
    }

    @Test
    public void testAllowFilteringOnMultipleRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, v1 int, v2 int, v3 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();

        // with only index restrictions
        test("SELECT * FROM %s WHERE v1=0 AND v2=0", false);
        test("SELECT * FROM %s WHERE v1>0 AND v2=0", false);
        test("SELECT * FROM %s WHERE v1>0 AND v1<1 AND v2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2>0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2>0 AND v2<1", false);
        test("SELECT * FROM %s WHERE v1>0 AND v1<1 AND v2>0 AND v2<1", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND v3=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND v3=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND v3=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND v3=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0) AND v3=0", true);
    }

    @Test
    public void testAllowFilteringOnClusteringAndRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, c4 int, v1 int, v2 int, v3 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3, c4))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c2) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c4) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));

        // with only index restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c4>0 AND v1>0 AND v2>0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c2<1 AND c4>0 AND c4<1 AND v1>0 AND v1<0 AND v2>0 AND v2<1", false);
        
        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND v3=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0", false);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND v3=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0 AND v3=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0) AND v3=0", true);
    }

    @Test
    public void testAllowFilteringOnCollectionColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, l list<int>, s set<int>, m_k map<int,int>,"
                    + " m_v map<int,int>, m_en map<int, int>, not_indexed list<int>, PRIMARY KEY ((k1, k2), c1, c2))");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(keys(m_k)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(values(m_v)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(m_en)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // single contains
        test("SELECT * FROM %s WHERE l contains 1", false);
        test("SELECT * FROM %s WHERE s contains 1", false);
        test("SELECT * FROM %s WHERE m_k contains key 1", false);
        test("SELECT * FROM %s WHERE m_v contains 1", false);
        test("SELECT * FROM %s WHERE m_en[1] = 1", false);

        // multiple contains on different indexed columns
        test("SELECT * FROM %s WHERE l contains 1 and s contains 2", false);
        test("SELECT * FROM %s WHERE l contains 1 and m_k contains key 2", false);
        test("SELECT * FROM %s WHERE l contains 1 and m_v contains 2", false);
        test("SELECT * FROM %s WHERE l contains 1 and m_en[2] = 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and s contains 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and m_k contains key 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and m_v contains 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and m_en[2] = 2", false);

        // multiple contains on the same column
        test("SELECT * FROM %s WHERE l contains 1 and l contains 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and s contains 2", false);
        test("SELECT * FROM %s WHERE m_k contains key 1 and m_k contains key 2", false);
        test("SELECT * FROM %s WHERE m_v contains 1 and m_v contains 2", false);
        test("SELECT * FROM %s WHERE m_en[1] = 1 and m_en[2] = 2", false);

        // multiple contains on different columns with not indexed column
        test("SELECT * FROM %s WHERE l contains 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE s contains 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE m_k contains key 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE m_v contains 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE m_en[1] = 1 and not_indexed contains 2", true);
    }

    private void test(String query, boolean requiresAllowFiltering) throws Throwable
    {
        if (requiresAllowFiltering)
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query);
        else
            assertNotNull(execute(query));

        assertNotNull(execute(query + " ALLOW FILTERING"));
    }

    @Test
    public void testUnsupportedIndexRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, d text, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(b) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(d) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();

        execute("INSERT INTO %s (a, b, c, d) VALUES ('Test1', 'Test1', 'Test1', 'Test1')");
        execute("INSERT INTO %s (a, b, c, d) VALUES ('Test2', 'Test2', 'Test2', 'Test2')");
        execute("INSERT INTO %s (a, b, c, d) VALUES ('Test3', 'Test3', 'Test3', 'Test3')");

        // Single restriction
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "b"), "SELECT * FROM %s WHERE b > 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "c"), "SELECT * FROM %s WHERE c > 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "d"), "SELECT * FROM %s WHERE d > 'Test'");

        // Supported and unsupported restriction
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "b"), "SELECT * FROM %s WHERE b > 'Test' AND c = 'Test1'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "c"), "SELECT * FROM %s WHERE c > 'Test' AND d = 'Test1'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "d"), "SELECT * FROM %s WHERE d > 'Test' AND b = 'Test1'");

        // Two unsupported restrictions
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "b"), "SELECT * FROM %s WHERE b > 'Test' AND b < 'Test3'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_MULTI, "[b, c]"), "SELECT * FROM %s WHERE c > 'Test' AND b < 'Test3'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_MULTI, "[b, d]"), "SELECT * FROM %s WHERE d > 'Test' AND b < 'Test3'");

        // The same queries with ALLOW FILTERING should work

        // Single restriction
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b > 'Test' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                    row("Test2", "Test2", "Test2", "Test2"),
                                                                                                    row("Test3", "Test3", "Test3", "Test3"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE c > 'Test' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                    row("Test2", "Test2", "Test2", "Test2"),
                                                                                                    row("Test3", "Test3", "Test3", "Test3"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE d > 'Test' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                    row("Test2", "Test2", "Test2", "Test2"),
                                                                                                    row("Test3", "Test3", "Test3", "Test3"));

        // Supported and unsupported restriction
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b > 'Test' AND c = 'Test1' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE c > 'Test' AND d = 'Test1' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE d > 'Test' AND b = 'Test1' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"));

        // Two unsupported restrictions
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b > 'Test' AND b < 'Test3' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                                    row("Test2", "Test2", "Test2", "Test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE c > 'Test' AND b < 'Test3' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                                    row("Test2", "Test2", "Test2", "Test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE d > 'Test' AND b < 'Test3' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                                    row("Test2", "Test2", "Test2", "Test2"));
    }

    @Test
    public void testIndexedColumnDoesNotSupportLikeRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, d text, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(b) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(d) USING '%s'", StorageAttachedIndex.class.getName()));

        // LIKE restriction
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, "b"), "SELECT * FROM %s WHERE b LIKE 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, "c"), "SELECT * FROM %s WHERE c LIKE 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, "d"), "SELECT * FROM %s WHERE d LIKE 'Test'");
    }
}
