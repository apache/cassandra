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

import static java.lang.String.format;
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
        createIndex(format("CREATE CUSTOM INDEX ON %%s(c1) USING '%s'", StorageAttachedIndex.class.getName()));

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
        createIndex(format("CREATE CUSTOM INDEX ON %%s(c3) USING '%s'", StorageAttachedIndex.class.getName()));

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
        createIndex(format("CREATE CUSTOM INDEX ON %%s(c2) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX ON %%s(c4) USING '%s'", StorageAttachedIndex.class.getName()));

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
        createIndex(format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));

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
        createIndex(format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));

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
        createIndex(format("CREATE CUSTOM INDEX ON %%s(c2) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX ON %%s(c4) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));

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
    public void testAllowFilteringTextWithINClause ()
    {
        createTable("CREATE TABLE %S (k1 TEXT, k2 TEXT, k3 TEXT, PRIMARY KEY(k1))");
        createIndex("CREATE INDEX ON %s(K2) USING 'sai'");

        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s1','s11','s111')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s2','s11','s11')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s3','s22','s111')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s4','s22','s111')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s5','s31','s111')");

        assertRowCount(execute("SELECT * FROM %s WHERE k2='s11' AND k3 IN ('s11','s111') ALLOW FILTERING"),2);
        assertRowCount(execute("SELECT * FROM %s WHERE k2='s22' AND k3 IN ('s111','s111') ALLOW FILTERING"), 2);
        assertRowCount(execute("SELECT * FROM %s WHERE k2='s22' AND k3 IN ('s','s1') ALLOW FILTERING"), 0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
        assertRowCount(execute("SELECT * FROM %s WHERE k2 IN ('s11','s22') ALLOW FILTERING"), 4);

    }

    @Test
    public void testAllowFilteringIntWithINClause ()
    {
        createTable("CREATE TABLE %S (k1 text, k2 int, k3 int, PRIMARY KEY(k1))");
        createIndex("CREATE INDEX ON %s(K2) USING 'sai'");

        execute("insert into %s (k1,k2,k3) values ('s1',11,1)");
        execute("insert into %s (k1,k2,k3) values ('s2',11,11)");
        execute("insert into %s (k1,k2,k3) values ('s3',11,111)");
        execute("insert into %s (k1,k2,k3) values ('s4',22,1)");
        execute("insert into %s (k1,k2,k3) values ('s5',22,11)");
        execute("insert into %s (k1,k2,k3) values ('s6',22,111)");

        assertRowCount(execute("SELECT * FROM %s WHERE k2=11 AND k3 IN (1,11,111) ALLOW FILTERING"),3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=22 AND k3 IN (1,11,111) ALLOW FILTERING"),3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=22 AND k3 IN (101,102) ALLOW FILTERING"),0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
        assertRowCount(execute("SELECT * FROM %s WHERE k2 IN (11,22) ALLOW FILTERING"), 6);

    }

    @Test
    public void testAllowFilteringBigIntWithINClause ()
    {
            createTable("CREATE TABLE %S (k1 text, k2 bigint, k3 bigint, PRIMARY KEY(k1))");
            createIndex("CREATE INDEX ON %s(K2) USING 'sai'");

            execute("insert into %s (k1, k2, k3) values ('s1', 1001, 100)");
            execute("insert into %s (k1, k2, k3) values ('s2', 1001, 200)");
            execute("insert into %s (k1, k2, k3) values ('s3', 1001, 300)");
            execute("insert into %s (k1, k2, k3) values ('s4', 2002, 100)");
            execute("insert into %s (k1, k2, k3) values ('s5', 2002, 200)");
            execute("insert into %s (k1, k2, k3) values ('s6', 2002, 300)");

            assertRowCount(execute("SELECT * FROM %s WHERE k2=1001 AND k3 IN (100, 200, 300) ALLOW FILTERING"), 3);
            assertRowCount(execute("SELECT * FROM %s WHERE k2=2002 AND k3 IN (100, 200, 300) ALLOW FILTERING"), 3);
            assertRowCount(execute("SELECT * FROM %s WHERE k2=2002 AND k3 IN (101, 201, 301) ALLOW FILTERING"), 0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
            assertRowCount(execute("SELECT * FROM %s WHERE k2 IN (1001,2002) ALLOW FILTERING"), 6);

    }

    @Test
    public void testAllowFilteringBigDecimalWithINClause()
    {
        createTable("CREATE TABLE %S (k1 text, k2 decimal, k3 decimal, PRIMARY KEY(k1))");
        createIndex("CREATE INDEX ON %s(K2) USING 'sai'");

        execute("insert into %s (k1, k2, k3) values ('s1', 1.1, 1.11)");
        execute("insert into %s (k1, k2, k3) values ('s2', 1.1, 1.12)");
        execute("insert into %s (k1, k2, k3) values ('s3', 1.1, 1.13)");
        execute("insert into %s (k1, k2, k3) values ('s4', 2.2, 1.11)");
        execute("insert into %s (k1, k2, k3) values ('s5', 2.2, 1.12)");
        execute("insert into %s (k1, k2, k3) values ('s6', 2.2, 1.13)");

        assertRowCount(execute("SELECT * FROM %s WHERE k2=1.1 AND k3 IN (1.11, 1.12, 1.13) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2.2 AND k3 IN (1.11, 1.12, 1.13) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2.2 AND k3 IN (1.21, 1.22, 1.13) ALLOW FILTERING"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2.2 AND k3 IN (1.21, 1.22, 1.23) ALLOW FILTERING"), 0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
        assertRowCount(execute("SELECT * FROM %s WHERE k2 IN (1.1,2.2) ALLOW FILTERING"), 6);
    }



    private void test(String query, boolean requiresAllowFiltering) throws Throwable
    {
        if (requiresAllowFiltering)
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query);
        else
            assertNotNull(execute(query));

        assertNotNull(execute(query + " ALLOW FILTERING"));
    }

}
