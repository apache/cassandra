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
package org.apache.cassandra.db;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AbstractReadQuery#toCQLString()}.
 */
public class AbstractReadQueryToCQLStringTest extends CQLTester
{
    @Test
    public void testSkinnyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");

        // column selection on unrestricted partition range query
        test("SELECT * FROM %s");
        test("SELECT k FROM %s",
             "SELECT * FROM %s");
        test("SELECT v1 FROM %s");
        test("SELECT v2 FROM %s");
        test("SELECT k, v1, v2 FROM %s",
             "SELECT v1, v2 FROM %s");

        // column selection on partition directed query
        test("SELECT * FROM %s WHERE k = 0");
        test("SELECT k FROM %s WHERE k = 0",
             "SELECT * FROM %s WHERE k = 0");
        test("SELECT v1 FROM %s WHERE k = 0");
        test("SELECT v2 FROM %s WHERE k = 0");
        test("SELECT k, v1, v2 FROM %s WHERE k = 0",
             "SELECT v1, v2 FROM %s WHERE k = 0");

        // token restrictions
        test("SELECT * FROM %s WHERE token(k) > 0");
        test("SELECT * FROM %s WHERE token(k) < 0");
        test("SELECT * FROM %s WHERE token(k) >= 0");
        test("SELECT * FROM %s WHERE token(k) <= 0");
        test("SELECT * FROM %s WHERE token(k) = 0",
             "SELECT * FROM %s WHERE token(k) >= 0 AND token(k) <= 0");

        // row filter without indexed column
        test("SELECT * FROM %s WHERE v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 < 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 > 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 <= 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 >= 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k = 0 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k) > 0 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k = 0 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k) > 0 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");

        // row filter with indexed column
        createIndex("CREATE INDEX ON %s (v1)");
        test("SELECT * FROM %s WHERE v1 = 1");
        test("SELECT * FROM %s WHERE v1 < 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 > 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 <= 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 >= 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k) > 0 AND v1 = 1");
        test("SELECT * FROM %s WHERE k = 0 AND v1 = 1",
             "SELECT * FROM %s WHERE token(k) >= token(0) AND token(k) <= token(0) AND v1 = 1");

        // grouped partition-directed queries, maybe producing multiple queries
        test("SELECT * FROM %s WHERE k IN (0)",
             "SELECT * FROM %s WHERE k = 0");
        test("SELECT * FROM %s WHERE k IN (0, 1)",
             "SELECT * FROM %s WHERE k = 0",
             "SELECT * FROM %s WHERE k = 1");
    }

    @Test
    public void testSkinnyTableWithMulticolumnKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v1 int, v2 int, PRIMARY KEY((k1, k2)))");

        // column selection on unrestricted partition range query
        test("SELECT * FROM %s");
        test("SELECT k1 FROM %s",
             "SELECT * FROM %s");
        test("SELECT k2 FROM %s",
             "SELECT * FROM %s");
        test("SELECT v1 FROM %s");
        test("SELECT v2 FROM %s");
        test("SELECT k1, k2, v1, v2 FROM %s",
             "SELECT v1, v2 FROM %s");

        // column selection on partition directed query
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT k1 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT k2 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT v1 FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT v2 FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT k1, k2, v1, v2 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT v1, v2 FROM %s WHERE k1 = 1 AND k2 = 2");

        // token restrictions
        test("SELECT * FROM %s WHERE token(k1, k2) > 0");
        test("SELECT * FROM %s WHERE token(k1, k2) < 0");
        test("SELECT * FROM %s WHERE token(k1, k2) >= 0");
        test("SELECT * FROM %s WHERE token(k1, k2) <= 0");
        test("SELECT * FROM %s WHERE token(k1, k2) = 0",
             "SELECT * FROM %s WHERE token(k1, k2) >= 0 AND token(k1, k2) <= 0");

        // row filter without indexed column
        test("SELECT * FROM %s WHERE k1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND v2 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 = 2 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 = 2 AND v2 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 0 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 0 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");

        // row filter with indexed column
        createIndex("CREATE INDEX ON %s (k1)");
        createIndex("CREATE INDEX ON %s (k2)");
        createIndex("CREATE INDEX ON %s (v1)");
        createIndex("CREATE INDEX ON %s (v2)");
        test("SELECT * FROM %s WHERE k1 = 1");
        test("SELECT * FROM %s WHERE k2 = 2");
        test("SELECT * FROM %s WHERE v1 = 1");
        test("SELECT * FROM %s WHERE v2 = 2");
        test("SELECT * FROM %s WHERE k1 > 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 > 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 > 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v2 > 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND v2 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 = 2 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 = 2 AND v2 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND k1 = 1");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND k2 = 2");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v1 = 1");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v2 = 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND v1 = 1",
             "SELECT * FROM %s WHERE token(k1, k2) >= token(1, 2) AND token(k1, k2) <= token(1, 2) AND v1 = 1");

        // grouped partition-directed queries, maybe producing multiple queries
        test("SELECT * FROM %s WHERE k1 IN (1) AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 IN (2)",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 IN (1) AND k2 IN (2)",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 IN (0, 1) AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 IN (2, 3)",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 3");
        test("SELECT * FROM %s WHERE k1 IN (0, 1) AND k2 IN (2, 3)",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 3",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 3");
    }

    @Test
    public void testWideTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 int, s int static, PRIMARY KEY(k, c))");

        // column selection on unrestricted partition range query
        test("SELECT * FROM %s");
        test("SELECT k FROM %s",
             "SELECT * FROM %s");
        test("SELECT c FROM %s",
             "SELECT * FROM %s");
        test("SELECT s FROM %s");
        test("SELECT v1 FROM %s");
        test("SELECT v2 FROM %s");
        test("SELECT k, c, s, v1, v2 FROM %s",
             "SELECT s, v1, v2 FROM %s");

        // column selection on partition directed query
        test("SELECT * FROM %s WHERE k = 0");
        test("SELECT k FROM %s WHERE k = 0",
             "SELECT * FROM %s WHERE k = 0");
        test("SELECT s FROM %s WHERE k = 0");
        test("SELECT v1 FROM %s WHERE k = 0");
        test("SELECT v2 FROM %s WHERE k = 0");
        test("SELECT k, c, s, v1, v2 FROM %s WHERE k = 0",
             "SELECT s, v1, v2 FROM %s WHERE k = 0");

        // clustering filters
        test("SELECT * FROM %s WHERE k = 0 AND c = 1");
        test("SELECT * FROM %s WHERE k = 0 AND c < 1");
        test("SELECT * FROM %s WHERE k = 0 AND c > 1");
        test("SELECT * FROM %s WHERE k = 0 AND c <= 1");
        test("SELECT * FROM %s WHERE k = 0 AND c >= 1");
        test("SELECT * FROM %s WHERE k = 0 AND c > 1 AND c <= 2");
        test("SELECT * FROM %s WHERE k = 0 AND c >= 1 AND c < 2");

        // token restrictions
        test("SELECT * FROM %s WHERE token(k) > 0");
        test("SELECT * FROM %s WHERE token(k) < 0");
        test("SELECT * FROM %s WHERE token(k) >= 0");
        test("SELECT * FROM %s WHERE token(k) <= 0");
        test("SELECT * FROM %s WHERE token(k) = 0",
             "SELECT * FROM %s WHERE token(k) >= 0 AND token(k) <= 0");

        // row filter without indexed column
        test("SELECT * FROM %s WHERE c = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE s = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k = 0 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k = 0 AND c = 1 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k) > 0 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k = 0 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k = 0 AND c = 1 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k) > 0 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k) > 0 AND c = 1 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");

        // expression filter with indexed column
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (s)");
        createIndex("CREATE INDEX ON %s (v1)");
        test("SELECT * FROM %s WHERE c = 1");
        test("SELECT * FROM %s WHERE v1 = 1");
        test("SELECT * FROM %s WHERE s = 1");
        test("SELECT * FROM %s WHERE v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k) > 0 AND v1 = 1");
        test("SELECT * FROM %s WHERE k = 0 AND v1 = 1",
             "SELECT * FROM %s WHERE token(k) >= token(0) AND token(k) <= token(0) AND v1 = 1");
        test("SELECT * FROM %s WHERE k = 0 AND v1 = 1 AND c = 1",
             "SELECT * FROM %s WHERE token(k) >= token(0) AND token(k) <= token(0) AND c = 1 AND v1 = 1 ALLOW FILTERING");

        // grouped partition-directed queries, maybe producing multiple queries
        test("SELECT * FROM %s WHERE k IN (0)",
             "SELECT * FROM %s WHERE k = 0");
        test("SELECT * FROM %s WHERE k IN (0, 1)",
             "SELECT * FROM %s WHERE k = 0",
             "SELECT * FROM %s WHERE k = 1");
        test("SELECT * FROM %s WHERE k IN (0, 1) AND c = 0",
             "SELECT * FROM %s WHERE k = 0 AND c = 0",
             "SELECT * FROM %s WHERE k = 1 AND c = 0");
        test("SELECT * FROM %s WHERE k IN (0, 1) AND c > 0",
             "SELECT * FROM %s WHERE k = 0 AND c > 0",
             "SELECT * FROM %s WHERE k = 1 AND c > 0");

        // order by
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c",
             "SELECT * FROM %s WHERE k = 0");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c ASC",
             "SELECT * FROM %s WHERE k = 0");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c DESC");

        // order by clustering filter
        test("SELECT * FROM %s WHERE k = 0 AND c = 1 ORDER BY c",
             "SELECT * FROM %s WHERE k = 0 AND c = 1");
        test("SELECT * FROM %s WHERE k = 0 AND c = 1 ORDER BY c ASC",
             "SELECT * FROM %s WHERE k = 0 AND c = 1");
        test("SELECT * FROM %s WHERE k = 0 AND c = 1 ORDER BY c DESC");
    }

    @Test
    public void testWideTableWithMulticolumnKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, v1 int, v2 int, PRIMARY KEY((k1, k2), c1, c2, c3))");

        // column selection on unrestricted partition range query
        test("SELECT * FROM %s");
        test("SELECT k1 FROM %s",
             "SELECT * FROM %s");
        test("SELECT k2 FROM %s",
             "SELECT * FROM %s");
        test("SELECT c1 FROM %s",
             "SELECT * FROM %s");
        test("SELECT c2 FROM %s",
             "SELECT * FROM %s");
        test("SELECT c3 FROM %s",
             "SELECT * FROM %s");
        test("SELECT v1 FROM %s");
        test("SELECT v2 FROM %s");
        test("SELECT k1, k2, c1, c2, c3, v1, v2 FROM %s",
             "SELECT v1, v2 FROM %s");

        // column selection on partition directed query
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT k1 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT k2 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT c1 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT c2 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT v1 FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT v2 FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT k1, k2, c1, c2, v1, v2 FROM %s WHERE k1 = 1 AND k2 = 2",
             "SELECT v1, v2 FROM %s WHERE k1 = 1 AND k2 = 2");

        // clustering filters
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 < 1");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 > 1");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 <= 1");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 >= 1");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 > 1 AND c1 < 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 > 1 AND c1 <= 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 >= 1 AND c1 < 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 > 1 AND c1 < 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 < 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 > 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 <= 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 >= 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 > 2 AND c2 < 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 > 2 AND c2 <= 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 >= 2 AND c2 < 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 > 2 AND c2 < 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 = 3",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND (c1, c2, c3) = (1, 2, 3)");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 > 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 < 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 >= 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 <= 3");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 > 3 AND c3 < 4");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 > 3 AND c3 <= 4");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 >= 3 AND c3 < 4");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND c2 = 2 AND c3 >= 3 AND c3 <= 4");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND (c1, c2, c3) = (1, 2, 3)");

        // token restrictions
        test("SELECT * FROM %s WHERE token(k1, k2) > 0");
        test("SELECT * FROM %s WHERE token(k1, k2) < 0");
        test("SELECT * FROM %s WHERE token(k1, k2) >= 0");
        test("SELECT * FROM %s WHERE token(k1, k2) <= 0");
        test("SELECT * FROM %s WHERE token(k1, k2) = 0",
             "SELECT * FROM %s WHERE token(k1, k2) >= 0 AND token(k1, k2) <= 0");

        // row filter without indexed column
        test("SELECT * FROM %s WHERE k1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE c1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE c2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE c3 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v1 = 1 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND c1 = 1 AND v1 = 1 AND v2 = 2 ALLOW FILTERING");

        // expression filter with indexed column
        createIndex("CREATE INDEX ON %s (k1)");
        createIndex("CREATE INDEX ON %s (k2)");
        createIndex("CREATE INDEX ON %s (c1)");
        createIndex("CREATE INDEX ON %s (c2)");
        createIndex("CREATE INDEX ON %s (c3)");
        createIndex("CREATE INDEX ON %s (v1)");
        createIndex("CREATE INDEX ON %s (v2)");
        test("SELECT * FROM %s WHERE k1 = 1");
        test("SELECT * FROM %s WHERE k2 = 2");
        test("SELECT * FROM %s WHERE c1 = 1");
        test("SELECT * FROM %s WHERE c2 = 2");
        test("SELECT * FROM %s WHERE c3 = 3");
        test("SELECT * FROM %s WHERE v1 = 1");
        test("SELECT * FROM %s WHERE v2 = 2");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT * FROM %s WHERE c1 = 1 AND c2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE c1 = 1 AND c2 = 2 AND c3 = 3 ALLOW FILTERING",
             "SELECT * FROM %s WHERE (c1, c2, c3) = (1, 2, 3) ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v1 = 1 AND v2 = 2 ALLOW FILTERING");
        test("SELECT * FROM %s WHERE token(k1, k2) > 0 AND v1 = 1");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND v1 = 1",
             "SELECT * FROM %s WHERE token(k1, k2) >= token(1, 2) AND token(k1, k2) <= token(1, 2) AND v1 = 1");
        test("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c1 = 1 AND v1 = 1",
             "SELECT * FROM %s WHERE token(k1, k2) >= token(1, 2) AND token(k1, k2) <= token(1, 2) AND c1 = 1 AND v1 = 1 ALLOW FILTERING");

        // grouped partition-directed queries, maybe producing multiple queries
        test("SELECT * FROM %s WHERE k1 IN (1) AND k2 IN (2)",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 IN (1, 2) AND k2 IN (3, 4)",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 3",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 4",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 3",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 4");
        test("SELECT * FROM %s WHERE k1 IN (1, 2) AND k2 IN (3, 4) AND c1 = 0",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 3 AND c1 = 0",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 4 AND c1 = 0",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 3 AND c1 = 0",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 4 AND c1 = 0");
        test("SELECT * FROM %s WHERE k1 IN (1, 2) AND k2 IN (3, 4) AND c1 > 0",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 3 AND c1 > 0",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 4 AND c1 > 0",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 3 AND c1 > 0",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 4 AND c1 > 0");
        test("SELECT * FROM %s WHERE k1 IN (1, 2) AND k2 IN (3, 4) AND (c1, c2, c3) IN ((5, 6, 7), (8, 9, 10))",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 3 AND (c1, c2, c3) IN ((5, 6, 7), (8, 9, 10))",
             "SELECT * FROM %s WHERE k1 = 1 AND k2 = 4 AND (c1, c2, c3) IN ((5, 6, 7), (8, 9, 10))",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 3 AND (c1, c2, c3) IN ((5, 6, 7), (8, 9, 10))",
             "SELECT * FROM %s WHERE k1 = 2 AND k2 = 4 AND (c1, c2, c3) IN ((5, 6, 7), (8, 9, 10))");

        // order by
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1 ASC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1 DESC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1 DESC, c2 DESC, c3 DESC");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1, c2",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1, c2 ASC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1 ASC, c2",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1 ASC, c2 ASC, c3 ASC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 ORDER BY c1 DESC, c2 DESC, c3 DESC");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1 ASC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1");

        // order by clustering filter
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1 DESC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1 DESC, c2 DESC, c3 DESC");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1, c2",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1, c2 ASC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1 ASC, c2",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1 ASC, c2 ASC, c3 ASC",
             "SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1");
        test("SELECT * FROM %s WHERE k1 = 0 AND k2 = 2 AND c1 = 1 ORDER BY c1 DESC, c2 DESC, c3 DESC");
    }

    @Test
    public void testQuotedNames() throws Throwable
    {
        createKeyspace("CREATE KEYSPACE \"K\" WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createTable("CREATE TABLE \"K\".\"T\" (\"K\" int, \"C\" int, \"S\" int static, \"V\" int, PRIMARY KEY(\"K\", \"C\"))");

        // column selection on unrestricted partition range query
        test("SELECT * FROM \"K\".\"T\"");
        test("SELECT \"K\" FROM \"K\".\"T\"",
             "SELECT * FROM \"K\".\"T\"");
        test("SELECT \"S\" FROM \"K\".\"T\"");
        test("SELECT \"V\" FROM \"K\".\"T\"");
        test("SELECT \"K\", \"C\", \"S\", \"V\" FROM \"K\".\"T\"",
             "SELECT \"S\", \"V\" FROM \"K\".\"T\"");

        // column selection on partition directed query
        test("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0");
        test("SELECT \"K\" FROM \"K\".\"T\" WHERE \"K\" = 0",
             "SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0");
        test("SELECT \"S\" FROM \"K\".\"T\" WHERE \"K\" = 0");
        test("SELECT \"V\" FROM \"K\".\"T\" WHERE \"K\" = 0");
        test("SELECT \"K\", \"C\", \"S\", \"V\" FROM \"K\".\"T\" WHERE \"K\" = 0",
             "SELECT \"S\", \"V\" FROM \"K\".\"T\" WHERE \"K\" = 0");

        // filters
        test("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" = 1");
        test("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" > 1 AND \"C\" <= 2");
        test("SELECT * FROM \"K\".\"T\" WHERE \"V\" = 0 ALLOW FILTERING");
        test("SELECT * FROM \"K\".\"T\" WHERE \"S\" = 0 ALLOW FILTERING");
        test("SELECT * FROM \"K\".\"T\" WHERE \"C\" = 0 ALLOW FILTERING");

        // order by
        test("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 ORDER BY \"C\" DESC");
        test("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" = 1 ORDER BY \"C\" DESC");
    }

    @Test
    public void testLiterals() throws Throwable
    {
        // skinny table
        createTable("CREATE TABLE %s (k text, c text, v text, PRIMARY KEY(k, c))");
        test("SELECT * FROM %s WHERE k = 'A'");
        test("SELECT * FROM %s WHERE c = 'A' ALLOW FILTERING");
        test("SELECT * FROM %s WHERE v = 'A' ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k = 'A' AND c = 'B'");
        test("SELECT * FROM %s WHERE k = 'A' AND v = 'B' ALLOW FILTERING");

        // wide table
        createTable("CREATE TABLE %s (k1 text, k2 text, c1 text, c2 text, v text, PRIMARY KEY((k1, k2), c1, c2))");
        test("SELECT * FROM %s WHERE k1 = 'A' ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k2 = 'A' ALLOW FILTERING");
        test("SELECT * FROM %s WHERE c1 = 'A' ALLOW FILTERING");
        test("SELECT * FROM %s WHERE c2 = 'A' ALLOW FILTERING");
        test("SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B'");
        test("SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B' AND c1 = 'C'");
        test("SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B' AND c1 > 'C'");
        test("SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B' AND c1 > 'C' AND c1 <= 'D'");
        test("SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B' AND c1 = 'C' AND c2 = 'D'",
             "SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B' AND (c1, c2) = ('C', 'D')");
        test("SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B' AND c1 = 'C' AND c2 > 'D'");
        test("SELECT * FROM %s WHERE k1 = 'A' AND k2 = 'B' AND c1 = 'C' AND c2 > 'D' AND c2 <= 'E'");
    }

    @Test
    public void testWideTableWithClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, c3 int, PRIMARY KEY(k, c1, c2, c3)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC, c3 DESC)");

        // one column
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1",
             "SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC",
             "SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC",
             "SELECT * FROM %s WHERE k = 0");

        // two columns
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1, c2 DESC",
             "SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC",
             "SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 ASC",
             "SELECT * FROM %s WHERE k = 0");

        // three columns
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1, c2 DESC, c3 ASC",
             "SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1, c2 DESC, c3 ASC",
             "SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC",
             "SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC, c3 ASC");
        test("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 ASC, c3 DESC",
             "SELECT * FROM %s WHERE k = 0");
    }

    @Test
    public void testCollections() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a text, b int)");
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "l list<text>, " +
                    "s set<text>, " +
                    "m map<text, text>, " +
                    "t tuple<text, int>, " +
                    "u " + udt + ")");

        // column selections
        test("SELECT l FROM %s");
        test("SELECT s FROM %s");
        test("SELECT m FROM %s");
        test("SELECT t FROM %s");
        test("SELECT u FROM %s");
        testInvalid("SELECT l['a'] FROM %s");
        test("SELECT s['a'] FROM %s");
        test("SELECT m['a'] FROM %s");
        test("SELECT u.a FROM %s",
             "SELECT u FROM %s");
        test("SELECT m['a'], m['b'], s['c'], s['d'], t, u.a, u.b FROM %s",
             "SELECT m['a'], m['b'], s['c'], s['d'], t, u FROM %s");

        // filtering
        testInvalid("SELECT * FROM %s WHERE l = ['a', 'b'] ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE s = {'a', 'b'} ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE m = {'a': 'b', 'c': 'd'} ALLOW FILTERING");
        test("SELECT * FROM %s WHERE t = ('a', 1) ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE u = {a: 'a', b: 1} ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE l['a'] = 'b' ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE s['a'] = 'b' ALLOW FILTERING");
        test("SELECT * FROM %s WHERE m['a'] = 'b' ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE u.a = 'a' ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE u.b = 0 ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE u.a = 'a' ANd u.b = 0 ALLOW FILTERING");
    }

    @Test
    public void testFrozenCollections() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a text, b int)");
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "l frozen<list<text>>, " +
                    "s frozen<set<text>>, " +
                    "m frozen<map<text, text>>, " +
                    "t frozen<tuple<text, int>>, " +
                    "u frozen<" + udt + ">)");

        // column selections
        test("SELECT l FROM %s");
        test("SELECT s FROM %s");
        test("SELECT m FROM %s");
        test("SELECT t FROM %s");
        test("SELECT u FROM %s");
        testInvalid("SELECT l['a'] FROM %s");
        test("SELECT s['a'] FROM %s",
             "SELECT s FROM %s");
        test("SELECT m['a'] FROM %s",
             "SELECT m FROM %s");
        test("SELECT u.a FROM %s",
             "SELECT u FROM %s");
        test("SELECT m['a'], m['b'], s['c'], s['d'], t, u.a, u.b FROM %s",
             "SELECT m, s, t, u FROM %s");

        // filtering
        test("SELECT * FROM %s WHERE l = ['a', 'b'] ALLOW FILTERING");
        test("SELECT * FROM %s WHERE s = {'a', 'b'} ALLOW FILTERING");
        test("SELECT * FROM %s WHERE m = {'a': 'b', 'c': 'd'} ALLOW FILTERING");
        test("SELECT * FROM %s WHERE t = ('a', 1) ALLOW FILTERING");
        test("SELECT * FROM %s WHERE u = {a: 'a', b: 1} ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE l['a'] = 'a' ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE s['a'] = 'a' ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE m['a'] = 'a' ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE u.a = 'a' ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE u.b = 0 ALLOW FILTERING");
        testInvalid("SELECT * FROM %s WHERE u.a = 'a' ANd u.b = 0 ALLOW FILTERING");
    }

    @Test
    public void testVirtualTable() throws Throwable
    {
        TableMetadata metadata =
        TableMetadata.builder("vk", "vt")
                     .kind(TableMetadata.Kind.VIRTUAL)
                     .addPartitionKeyColumn("k", Int32Type.instance)
                     .addClusteringColumn("c", Int32Type.instance)
                     .addRegularColumn("v", Int32Type.instance)
                     .addStaticColumn("s", Int32Type.instance)
                     .build();
        SimpleDataSet data = new SimpleDataSet(metadata);
        VirtualTable table = new AbstractVirtualTable(metadata)
        {
            public DataSet data()
            {
                return data;
            }
        };
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace("vk", ImmutableList.of(table)));

        // column selection on unrestricted partition range query
        test("SELECT * FROM vk.vt");
        test("SELECT k FROM vk.vt",
             "SELECT * FROM vk.vt");
        test("SELECT c FROM vk.vt",
             "SELECT * FROM vk.vt");
        test("SELECT s FROM vk.vt");
        test("SELECT v FROM vk.vt");
        test("SELECT k, c, s, v FROM vk.vt",
             "SELECT s, v FROM vk.vt");

        // column selection on partition directed query
        test("SELECT * FROM vk.vt WHERE k = 1");
        test("SELECT k FROM vk.vt WHERE k = 1",
             "SELECT * FROM vk.vt WHERE k = 1");
        test("SELECT c FROM vk.vt WHERE k = 1",
             "SELECT * FROM vk.vt WHERE k = 1");
        test("SELECT v FROM vk.vt WHERE k = 1");
        test("SELECT s FROM vk.vt WHERE k = 1");
        test("SELECT k, c, s, v FROM vk.vt WHERE k = 1",
             "SELECT s, v FROM vk.vt WHERE k = 1");

        // clustering filters
        test("SELECT * FROM vk.vt WHERE k = 0 AND c = 1");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c < 1");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c > 1");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c <= 1");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c >= 1");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c > 1 AND c <= 2");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c >= 1 AND c < 2");

        // token restrictions
        test("SELECT * FROM vk.vt WHERE token(k) > 0");
        test("SELECT * FROM vk.vt WHERE token(k) < 0");
        test("SELECT * FROM vk.vt WHERE token(k) >= 0");
        test("SELECT * FROM vk.vt WHERE token(k) <= 0");
        test("SELECT * FROM vk.vt WHERE token(k) = 0",
             "SELECT * FROM vk.vt WHERE token(k) >= 0 AND token(k) <= 0");

        // row filters
        test("SELECT * FROM vk.vt WHERE c = 1 ALLOW FILTERING");
        test("SELECT * FROM vk.vt WHERE s = 1 ALLOW FILTERING");
        test("SELECT * FROM vk.vt WHERE v = 1 ALLOW FILTERING");
        test("SELECT * FROM vk.vt WHERE k = 0 AND v = 1 ALLOW FILTERING");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c = 1 AND v = 1 ALLOW FILTERING");
        test("SELECT * FROM vk.vt WHERE token(k) > 0 AND v = 1 ALLOW FILTERING");
        test("SELECT * FROM vk.vt WHERE token(k) > 0 AND c = 1 AND v = 1 ALLOW FILTERING");

        // grouped partition-directed queries, maybe producing multiple queries
        test("SELECT * FROM vk.vt WHERE k IN (0)",
             "SELECT * FROM vk.vt WHERE k = 0");
        test("SELECT * FROM vk.vt WHERE k IN (0, 1)",
             "SELECT * FROM vk.vt WHERE k = 0",
             "SELECT * FROM vk.vt WHERE k = 1");
        test("SELECT * FROM vk.vt WHERE k IN (0, 1) AND c = 0",
             "SELECT * FROM vk.vt WHERE k = 0 AND c = 0",
             "SELECT * FROM vk.vt WHERE k = 1 AND c = 0");
        test("SELECT * FROM vk.vt WHERE k IN (0, 1) AND c > 0",
             "SELECT * FROM vk.vt WHERE k = 0 AND c > 0",
             "SELECT * FROM vk.vt WHERE k = 1 AND c > 0");

        // order by
        test("SELECT * FROM vk.vt WHERE k = 0 ORDER BY c",
             "SELECT * FROM vk.vt WHERE k = 0");
        test("SELECT * FROM vk.vt WHERE k = 0 ORDER BY c ASC",
             "SELECT * FROM vk.vt WHERE k = 0");
        test("SELECT * FROM vk.vt WHERE k = 0 ORDER BY c DESC");

        // order by clustering filter
        test("SELECT * FROM vk.vt WHERE k = 0 AND c = 1 ORDER BY c",
             "SELECT * FROM vk.vt WHERE k = 0 AND c = 1");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c = 1 ORDER BY c ASC",
             "SELECT * FROM vk.vt WHERE k = 0 AND c = 1");
        test("SELECT * FROM vk.vt WHERE k = 0 AND c = 1 ORDER BY c DESC");
    }

    private List<String> toCQLString(String query)
    {
        String fullQuery = formatQuery(query);
        ClientState state = ClientState.forInternalCalls();
        CQLStatement statement = QueryProcessor.getStatement(fullQuery, state);

        assertTrue(statement instanceof SelectStatement);
        SelectStatement select = (SelectStatement) statement;

        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ReadQuery readQuery = select.getQuery(options, FBUtilities.nowInSeconds());

        if (readQuery instanceof SinglePartitionReadCommand.Group)
        {
            SinglePartitionReadCommand.Group group = (SinglePartitionReadCommand.Group) readQuery;
            return group.queries.stream().map(AbstractReadQuery::toCQLString).collect(Collectors.toList());
        }
        else
        {
            assertTrue(readQuery instanceof AbstractReadQuery);
            return Collections.singletonList(((AbstractReadQuery) readQuery).toCQLString());
        }
    }

    private void test(String query) throws Throwable
    {
        test(query, query);
    }

    private void test(String query, String... expected) throws Throwable
    {
        List<String> actual = toCQLString(query);
        List<String> fullExpected = Stream.of(expected)
                                          .map(this::formatQuery)
                                          .map(s -> s.endsWith(" ALLOW FILTERING") ? s : s + " ALLOW FILTERING")
                                          .collect(Collectors.toList());
        assertEquals(fullExpected, actual);

        // execute both the expected output commands to verify that they are valid CQL
        for (String q : expected)
            execute(q);
    }

    private void testInvalid(String query) throws Throwable
    {
        assertInvalidThrow(RequestValidationException.class, query);
    }
}
