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

package org.apache.cassandra.cql3.functions.masking;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;

import static java.lang.String.format;

/**
 * Test queries on columns that have attached a dynamic data masking function.
 */
@RunWith(Parameterized.class)
public abstract class ColumnMaskQueryTester extends ColumnMaskTester
{
    @Parameterized.Parameter
    public String order;

    @Parameterized.Parameter(1)
    public String mask;

    @Parameterized.Parameter(2)
    public String columnType;

    @Parameterized.Parameter(3)
    public Object columnValue;

    @Parameterized.Parameter(4)
    public Object maskedValue;

    @Before
    public void setupSchema() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    format("k1 %s, k2 %<s MASKED WITH %s, ", columnType, mask) +
                    format("c1 %s, c2 %<s MASKED WITH %s, ", columnType, mask) +
                    format("r1 %s, r2 %<s MASKED WITH %s, ", columnType, mask) +
                    format("s1 %s static, s2 %<s static MASKED WITH %s, ", columnType, mask) +
                    format("PRIMARY KEY((k1, k2), c1, c2)) WITH CLUSTERING ORDER BY (c1 %s, c2 %<s)", order));

        createView("CREATE MATERIALIZED VIEW %s AS SELECT k2, k1, c2, c1, r2, r1 FROM %s " +
                   "WHERE k1 IS NOT NULL AND k2 IS NOT NULL " +
                   "AND c1 IS NOT NULL AND c2 IS NOT NULL " +
                   "AND r1 IS NOT NULL AND r2 IS NOT NULL " +
                   "PRIMARY KEY ((c2, c1), k2, k1)");

        executeNet("INSERT INTO %s(k1, k2, c1, c2, r1, r2, s1, s2) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                   columnValue, columnValue, columnValue, columnValue, columnValue, columnValue, columnValue, columnValue);
    }

    @Test
    public void testSelectWithWilcard() throws Throwable
    {
        ResultSet rs = executeNet("SELECT * FROM %s");
        assertColumnNames(rs, "k1", "k2", "c1", "c2", "s1", "s2", "r1", "r2");
        assertRowsNet(rs, row(columnValue, maskedValue,
                              columnValue, maskedValue,
                              columnValue, maskedValue,
                              columnValue, maskedValue));

        rs = executeNet(format("SELECT * FROM %s.%s", KEYSPACE, currentView()));
        assertColumnNames(rs, "c2", "c1", "k2", "k1", "r1", "r2");
        assertRowsNet(rs, row(maskedValue, columnValue,
                              maskedValue, columnValue,
                              columnValue, maskedValue));
    }

    @Test
    public void testSelectWithAllColumnNames() throws Throwable
    {
        ResultSet rs = executeNet("SELECT c2, c1, k2, k1, r2, r1, s2, s1 FROM %s");
        assertColumnNames(rs, "c2", "c1", "k2", "k1", "r2", "r1", "s2", "s1");
        assertRowsNet(rs, row(maskedValue, columnValue,
                              maskedValue, columnValue,
                              maskedValue, columnValue,
                              maskedValue, columnValue));

        rs = executeNet(format("SELECT c2, c1, k2, k1, r2, r1 FROM %s.%s", KEYSPACE, currentView()));
        assertColumnNames(rs, "c2", "c1", "k2", "k1", "r2", "r1");
        assertRowsNet(rs, row(maskedValue, columnValue,
                              maskedValue, columnValue,
                              maskedValue, columnValue));
    }

    @Test
    public void testSelectOnlyMaskedColumns() throws Throwable
    {
        ResultSet rs = executeNet("SELECT k2, c2, s2, r2 FROM %s");
        assertColumnNames(rs, "k2", "c2", "s2", "r2");
        assertRowsNet(rs, row(maskedValue, maskedValue, maskedValue, maskedValue));

        rs = executeNet(format("SELECT k2, c2, r2 FROM %s.%s", KEYSPACE, currentView()));
        assertColumnNames(rs, "k2", "c2", "r2");
        assertRowsNet(rs, row(maskedValue, maskedValue, maskedValue));
    }

    @Test
    public void testSelectOnlyNotMaskedColumns() throws Throwable
    {
        ResultSet rs = executeNet("SELECT k1, c1, s1, r1 FROM %s");
        assertColumnNames(rs, "k1", "c1", "s1", "r1");
        assertRowsNet(rs, row(columnValue, columnValue, columnValue, columnValue));

        rs = executeNet(format("SELECT k1, c1, r1 FROM %s.%s", KEYSPACE, currentView()));
        assertColumnNames(rs, "k1", "c1", "r1");
        assertRowsNet(rs, row(columnValue, columnValue, columnValue));
    }
}
