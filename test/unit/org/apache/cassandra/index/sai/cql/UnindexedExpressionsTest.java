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

import static org.junit.Assert.assertTrue;

public class UnindexedExpressionsTest extends SAITester
{
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