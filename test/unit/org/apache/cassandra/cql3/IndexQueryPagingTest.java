/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3;

import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import static org.junit.Assert.assertEquals;

public class IndexQueryPagingTest extends CQLTester
{
    /*
     * Some simple tests to verify the behaviour of paging during
     * 2i queries. We only use a single index type (CompositesIndexOnRegular)
     * as the code we want to exercise here is in their abstract
     * base class.
     */

    @Test
    public void pagingOnRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k1 int," +
                    " v1 int," +
                    "PRIMARY KEY (k1))");
        createIndex("CREATE INDEX ON %s(v1)");

        int rowCount = 3;
        for (int i=0; i<rowCount; i++)
            execute("INSERT INTO %s (k1, v1) VALUES (?, ?)", i, 0);

        executePagingQuery("SELECT * FROM %s WHERE v1=0", rowCount);
    }

    @Test
    public void pagingOnRegularColumnWithPartitionRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k1 int," +
                    " c1 int," +
                    " v1 int," +
                    "PRIMARY KEY (k1, c1))");
        createIndex("CREATE INDEX ON %s(v1)");

        int partitions = 3;
        int rowCount = 3;
        for (int i=0; i<partitions; i++)
            for (int j=0; j<rowCount; j++)
                execute("INSERT INTO %s (k1, c1, v1) VALUES (?, ?, ?)", i, j, 0);

        executePagingQuery("SELECT * FROM %s WHERE k1=0 AND v1=0", rowCount);
    }

    @Test
    public void pagingOnRegularColumnWithClusteringRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k1 int," +
                    " c1 int," +
                    " v1 int," +
                    "PRIMARY KEY (k1, c1))");
        createIndex("CREATE INDEX ON %s(v1)");

        int partitions = 3;
        int rowCount = 3;
        for (int i=0; i<partitions; i++)
            for (int j=0; j<rowCount; j++)
                execute("INSERT INTO %s (k1, c1, v1) VALUES (?, ?, ?)", i, j, 0);

        executePagingQuery("SELECT * FROM %s WHERE k1=0 AND c1>=0 AND c1<=3 AND v1=0", rowCount);
    }

    @Test
    public void testPagingOnPartitionsWithoutRows() throws Throwable
    {
        requireNetwork();

        createTable("CREATE TABLE %s (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX on %s(s)");

        execute("INSERT INTO %s (pk, s) VALUES (201, 200);");
        execute("INSERT INTO %s (pk, s) VALUES (202, 200);");
        execute("INSERT INTO %s (pk, s) VALUES (203, 200);");
        execute("INSERT INTO %s (pk, s) VALUES (100, 100);");

        for (int pageSize = 1; pageSize < 10; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("select * from %s where s = 200 and pk = 201;", pageSize),
                          row(201, null, 200, null));

            assertRowsNet(executeNetWithPaging("select * from %s where s = 200;", pageSize),
                          row(201, null, 200, null),
                          row(203, null, 200, null),
                          row(202, null, 200, null));

            assertRowsNet(executeNetWithPaging("select * from %s where s = 100;", pageSize),
                          row(100, null, 100, null));
        }
    }

    @Test
    public void testPagingOnPartitionsWithoutClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        createIndex("CREATE INDEX on %s(v)");

        execute("INSERT INTO %s (pk, v) VALUES (201, 200);");
        execute("INSERT INTO %s (pk, v) VALUES (202, 200);");
        execute("INSERT INTO %s (pk, v) VALUES (203, 200);");
        execute("INSERT INTO %s (pk, v) VALUES (100, 100);");

        for (int pageSize = 1; pageSize < 10; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("select * from %s where v = 200 and pk = 201;", pageSize),
                          row(201, 200));

            assertRowsNet(executeNetWithPaging("select * from %s where v = 200;", pageSize),
                          row(201, 200),
                          row(203, 200),
                          row(202, 200));

            assertRowsNet(executeNetWithPaging("select * from %s where v = 100;", pageSize),
                          row(100, 100));
        }
    }

    private void executePagingQuery(String cql, int rowCount)
    {
        // Execute an index query which should return all rows,
        // setting the fetch size < than the row count. Assert
        // that all rows are returned, so we know that paging
        // of the results was involved.
        Session session = sessionNet();
        Statement stmt = new SimpleStatement(String.format(cql, KEYSPACE + '.' + currentTable()));
        stmt.setFetchSize(rowCount - 1);
        assertEquals(rowCount, session.execute(stmt).all().size());
    }
}
