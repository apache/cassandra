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

package org.apache.cassandra.index.sai.metrics;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;

public class IndexSelectionTest extends AbstractMetricsTest
{
    @Test
    public void shouldSelectLegacyIndexByDefault()
    {
        createTable("CREATE TABLE %s (pk int, ck int, val1 int, val2 int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE INDEX ON %s(val1) USING 'legacy_local_table'");
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");

        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 1, 1, 2, 1);

        assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2"), row(1, 1, 2, 1));
        assertEquals(0L, getTableQueryMetrics(KEYSPACE, currentTable(), "TotalQueriesCompleted"));

        DatabaseDescriptor.setPrioritizeSAIOverLegacyIndex(true);
        assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2"), row(1, 1, 2, 1));
        assertEquals(1L, getTableQueryMetrics(KEYSPACE, currentTable(), "TotalQueriesCompleted"));

        DatabaseDescriptor.setPrioritizeSAIOverLegacyIndex(false);
        assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2"), row(1, 1, 2, 1));
        assertEquals(1L, getTableQueryMetrics(KEYSPACE, currentTable(), "TotalQueriesCompleted"));
    }

    @Test
    public void shouldSelectLegacyIndexByDefaultForAndQueries()
    {
        createTable("CREATE TABLE %s (pk int, ck int, val1 int, val2 int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE INDEX ON %s(val1) USING 'legacy_local_table'");
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(val2) USING 'legacy_local_table'");
        createIndex("CREATE INDEX ON %s(val2) USING 'sai'");

        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 1, 1, 2, 1);

        assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2 AND val2 = 1"), row(1, 1, 2, 1));
        assertEquals(0L, getTableQueryMetrics(KEYSPACE, currentTable(), "TotalQueriesCompleted"));

        DatabaseDescriptor.setPrioritizeSAIOverLegacyIndex(true);
        assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2 AND val2 = 1"), row(1, 1, 2, 1));
        assertEquals(1L, getTableQueryMetrics(KEYSPACE, currentTable(), "TotalQueriesCompleted"));

        DatabaseDescriptor.setPrioritizeSAIOverLegacyIndex(false);
        assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2 AND val2 = 1"), row(1, 1, 2, 1));
        assertEquals(1L, getTableQueryMetrics(KEYSPACE, currentTable(), "TotalQueriesCompleted"));
    }
}
