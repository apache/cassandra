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
package org.apache.cassandra.index.sai.virtual;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.SchemaConstants;

/**
 * Tests the virtual table exposing storage-attached column index metadata.
 */
public class IndexesSystemViewTest extends SAITester
{
    private static final String SELECT = String.format("SELECT %s, %s, %s, %s, %s, %s, %s FROM %s.%s WHERE %s = '%s'",
                                                       IndexesSystemView.INDEX_NAME,
                                                       IndexesSystemView.TABLE_NAME,
                                                       IndexesSystemView.COLUMN_NAME,
                                                       IndexesSystemView.IS_QUERYABLE,
                                                       IndexesSystemView.IS_BUILDING,
                                                       IndexesSystemView.IS_STRING,
                                                       IndexesSystemView.ANALYZER,
                                                       SchemaConstants.VIRTUAL_VIEWS,
                                                       IndexesSystemView.NAME,
                                                       IndexesSystemView.KEYSPACE_NAME,
                                                       KEYSPACE);

    private static final Injections.Barrier blockIndexBuild = Injections.newBarrier("block_index_build", 2, false)
                                                                        .add(InvokePointBuilder.newInvokePoint()
                                                                                               .onClass(StorageAttachedIndex.class)
                                                                                               .onMethod("startInitialBuild"))
                                                                        .build();

    @BeforeClass
    public static void setup() throws Exception
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS, ImmutableList.of(new IndexesSystemView(SchemaConstants.VIRTUAL_VIEWS))));

        CQLTester.setUpClass();
    }

    @Test
    public void testVirtualTableThroughIndexLifeCycle() throws Throwable
    {
        // create the table and verify that the virtual table is empty before creating any indexes
        assertEmpty(execute(SELECT));
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 text, PRIMARY KEY (k, c))");

        // create the index simulating a long build and verify that there is an empty record in the virtual table
        Injections.inject(blockIndexBuild);
        String v1IndexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));

        assertRows(execute(SELECT), row(v1IndexName, "v1", false, true, false));

        // unblock the long build and verify that there is an finished empty record in the virtual table
        blockIndexBuild.countDown();
        blockIndexBuild.disable();
        waitForIndexQueryable();
        assertRows(execute(SELECT), row(v1IndexName, "v1", true, false, false));

        // insert some data and verify that virtual table record is still empty since we haven't flushed yet
        execute("INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)", 1, 10, 100, "1000");
        execute("INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)", 2, 20, 200, "2000");
        assertRows(execute(SELECT), row(v1IndexName, "v1", true, false, false));

        // flush the memtable and verify the not-empty record in the virtual table
        flush();
        assertRows(execute(SELECT), row(v1IndexName, "v1", true, false, false));

        // flush a second memtable and verify the updated record in the virtual table
        execute("INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)", 3, 30, 300, "3000");
        flush();
        assertRows(execute(SELECT), row(v1IndexName, "v1", true, false, false));

        // create a second index, this should create a new additional entry in the table
        String v2IndexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();
        assertRows(execute(SELECT),
                   row(v1IndexName, "v1", true, false, false),
                   row(v2IndexName, "v2", true, false, true));

        // update some of the existing rows, this should increase the cell count due to the multiple versions
        execute("INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)", 1, 10, 111, "1111");
        execute("INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)", 2, 20, 222, "2222");
        flush();
        assertRowsIgnoringOrderAndExtra(execute(SELECT),
                                        row(v1IndexName, "v1", true, false, false),
                                        row(v2IndexName, "v2", true, false, true));

        // compact and verify that the cell count decreases
        compact();
        waitForCompactionsFinished();

        assertRowsIgnoringOrderAndExtra(execute(SELECT),
                                        row(v1IndexName, "v1", true, false, false),
                                        row(v2IndexName, "v2", true, false, true));



        // drop the second index and verify that there is not entry for it in the virtual table
        dropIndex("DROP INDEX %s." + v2IndexName);
        assertRowsIgnoringOrderAndExtra(execute(SELECT), row(v1IndexName, "v1", true, false, false));

        // truncate the base table and verify that there is still an entry in the virtual table and it's empty
        truncate(false);
        assertRowsIgnoringOrderAndExtra(execute(SELECT), row(v1IndexName, "v1", true, false, false));

        // drop the base table and verify that the virtual table is empty
        dropTable("DROP TABLE %s");
        assertEmpty(execute(SELECT));
    }

    private Object[] row(String indexName,
                         String columnName,
                         boolean isQueryable,
                         boolean isBuilding,
                         boolean isString)
    {
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
            IndexContext context = sai.getIndexContext();

            return row(indexName,
                       currentTable(),
                       columnName,
                       isQueryable,
                       isBuilding,
                       isString,
                       context.getIndexAnalyzerFactory().toString());
    }
}
