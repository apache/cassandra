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
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SchemaConstants;

/**
 * Tests the virtual table exposing SSTable index metadata.
 */
public class SSTablesSystemViewTest extends SAITester
{
    private static final String SELECT = String.format("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s " +
                                                       "FROM %s.%s WHERE %s = '%s'",
                                                       SSTablesSystemView.INDEX_NAME,
                                                       SSTablesSystemView.SSTABLE_NAME,
                                                       SSTablesSystemView.TABLE_NAME,
                                                       SSTablesSystemView.COLUMN_NAME,
                                                       SSTablesSystemView.FORMAT_VERSION,
                                                       SSTablesSystemView.CELL_COUNT,
                                                       SSTablesSystemView.MIN_ROW_ID,
                                                       SSTablesSystemView.MAX_ROW_ID,
                                                       SSTablesSystemView.START_TOKEN,
                                                       SSTablesSystemView.END_TOKEN,
                                                       SSTablesSystemView.PER_TABLE_DISK_SIZE,
                                                       SSTablesSystemView.PER_COLUMN_DISK_SIZE,
                                                       SchemaConstants.VIRTUAL_VIEWS,
                                                       SSTablesSystemView.NAME,
                                                       SSTablesSystemView.KEYSPACE_NAME,
                                                       KEYSPACE);

    @BeforeClass
    public static void setup() throws Exception
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS, ImmutableList.of(new SSTablesSystemView(SchemaConstants.VIRTUAL_VIEWS))));

        CQLTester.setUpClass();
    }

    @Test
    public void testVirtualTableThroughIndexLifeCycle() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY (k, c))");
        String v1IndexName = createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        String insert = "INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)";

        // the virtual table should be empty before adding contents
        assertEmpty(execute(SELECT));

        // insert a row and verify that the virtual table is empty before flushing
        execute(insert, 1, 10, 100, 1000);
        assertEmpty(execute(SELECT));

        // flush the memtable and verify the new record in the virtual table
        flush();
        Object[] row1 = row(v1IndexName, 1, "v1", 1L, 0L, 0L);
        assertRows(execute(SELECT), row1);

        // flush a second memtable and verify both the old and the new record in the virtual table
        execute(insert, 2, 20, 200, 2000);
        execute(insert, 3, 30, 300, 3000);
        flush();
        Object[] row2 = row(v1IndexName, 2, "v1", 2L, 0L, 1L);
        assertRows(execute(SELECT), row1, row2);

        // create a second index, this should create a new additional entry in the table for each sstable
        String v2IndexName = createIndex("CREATE CUSTOM INDEX ON %s(v2) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        Object[] row3 = row(v2IndexName, 1, "v2", 1L, 0L, 0L);
        Object[] row4 = row(v2IndexName, 2, "v2", 2L, 0L, 1L);
        assertRows(execute(SELECT), row1, row2, row3, row4);

        // create a new sstable that only contains data for the second index, this should add only one new entry
        execute(insert, 4, 40, null, 4000);
        flush();
        Object[] row5 = row(v2IndexName, 3, "v2", 1L, 0L, 0L);
        assertRows(execute(SELECT), row1, row2, row3, row4, row5);

        // create a new sstable with rows with contents for either one of the indexes or the other
        execute(insert, 5, 50, 500, null);
        execute(insert, 6, 60, null, 6000);
        flush();
        Object[] row6 = row(v1IndexName, 4, "v1", 1L, 0L, 0L);
        Object[] row7 = row(v2IndexName, 4, "v2", 1L, 1L, 1L);
        assertRows(execute(SELECT), row1, row2, row6, row3, row4, row5, row7);

        // compact the table and verify that the virtual table has a single entry per index
        compact();
        waitForCompactions();
        // Compaction may result in sstables with generation 5 or 6. Try both.
        // key 4, key 6 are not indexable on v1
        Object[] row8 = row(v1IndexName, 5, 6, "v1", 4L, 0L, 5L);
        // key 5 is not indexable on v2
        Object[] row9 = row(v2IndexName, 5, 6, "v2", 5L, 1L, 5L);
        assertRows(execute(SELECT), row8, row9);

        // drop the first index and verify that there are not entries for it in the table
        dropIndex("DROP INDEX %s." + v1IndexName);
        assertRows(execute(SELECT), row9);

        // drop the base table and verify that the virtual table is empty
        dropTable("DROP TABLE %s");
        assertEmpty(execute(SELECT));
    }

    private Object[] row(String indexName,
                         int generationMin,
                         int generationMax,
                         String columnName,
                         long cellCount,
                         long minSSTableRowId,
                         long maxSSTableRowId) throws Exception
    {
        for (int generation = generationMin; generation <= generationMax; ++generation)
        {
            Object[] row = row(indexName, generation, columnName, cellCount, minSSTableRowId, maxSSTableRowId);
            if (row != null)
                return row;
        }
        return null;
    }

    private Object[] row(String indexName,
                         int generation,
                         String columnName,
                         long cellCount,
                         long minSSTableRowId,
                         long maxSSTableRowId) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);

        for (SSTableIndex sstableIndex : sai.getContext().getView())
        {
            SSTableReader sstable = sstableIndex.getSSTable();

            if (sstable.descriptor.generation == generation)
            {
                Token.TokenFactory tokenFactory = cfs.metadata().partitioner.getTokenFactory();
                AbstractBounds<Token> bounds = sstable.getBounds();

                CompressionParams params = CryptoUtils.getCompressionParams(sstable);

                return row(indexName,
                           sstable.getFilename(),
                           currentTable(),
                           columnName,
                           sstableIndex.getVersion().toString(),
                           cellCount,
                           minSSTableRowId,
                           maxSSTableRowId,
                           tokenFactory.toString(bounds.left),
                           tokenFactory.toString(bounds.right),
                           sstableIndex.getSSTableContext().diskUsage(),
                           sstableIndex.sizeOfPerColumnComponents());
            }
        }
        return null;
    }
}
