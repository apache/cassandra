/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;
import static org.apache.cassandra.db.context.CounterContext.ContextState;

import static junit.framework.Assert.assertEquals;

public class LongCompactionSpeedTest extends CleanupHelper
{
    public static final String TABLE1 = "Keyspace1";

    /**
     * Test compaction with a very wide row.
     */
    @Test
    public void testCompactionWide() throws Exception
    {
        testCompaction(2, 1, 200000);
    }

    /**
     * Test compaction with lots of skinny rows.
     */
    @Test
    public void testCompactionSlim() throws Exception
    {
        testCompaction(2, 200000, 1);
    }

    /**
     * Test compaction with lots of small sstables.
     */
    @Test
    public void testCompactionMany() throws Exception
    {
        testCompaction(100, 800, 5);
    }

    /**
     * Test aes counter repair with a very wide row.
     */
    @Test
    public void testAESCountersRepairWide() throws Exception
    {
        testAESCountersRepair(2, 1, 500000);
    }

    /**
     * Test aes counter repair with lots of skinny rows.
     */
    @Test
    public void testAESCountersRepairSlim() throws Exception
    {
        testAESCountersRepair(2, 500000, 1);
    }

    /**
     * Test aes counter repair with lots of small sstables.
     */
    @Test
    public void testAESCounterRepairMany() throws Exception
    {
        testAESCountersRepair(100, 1000, 5);
    }

    protected void testCompaction(int sstableCount, int rowsPerSSTable, int colsPerRow) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        ArrayList<SSTableReader> sstables = new ArrayList<SSTableReader>();
        for (int k = 0; k < sstableCount; k++)
        {
            SortedMap<String,ColumnFamily> rows = new TreeMap<String,ColumnFamily>();
            for (int j = 0; j < rowsPerSSTable; j++)
            {
                String key = String.valueOf(j);
                IColumn[] cols = new IColumn[colsPerRow];
                for (int i = 0; i < colsPerRow; i++)
                {
                    // last sstable has highest timestamps
                    cols[i] = Util.column(String.valueOf(i), String.valueOf(i), k);
                }
                rows.put(key, SSTableUtils.createCF(Long.MIN_VALUE, Integer.MIN_VALUE, cols));
            }
            SSTableReader sstable = SSTableUtils.prepare().write(rows);
            sstables.add(sstable);
            store.addSSTable(sstable);
        }

        // give garbage collection a bit of time to catch up
        Thread.sleep(1000);

        long start = System.currentTimeMillis();
        CompactionManager.instance.doCompaction(store, sstables, (int) (System.currentTimeMillis() / 1000) - DatabaseDescriptor.getCFMetaData(TABLE1, "Standard1").getGcGraceSeconds());
        System.out.println(String.format("%s: sstables=%d rowsper=%d colsper=%d: %d ms",
                                         this.getClass().getName(),
                                         sstableCount,
                                         rowsPerSSTable,
                                         colsPerRow,
                                         System.currentTimeMillis() - start));
    }

    protected void testAESCountersRepair(int sstableCount, final int rowsPerSSTable, final int colsPerRow) throws Exception
    {
        final String cfName = "Counter1";
        CompactionManager.instance.disableAutoCompaction();

        ArrayList<SSTableReader> sstables = new ArrayList<SSTableReader>();
        for (int k = 0; k < sstableCount; k++)
        {
            final int sstableNum = k;
            SSTableReader sstable = SSTableUtils.prepare().ks(TABLE1).cf(cfName).write(rowsPerSSTable, new SSTableUtils.Appender(){
                int written = 0;
                public boolean append(SSTableWriter writer) throws IOException
                {
                    if (written > rowsPerSSTable)
                        return false;

                    DecoratedKey key = Util.dk(String.format("%020d", written));
                    ColumnFamily cf = ColumnFamily.create(TABLE1, cfName);
                    for (int i = 0; i < colsPerRow; i++)
                        cf.addColumn(createCounterColumn(String.valueOf(i)));
                    writer.append(key, cf);
                    written++;
                    return true;
                }
            });

            // whack the index to trigger the recover
            FileUtils.deleteWithConfirm(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX));
            FileUtils.deleteWithConfirm(sstable.descriptor.filenameFor(Component.FILTER));

            sstables.add(sstable);
        }

        // give garbage collection a bit of time to catch up
        Thread.sleep(1000);

        long start = System.currentTimeMillis();

        for (SSTableReader sstable : sstables)
            CompactionManager.instance.submitSSTableBuild(sstable.descriptor, OperationType.AES).get();

        System.out.println(String.format("%s: sstables=%d rowsper=%d colsper=%d: %d ms",
                                         this.getClass().getName(),
                                         sstableCount,
                                         rowsPerSSTable,
                                         colsPerRow,
                                         System.currentTimeMillis() - start));
    }

    protected CounterColumn createCounterColumn(String name)
    {
        ContextState context = ContextState.allocate(4, 1);
        context.writeElement(NodeId.fromInt(1), 4L, 2L, true);
        context.writeElement(NodeId.fromInt(2), 4L, 2L);
        context.writeElement(NodeId.fromInt(4), 3L, 3L);
        context.writeElement(NodeId.fromInt(8), 2L, 4L);

        return new CounterColumn(ByteBufferUtil.bytes(name), context.context, 0L);
    }
}
