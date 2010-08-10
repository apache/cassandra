package org.apache.cassandra.db;
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


import java.io.IOException;

import org.junit.Test;

import static org.apache.cassandra.Util.column;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.commitlog.CommitLog;

public class RecoveryManager2Test extends CleanupHelper
{
    @Test
    /* test that commit logs do not replay flushed data */
    public void testWithFlush() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        for (int i = 0; i < 100; i++)
        {
            String key = "key" + i;
            insertRow(key);
        }

        Table table1 = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table1.getColumnFamilyStore("Standard1");
        cfs.forceBlockingFlush();

        // forceBlockingFlush above adds persistent stats to the current commit log segment
        // it ends up in the same segment as key99 meaning that segment still has unwritten data
        // thus the commit log replays it when recover is called below
        Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(StatisticsTable.STATISTICS_CF).forceBlockingFlush();

        // remove all SSTable/MemTables
        cfs.clearUnsafe();

        // replay the commit log (nothing should be replayed since everything was flushed)
        CommitLog.recover();

        // since everything that was flushed was removed (i.e. clearUnsafe)
        // and the commit shouldn't have replayed anything, there should be no data
        assert Util.getRangeSlice(cfs).isEmpty();
    }

    private void insertRow(String key) throws IOException
    {
        RowMutation rm = new RowMutation("Keyspace1", key.getBytes());
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "val1", new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();
    }
}
