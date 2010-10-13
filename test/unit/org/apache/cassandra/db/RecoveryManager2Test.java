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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.Util.column;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.commitlog.CommitLog;

public class RecoveryManager2Test extends CleanupHelper
{
    private static Logger logger = LoggerFactory.getLogger(RecoveryManager2Test.class);

    @Test
    /* test that commit logs do not replay flushed data */
    public void testWithFlush() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        // add a row to another CF so we test skipping mutations within a not-entirely-flushed CF
        insertRow("Standard2", "key");

        for (int i = 0; i < 100; i++)
        {
            String key = "key" + i;
            insertRow("Standard1", key);
        }

        Table table1 = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table1.getColumnFamilyStore("Standard1");
        logger.debug("forcing flush");
        cfs.forceBlockingFlush();

        // remove Standard1 SSTable/MemTables
        cfs.clearUnsafe();

        logger.debug("begin manual replay");
        // replay the commit log (nothing should be replayed since everything was flushed)
        CommitLog.recover();

        // since everything that was flushed was removed (i.e. clearUnsafe)
        // and the commit shouldn't have replayed anything, there should be no data
        assert Util.getRangeSlice(cfs).isEmpty();
    }

    private void insertRow(String cfname, String key) throws IOException
    {
        RowMutation rm = new RowMutation("Keyspace1", key.getBytes());
        ColumnFamily cf = ColumnFamily.create("Keyspace1", cfname);
        cf.addColumn(column("col1", "val1", 1L));
        rm.add(cf);
        rm.apply();
    }
}
