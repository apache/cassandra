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
package org.apache.cassandra.db;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryPath;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;

public class CommitLogTest extends CleanupHelper
{
    @Test
    public void testCleanup() throws IOException, ExecutionException, InterruptedException
    {
        assert CommitLog.instance().getSegmentCount() == 1;
        CommitLog.setSegmentSize(1000);

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store1 = table.getColumnFamilyStore("Standard1");
        ColumnFamilyStore store2 = table.getColumnFamilyStore("Standard2");
        RowMutation rm;
        byte[] value = new byte[501];

        // add data.  use relatively large values to force quick segment creation since we have a low flush threshold in the test config.
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation("Keyspace1", "key1".getBytes());
            rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), value, new TimestampClock(0));
            rm.add(new QueryPath("Standard2", null, "Column1".getBytes()), value, new TimestampClock(0));
            rm.apply();
        }
        assert CommitLog.instance().getSegmentCount() > 1;

        // nothing should get removed after flushing just Standard1
        store1.forceBlockingFlush();
        assert CommitLog.instance().getSegmentCount() > 1;

        // after flushing Standard2 we should be able to clean out all segments
        store2.forceBlockingFlush();
        assert CommitLog.instance().getSegmentCount() == 1;
    }

    @Test
    public void testRecoveryWithPartiallyWrittenHeader() throws Exception
    {
        File tmpFile = File.createTempFile("testRecoveryWithPartiallyWrittenHeaderTestFile", null);
        tmpFile.deleteOnExit();
        OutputStream out = new FileOutputStream(tmpFile);
        out.write(new byte[6]);
        //statics make it annoying to test things correctly
        CommitLog.instance().recover(new File[] {tmpFile}); //CASSANDRA-1119 throws on failure
    }
}
