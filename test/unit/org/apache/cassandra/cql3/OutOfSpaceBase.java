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
package org.apache.cassandra.cql3;

import static junit.framework.Assert.fail;

import java.io.IOError;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Assert;

import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSWriteError;

/**
 * Test that exceptions during flush are treated according to the disk failure policy.
 * We cannot recover after a failed flush due to postFlushExecutor being stuck, so each test needs to run separately.
 */
public class OutOfSpaceBase extends CQLTester
{
    public void makeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");
    }

    public void markDirectoriesUnwriteable()
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        try
        {
            for ( ; ; )
            {
                DataDirectory dir = cfs.directories.getWriteableLocation(1);
                DisallowedDirectories.maybeMarkUnwritable(cfs.directories.getLocationForDisk(dir));
            }
        }
        catch (IOError e)
        {
            // Expected -- marked all directories as unwritable
        }
    }

    public void flushAndExpectError() throws InterruptedException, ExecutionException
    {
        try
        {
            Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable()).forceFlush().get();
            fail("FSWriteError expected.");
        }
        catch (ExecutionException e)
        {
            // Correct path.
            Assert.assertTrue(e.getCause() instanceof FSWriteError);
        }

        // Make sure commit log wasn't discarded.
        UUID cfid = currentTableMetadata().cfId;
        for (CommitLogSegment segment : CommitLog.instance.allocator.getActiveSegments())
            if (segment.getDirtyCFIDs().contains(cfid))
                return;
        fail("Expected commit log to remain dirty for the affected table.");
    }


    @After
    public void afterTest() throws Throwable
    {
        // Override CQLTester's afterTest method; clean-up will fail due to flush failing.
    }
}
