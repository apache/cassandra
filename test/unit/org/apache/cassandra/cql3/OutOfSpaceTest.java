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

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

/**
 * Test that TombstoneOverwhelmingException gets thrown when it should be and doesn't when it shouldn't be.
 */
public class OutOfSpaceTest extends CQLTester
{
    @Test
    public void testFlushUnwriteableDie() throws Throwable
    {
        makeTable();

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try (Closeable c = Util.markDirectoriesUnwriteable(getCurrentColumnFamilyStore()))
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.die);
            flushAndExpectError();
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertFalse(killerForTests.wasKilledQuietly()); //only killed quietly on startup failure
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testFlushUnwriteableStop() throws Throwable
    {
        makeTable();

        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try (Closeable c = Util.markDirectoriesUnwriteable(getCurrentColumnFamilyStore()))
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.stop);
            flushAndExpectError();
            Assert.assertFalse(Gossiper.instance.isEnabled());
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
        }
    }

    @Test
    public void testFlushUnwriteableIgnore() throws Throwable
    {
        makeTable();

        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try (Closeable c = Util.markDirectoriesUnwriteable(getCurrentColumnFamilyStore()))
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.ignore);
            flushAndExpectError();
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
        }

        // Next flush should succeed.
        flush();
    }

    public void makeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the amount of tombstones that shouldn't trigger an exception
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");
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
        for (CommitLogSegment segment : CommitLog.instance.segmentManager.getActiveSegments())
            if (segment.getDirtyCFIDs().contains(cfid))
                return;
        fail("Expected commit log to remain dirty for the affected table.");
    }
}
