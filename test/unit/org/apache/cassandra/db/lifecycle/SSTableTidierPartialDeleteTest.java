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
package org.apache.cassandra.db.lifecycle;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.MockSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that when the {@link LogTransaction.SSTableTidier} fails partway through deleting an sstable,
 * the transaction log is kept and still matches the files left on disk, so both the tidier's retry
 * and the startup cleanup finish the deletion.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "Arm a delete failure after the given number of successful deletes",
            targetClass = "org.apache.cassandra.db.lifecycle.SSTableTidierPartialDeleteTest",
            targetMethod = "runTidier",
            targetLocation = "AT ENTRY",
            action = "createCountDown(\"deletes\", $1);"),
    @BMRule(name = "Fail the delete once the countdown runs out",
            targetClass = "org.apache.cassandra.io.util.FileUtils",
            targetMethod = "deleteWithConfirm(java.lang.String)",
            targetLocation = "AT ENTRY",
            condition = "countDown(\"deletes\")",
            action = "throw new org.apache.cassandra.io.FSWriteError(new java.io.IOException(\"simulated delete failure\"), $1);")
})
public class SSTableTidierPartialDeleteTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        MockSchema.cleanup();
    }

    @Test
    public void testPartialDeleteIsFinishedByRetryOrStartupCleanup()
    {
        // fail after each number of successful deletes that leaves files behind (there are 4 components in MockSchema)
        for (int deletes = 1; deletes < 4; deletes++)
        {
            assertPartialDeleteFinished(deletes, false);
            assertPartialDeleteFinished(deletes, true);
        }
    }

    private static void assertPartialDeleteFinished(int deletes, boolean restart)
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        SSTableReader sstableOld = MockSchema.sstable(0, true, cfs);
        SSTableReader sstableNew = MockSchema.sstable(1, true, cfs);
        List<File> files = sstableOld.getAllFilePaths().stream().map(File::new).collect(Collectors.toList());

        // make DATA the newest file, so deleting it early would cause the REMOVE record to have a different timestamp
        File data = new File(sstableOld.descriptor.filenameFor(Component.DATA));
        long newest = files.stream().mapToLong(File::lastModified).max().getAsLong();
        assertTrue(data.trySetLastModified(newest + 1000));

        LogTransaction log = new LogTransaction(OperationType.COMPACTION);
        log.trackNew(sstableNew);
        LogTransaction.SSTableTidier tidier = log.obsoleted(sstableOld);
        List<File> logFiles = log.txnFile().getFiles();
        log.finish();

        runTidier(deletes, tidier, sstableOld);

        // the tidier failed partway, but DATA is deleted last so the txn log still matches the files on disk
        assertEquals(files.size() - deletes, files.stream().filter(File::exists).count());
        assertTrue("DATA should be deleted last", data.exists());
        for (File logFile : logFiles)
            assertTrue("txn log invalid after " + deletes + " deletes", LogFile.make(logFile).verify());

        if (restart)
        {
            // the startup txn log replay finishes the deletion, and the old sstable isn't picked up as live
            assertTrue(LogTransaction.removeUnfinishedLeftovers(cfs.metadata()));
            assertEquals(Collections.singleton(sstableNew.descriptor),
                         new Directories(cfs.metadata()).sstableLister(Directories.OnTxnErr.THROW).list().keySet());
        }

        // the tidier's retry finishes the deletion (or finds nothing left after a restart) and releases the txn log
        sstableNew.selfRef().release();
        LogTransaction.rescheduleFailedDeletions();
        LogTransaction.waitForDeletions();
        assertTrue(Stream.concat(files.stream(), logFiles.stream()).noneMatch(File::exists));
    }

    // the BMRules above use deletesBeforeFailure to fail the next delete
    private static void runTidier(int deletesBeforeFailure, LogTransaction.SSTableTidier tidier, SSTableReader sstable)
    {
        sstable.markObsolete(tidier);
        sstable.selfRef().release();
        LogTransaction.waitForDeletions();
    }
}
