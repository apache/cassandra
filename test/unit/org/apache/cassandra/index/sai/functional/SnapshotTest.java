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
package org.apache.cassandra.index.sai.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.inject.Injections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SnapshotTest extends SAITester
{
    @Before
    public void injectCounters() throws Throwable
    {
        Injections.inject(perSSTableValidationCounter, perColumnValidationCounter);
    }

    @After
    public void resetCounters()
    {
        resetValidationCount();
    }

    @Test
    public void shouldTakeAndRestoreSnapshots() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        // Insert some initial data and create the index over it
        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0);");
        IndexIdentifier indexIdentifier = createIndexIdentifier(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));
        IndexTermType indexTermType = createIndexTermType(Int32Type.instance);
        waitForTableIndexesQueryable();
        flush();
        verifyIndexFiles(indexTermType, indexIdentifier, 1, 1, 1);
        // Note: This test will fail here if it is run on its own because the per-index validation
        // is run if the node is starting up but validatation isn't done once the node is started
        assertValidationCount(0, 0);
        resetValidationCount();

        // Add some data into a second sstable
        execute("INSERT INTO %s (id1, v1) VALUES ('1', 0);");
        flush();
        verifyIndexFiles(indexTermType, indexIdentifier, 2, 2, 2);
        assertValidationCount(0, 0);

        // Take a snapshot recording the index files last modified date
        String snapshot = "s";
        assertEquals(1, snapshot(snapshot));
        long snapshotLastModified = indexFilesLastModified();

        // File.lastModified result can be truncated one second resolution, which can be lesser than the index build
        // time, so we sleep for that time to guarantee that the modification date any of overridden index file will be
        // different to that of the original file
        Thread.sleep(1000);

        // Add some data into a third sstable, out of the scope of our snapshot
        execute("INSERT INTO %s (id1, v1) VALUES ('2', 0);");
        flush();
        verifyIndexFiles(indexTermType, indexIdentifier, 3, 3, 3);
        assertNumRows(3, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(0, 0);

        // Truncate the table
        truncate(false);
        waitForAssert(() -> verifyNoIndexFiles());
        assertNumRows(0, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(0, 0);

        // Restore the snapshot, only the two first sstables should be restored
        restoreSnapshot(snapshot);
        verifyIndexFiles(indexTermType, indexIdentifier, 2, 2, 2);
        assertEquals(snapshotLastModified, indexFilesLastModified());
        assertNumRows(2, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(2, 2); // newly loaded

        // index components are included after restore
        verifyIndexComponentsIncludedInSSTable();

        // Rebuild the index to verify that the index files are overridden
        rebuildIndexes(indexIdentifier.indexName);
        verifyIndexFiles(indexTermType, indexIdentifier, 2);
        assertNotEquals(snapshotLastModified, indexFilesLastModified());
        assertNumRows(2, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(2, 2); // compaction should not validate

        // index components are included after rebuild
        verifyIndexComponentsIncludedInSSTable();
    }

    @Test
    public void shouldSnapshotAfterIndexBuild() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyNoIndexFiles();

        // Insert some initial data
        execute("INSERT INTO %s (id1, v1) VALUES ('0', 0);");
        flush();

        // Add some data into a second sstable
        execute("INSERT INTO %s (id1, v1) VALUES ('1', 0);");
        flush();

        // index components are not included
        verifyIndexComponentsNotIncludedInSSTable();

        // create index
        IndexIdentifier indexIdentifier = createIndexIdentifier(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));
        IndexTermType indexTermType = createIndexTermType(Int32Type.instance);
        waitForTableIndexesQueryable();
        verifyIndexFiles(indexTermType, indexIdentifier, 2);
        assertValidationCount(0, 0);

        // index components are included after initial build
        verifyIndexComponentsIncludedInSSTable();

        // Take a snapshot recording the index files last modified date
        String snapshot = "s";
        assertEquals(1, snapshot(snapshot));
        long snapshotLastModified = indexFilesLastModified();

        // File.lastModified result can be truncated one second resolution, which can be lesser than the index build
        // time, so we sleep for that time to guarantee that the modification date any of overridden index file will be
        // different to that of the original file
        Thread.sleep(1000);

        // Truncate the table
        truncate(false);
        waitForAssert(() -> verifyNoIndexFiles());
        assertNumRows(0, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(0, 0);

        // Restore the snapshot
        restoreSnapshot(snapshot);
        verifyIndexFiles(indexTermType, indexIdentifier, 2);
        assertEquals(snapshotLastModified, indexFilesLastModified());
        assertNumRows(2, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(2, 2); // newly loaded

        // index components are included after restore snapshot
        verifyIndexComponentsIncludedInSSTable();
    }
}
