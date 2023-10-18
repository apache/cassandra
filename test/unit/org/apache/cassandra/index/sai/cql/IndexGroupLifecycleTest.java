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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexGroupLifecycleTest extends SAITester
{
    @Test
    public void testDropAndRecreate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, value text, PRIMARY KEY (pk))");
        populateOneSSTable();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Tracker tracker = cfs.getTracker();

        // create index and drop it: StorageAttachedIndexGroup should be removed
        createIndex("CREATE CUSTOM INDEX sai ON %s(value) USING 'StorageAttachedIndex'");

        StorageAttachedIndexGroup group = (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY);
        assertTrue(tracker.contains(group));
        assertEquals(1, group.sstableContextManager().size());

        dropIndex(format("DROP INDEX %s.sai", KEYSPACE));
        assertFalse(tracker.contains(group));
        assertEquals(0, group.sstableContextManager().size()); // sstable should be cleared from old group
        assertNull(cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY));

        // populate 2nd sstable. Old group should not track it
        populateOneSSTable();
        assertEquals(0, group.sstableContextManager().size());

        // create index again: expect a new StorageAttachedIndexGroup to be registered into tracker
        createIndex("CREATE CUSTOM INDEX sai ON %s(value) USING 'StorageAttachedIndex'");

        StorageAttachedIndexGroup newGroup = (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY);
        assertNotSame(group, newGroup);
        assertTrue(tracker.contains(newGroup));
        assertEquals(2, newGroup.sstableContextManager().size());

        // populate 3rd sstable. new group should  track it
        populateOneSSTable();
        assertEquals(3, newGroup.sstableContextManager().size());
    }

    private void populateOneSSTable()
    {
        execute("INSERT INTO %s(pk, value) VALUES('k', 'v')");
        flush();
    }
}