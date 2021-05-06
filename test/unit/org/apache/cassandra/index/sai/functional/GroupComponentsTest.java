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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

public class GroupComponentsTest extends SAITester
{
    @Test
    public void getLiveComponentsForEmptyIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = group.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        // 4 per-sstable components and column complete marker
        assertEquals(5, components.size());
    }

    @Test
    public void getLiveComponentsForPopulatedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk, value) VALUES (1, 1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = group.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        // 4 per-sstable components and 4 column components
        assertEquals(8, components.size());
    }

    private Collection<StorageAttachedIndex> getIndexesFromGroup(StorageAttachedIndexGroup group)
    {
        return group.getIndexes().stream().map(index -> (StorageAttachedIndex)index).collect(Collectors.toList());
    }
}
