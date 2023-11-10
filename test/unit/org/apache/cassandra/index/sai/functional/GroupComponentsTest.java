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

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GroupComponentsTest extends SAITester
{
    @Test
    public void testInvalidateWithoutObsolete()
    {
        createTable("CREATE TABLE %s (pk int primary key, value text)");
        createIndex("CREATE INDEX ON %s(value) USING 'sai'");
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);

        StorageAttachedIndex index = (StorageAttachedIndex) group.getIndexes().iterator().next();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        Set<Component> components = StorageAttachedIndexGroup.getLiveComponents(sstable, getIndexesFromGroup(group));
        assertEquals(Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).size() + 1, components.size());

        // index files are released but not removed
        cfs.invalidate(true, false);
        Assert.assertTrue(index.view().getIndexes().isEmpty());
        for (Component component : components)
            Assert.assertTrue(sstable.descriptor.fileFor(component).exists());
    }

    @Test
    public void getLiveComponentsForEmptyIndex()
    {
        createTable("CREATE TABLE %s (pk int primary key, value text)");
        createIndex("CREATE INDEX ON %s(value) USING 'sai'");
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = StorageAttachedIndexGroup.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        assertEquals(Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).size() + 1, components.size());
    }

    @Test
    public void getLiveComponentsForPopulatedIndex()
    {
        createTable("CREATE TABLE %s (pk int primary key, value text)");

        createIndex("CREATE INDEX ON %s(value) USING 'sai'");
        IndexTermType indexTermType = createIndexTermType(UTF8Type.instance);

        execute("INSERT INTO %s (pk, value) VALUES (1, '1')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = StorageAttachedIndexGroup.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        assertEquals(Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).size() +
                     Version.LATEST.onDiskFormat().perColumnIndexComponents(indexTermType).size(),
                     components.size());
    }

    private Collection<StorageAttachedIndex> getIndexesFromGroup(StorageAttachedIndexGroup group)
    {
        return group.getIndexes().stream().map(index -> (StorageAttachedIndex)index).collect(Collectors.toList());
    }
}
