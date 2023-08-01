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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.util.File;

public class MultipleDataDirectoryTest extends TestBaseImpl
{
    private static Cluster CLUSTER;
    private static IInvokableInstance NODE;

    @BeforeClass
    public static void before() throws IOException
    {
        CLUSTER = init(Cluster.build().withNodes(1).withDataDirCount(3).start());
        NODE = CLUSTER.get(1);
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': 'LeveledCompactionStrategy', 'enabled': 'false'}"));
        Assert.assertEquals(3, NODE.callsOnInstance(() -> DatabaseDescriptor.getAllDataFileLocations().length).call().intValue());
    }

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void populateData()
    {
        final int rowsPerFile = 500;
        final int files = 5;
        for (int k = 0; k < files; k++)
        {
            for (int i = k * rowsPerFile; i < k * rowsPerFile + rowsPerFile; ++i)
                NODE.executeInternal(withKeyspace("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');"), Integer.toString(i));
            NODE.nodetool("flush");
        }
    }

    @After
    public void cleanupData()
    {
        NODE.runOnInstance(() -> {
            Keyspace.open(KEYSPACE).getColumnFamilyStore("cf").truncateBlocking();
        });
    }

    @Test
    public void testSSTablesAreInCorrectLocation()
    {
        NODE.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            Assert.assertFalse("All SSTables should be in the correct location",
                               cfs.hasMisplacedSSTables());
        });
    }

    @Test
    public void testDetectSSTableMisplaced()
    {
        setupMisplacedSSTables();
        NODE.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            Assert.assertTrue("Some SSTable should be misplaced",
                               cfs.hasMisplacedSSTables());
        });
    }

    @Test
    public void testNodeToolRelocateSSTablesFindNoFilesToMove()
    {
        long logStartLoc = NODE.logs().mark();
        NODE.nodetoolResult("relocatesstables", KEYSPACE, "cf")
            .asserts()
            .success();
        String expectedLog = String.format("No sstables to RELOCATE for %s.%s", KEYSPACE, "cf");
        Assert.assertEquals("relocatesstables should find no sstables to move",
                            1, NODE.logs().grep(logStartLoc, expectedLog).getResult().size());
    }

    @Test
    public void testNodeToolRelocateSSTables()
    {
        setupMisplacedSSTables();
        long logStartLoc = NODE.logs().mark();
        NODE.nodetoolResult("relocatesstables", KEYSPACE, "cf")
            .asserts()
            .success();
        String expectedLog = String.format("Finished Relocate sstables to correct disk for %s.%s successfully", KEYSPACE, "cf");
        Assert.assertEquals("relocatesstables should find sstables to move",
                            1, NODE.logs().grep(logStartLoc, expectedLog).getResult().size());
        NODE.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            Assert.assertFalse("All SSTables should be in the correct location",
                              cfs.hasMisplacedSSTables());
        });
    }

    // by moving all sstables from the first data directory to the second.
    private void setupMisplacedSSTables()
    {
        NODE.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            Assert.assertNotEquals(0, cfs.getLiveSSTables().size());
            Iterator<SSTableReader> sstables = cfs.getLiveSSTables().iterator();
            // finding 2 descriptors that live in different data directory
            Descriptor first = sstables.next().descriptor;
            Descriptor second = null;
            while (sstables.hasNext() && second == null) {
                second = sstables.next().descriptor;
                if (first.directory.equals(second.directory))
                    second = null;
            }
            Assert.assertNotNull("There should be SSTables in multiple data directories", second);
            // getting a new file index in order to move SSTable between directories.
            second = cfs.newSSTableDescriptor(second.directory);
            // now we just move all sstables from first to second
            for (Component component : TOCComponent.loadOrCreate(first))
            {
                File file = first.fileFor(component);
                if (file.exists())
                {
                    try
                    {
                        Files.copy(file.toPath(), second.fileFor(component).toPath());
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Something wrong with copying sstables", e);
                    }
                }
            }
            ColumnFamilyStore.loadNewSSTables(KEYSPACE, "cf");
        });
    }
}
