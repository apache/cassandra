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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

public class DescriptorTest
{
    private final String ksname = "ks";
    private final String cfname = "cf";
    private final String cfId = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(UUID.randomUUID()));
    private final File tempDataDir;

    public DescriptorTest() throws IOException
    {
        // create CF directories, one without CFID and one with it
        tempDataDir = FileUtils.createTempFile("DescriptorTest", null).getParentFile();
    }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testFromFilename() throws Exception
    {
        File cfIdDir = new File(tempDataDir.getAbsolutePath() + File.separator + ksname + File.separator + cfname + '-' + cfId);
        testFromFilenameFor(cfIdDir);
    }

    @Test
    public void testFromFilenameInBackup() throws Exception
    {
        File backupDir = new File(StringUtils.join(new String[]{tempDataDir.getAbsolutePath(), ksname, cfname + '-' + cfId, Directories.BACKUPS_SUBDIR}, File.separator));
        testFromFilenameFor(backupDir);
    }

    @Test
    public void testFromFilenameInSnapshot() throws Exception
    {
        File snapshotDir = new File(StringUtils.join(new String[]{tempDataDir.getAbsolutePath(), ksname, cfname + '-' + cfId, Directories.SNAPSHOT_SUBDIR, "snapshot_name"}, File.separator));
        testFromFilenameFor(snapshotDir);
    }

    @Test
    public void testFromFilenameInLegacyDirectory() throws Exception
    {
        File cfDir = new File(tempDataDir.getAbsolutePath() + File.separator + ksname + File.separator + cfname);
        testFromFilenameFor(cfDir);
    }

    private void testFromFilenameFor(File dir)
    {
        checkFromFilename(new Descriptor(dir, ksname, cfname, 1, SSTableFormat.Type.BIG));

        // secondary index
        String idxName = "myidx";
        File idxDir = new File(dir.getAbsolutePath() + File.separator + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName);
        checkFromFilename(new Descriptor(idxDir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, 4, SSTableFormat.Type.BIG));
    }

    private void checkFromFilename(Descriptor original)
    {
        File file = new File(original.filenameFor(Component.DATA));

        Pair<Descriptor, Component> pair = Descriptor.fromFilenameWithComponent(file);
        Descriptor desc = pair.left;

        assertEquals(original.directory, desc.directory);
        assertEquals(original.ksname, desc.ksname);
        assertEquals(original.cfname, desc.cfname);
        assertEquals(original.version, desc.version);
        assertEquals(original.generation, desc.generation);
        assertEquals(Component.DATA, pair.right);
    }

    @Test
    public void testEquality()
    {
        // Descriptor should be equal when parent directory points to the same directory
        File dir = new File(".");
        Descriptor desc1 = new Descriptor(dir, "ks", "cf", 1, SSTableFormat.Type.BIG);
        Descriptor desc2 = new Descriptor(dir.getAbsoluteFile(), "ks", "cf", 1, SSTableFormat.Type.BIG);
        assertEquals(desc1, desc2);
        assertEquals(desc1.hashCode(), desc2.hashCode());
    }

    @Test
    public void validateNames()
    {
        String[] names = {
             "ma-1-big-Data.db",
             // 2ndary index
             ".idx1" + File.separator + "ma-1-big-Data.db",
        };

        for (String name : names)
        {
            assertNotNull(Descriptor.fromFilename(name));
        }
    }

    @Test
    public void badNames()
    {
        String names[] = {
                "system-schema_keyspaces-k234a-1-CompressionInfo.db",  "system-schema_keyspaces-ka-aa-Summary.db",
                "system-schema_keyspaces-XXX-ka-1-Data.db",             "system-schema_keyspaces-k",
                "system-schema_keyspace-ka-1-AAA-Data.db",  "system-schema-keyspace-ka-1-AAA-Data.db"
        };

        for (String name : names)
        {
            try
            {
                Descriptor d = Descriptor.fromFilename(name);
                Assert.fail(name);
            } catch (Throwable e) {
                //good
            }
        }
    }

    @Test
    public void testKeyspaceTableParsing()
    {
        String[] locations = new String[]{
        "/path/to/cassandra/data/dir1",
        "/path/to/cassandra/data/dir2/dir5",
        "/path/to/cassandra/data/dir2/dir5/dir6",
        "/path/to/cassandra/data/dir3",
        "/path/to/cassandra/data/dir3/dir4",
        "/path/to/cassandra/data/dir2/dir6",
        };

        // from Cassandra dirs

        String[] filePaths = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1/snapshots/snapshot/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1/backups/na-1-big-Index.db",
        };

        testKeyspaceTableParsing(filePaths, locations, "ks1", "tab1");

        // indexes

        String[] filePathsIndexes = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1/snapshots/snapshot/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1/backups/.index/na-1-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsIndexes, locations, "ks1", "tab1.index");

        // what if even a snapshot of a keyspace and table called snapshots is called snapshots?

        String[] filePathsWithSnapshotKeyspaceAndTable = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots/snapshots/snapshots/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots/backups/na-1-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithSnapshotKeyspaceAndTable, locations, "snapshots", "snapshots");


        String[] filePathsWithSnapshotKeyspaceAndTableWithIndices = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots/snapshots/snapshots/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots/backups/.index/na-1-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithSnapshotKeyspaceAndTableWithIndices, locations, "snapshots", "snapshots.index");

        // what if keyspace and table is called backups?

        String[] filePathsWithBackupsKeyspaceAndTable = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/snapshots/snapshots/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/backups/na-1-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithBackupsKeyspaceAndTable, locations, "backups", "backups");


        String[] filePathsWithBackupsKeyspaceAndTableWithIndices = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/snapshots/snapshots/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/backups/.index/na-1-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithBackupsKeyspaceAndTableWithIndices, locations, "backups", "backups.index");

        String[] outsideOfCassandra = new String[]{
        "/tmp/some/path/tests/keyspace/table/na-1-big-Index.db",
        "/tmp/some/path/tests/keyspace/table/snapshots/snapshots/na-1-big-Index.db",
        "/tmp/some/path/tests/keyspace/table/backups/na-1-big-Index.db",
        "/tmp/tests/keyspace/table/na-1-big-Index.db",
        "/keyspace/table/na-1-big-Index.db"
        };

        testKeyspaceTableParsing(outsideOfCassandra, locations, "keyspace", "table");

        String[] outsideOfCassandraIndexes = new String[]{
        "/tmp/some/path/tests/keyspace/table/.index/na-1-big-Index.db",
        "/tmp/some/path/tests/keyspace/table/snapshots/snapshots/.index/na-1-big-Index.db",
        "/tmp/some/path/tests/keyspace/table/backups/.index/na-1-big-Index.db"
        };

        testKeyspaceTableParsing(outsideOfCassandraIndexes, locations, "keyspace", "table.index");
    }

    private void testKeyspaceTableParsing(String[] filePaths, String[] locations, String expectedKeyspace, String expectedTable)
    {
        for (String filePath : filePaths)
        {
            Pair<String, String> ksTab = Descriptor.parseKeyspaceAndTable(locations, new File(filePath));
            Assert.assertNotNull(ksTab);
            Assert.assertEquals(expectedKeyspace, ksTab.left);
            Assert.assertEquals(expectedTable, ksTab.right);
        }
    }
}
