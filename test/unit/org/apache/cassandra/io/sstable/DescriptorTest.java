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

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.io.util.File;
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
        tempDataDir = FileUtils.createTempFile("DescriptorTest", null).parent();
    }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testFromFilename() throws Exception
    {
        File cfIdDir = new File(tempDataDir.absolutePath() + File.pathSeparator() + ksname + File.pathSeparator() + cfname + '-' + cfId);
        testFromFilenameFor(cfIdDir);
    }

    @Test
    public void testFromFilenameInBackup() throws Exception
    {
        File backupDir = new File(StringUtils.join(new String[]{ tempDataDir.absolutePath(), ksname, cfname + '-' + cfId, Directories.BACKUPS_SUBDIR}, File.pathSeparator()));
        testFromFilenameFor(backupDir);
    }

    @Test
    public void testFromFilenameInSnapshot() throws Exception
    {
        File snapshotDir = new File(StringUtils.join(new String[]{ tempDataDir.absolutePath(), ksname, cfname + '-' + cfId, Directories.SNAPSHOT_SUBDIR, "snapshot_name"}, File.pathSeparator()));
        testFromFilenameFor(snapshotDir);
    }

    @Test
    public void testFromFilenameInLegacyDirectory() throws Exception
    {
        File cfDir = new File(tempDataDir.absolutePath() + File.pathSeparator() + ksname + File.pathSeparator() + cfname);
        testFromFilenameFor(cfDir);
    }

    private void testFromFilenameFor(File dir)
    {
        checkFromFilename(new Descriptor(dir, ksname, cfname, new SequenceBasedSSTableId(1), SSTableFormat.Type.BIG));

        // secondary index
        String idxName = "myidx";
        File idxDir = new File(dir.absolutePath() + File.pathSeparator() + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName);
        checkFromFilename(new Descriptor(idxDir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, new SequenceBasedSSTableId(4), SSTableFormat.Type.BIG));
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
        assertEquals(original.id, desc.id);
        assertEquals(Component.DATA, pair.right);
    }

    @Test
    public void testEquality()
    {
        // Descriptor should be equal when parent directory points to the same directory
        File dir = new File(".");
        Descriptor desc1 = new Descriptor(dir, "ks", "cf", new SequenceBasedSSTableId(1), SSTableFormat.Type.BIG);
        Descriptor desc2 = new Descriptor(dir.toAbsolute(), "ks", "cf", new SequenceBasedSSTableId(1), SSTableFormat.Type.BIG);
        assertEquals(desc1, desc2);
        assertEquals(desc1.hashCode(), desc2.hashCode());
    }

    @Test
    public void validateNames()
    {
        String[] names = {
             "ma-1-big-Data.db",
             // 2ndary index
             ".idx1" + File.pathSeparator() + "ma-1-big-Data.db",
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
        // from Cassandra dirs

        String[] filePaths = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/snapshots/snapshot/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/backups/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/snapshots/snapshot/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/backups/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        };

        testKeyspaceTableParsing(filePaths, "ks1", "tab1");

        // indexes

        String[] filePathsIndexes = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/snapshots/snapshot/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/backups/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/snapshots/snapshot/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/ks1/tab1-3424234234324/backups/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsIndexes, "ks1", "tab1.index");

        // what if even a snapshot of a keyspace and table called snapshots is called snapshots?

        String[] filePathsWithSnapshotKeyspaceAndTable = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/snapshots/snapshots/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/backups/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/snapshots/snapshots/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/backups/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithSnapshotKeyspaceAndTable, "snapshots", "snapshots");

        String[] filePathsWithSnapshotKeyspaceAndTableWithIndices = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/snapshots/snapshots/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/backups/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/snapshots/snapshots/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/snapshots/snapshots-742738427389478/backups/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithSnapshotKeyspaceAndTableWithIndices, "snapshots", "snapshots.index");

        // what if keyspace and table is called backups?

        String[] filePathsWithBackupsKeyspaceAndTable = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/snapshots/snapshots/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/backups/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/snapshots/snapshots/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/backups/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithBackupsKeyspaceAndTable, "backups", "backups");

        // what if legacy keyspace and table is called backups?

        String[] legacyFilePathsWithBackupsKeyspaceAndTable = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/na-1-big-Index.db",
        //"/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/snapshots/snapshots/na-1-big-Index.db", #not supported (CASSANDRA-14013)
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/backups/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/nb-22-big-Index.db",
        //"/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/snapshots/snapshots/nb-22-big-Index.db", #not supported (CASSANDRA-14013)
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups/backups/nb-22-big-Index.db",
        };

        testKeyspaceTableParsing(legacyFilePathsWithBackupsKeyspaceAndTable, "backups", "backups");

        // what if even a snapshot of a keyspace and table called backups is called snapshots?

        String[] filePathsWithBackupsKeyspaceAndTableWithIndices = new String[]{
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/snapshots/snapshots/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/backups/.index/na-1-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/snapshots/snapshots/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/path/to/cassandra/data/dir2/dir5/dir6/backups/backups-742738427389478/backups/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        };

        testKeyspaceTableParsing(filePathsWithBackupsKeyspaceAndTableWithIndices, "backups", "backups.index");

        String[] outsideOfCassandra = new String[]{
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/na-1-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/snapshots/snapshots/na-1-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/backups/na-1-big-Index.db",
        "/testroot/tests/keyspace/table-34234234234234234234234234234234/na-1-big-Index.db",
        "/keyspace/table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/snapshots/snapshots/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/backups/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/tests/keyspace/table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/keyspace/table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db"
        };

        testKeyspaceTableParsing(outsideOfCassandra, "keyspace", "table");

        String[] outsideOfCassandraUppercaseKeyspaceAndTable = new String[]{
        "/testroot/some/path/tests/Keyspace/Table-34234234234234234234234234234234/na-1-big-Index.db",
        "/testroot/some/path/tests/Keyspace/Table-34234234234234234234234234234234/snapshots/snapshots/na-1-big-Index.db",
        "/testroot/some/path/tests/Keyspace/Table-34234234234234234234234234234234/backups/na-1-big-Index.db",
        "/testroot/tests/Keyspace/Table-34234234234234234234234234234234/na-1-big-Index.db",
        "/Keyspace/Table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/Keyspace/Table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/Keyspace/Table-34234234234234234234234234234234/snapshots/snapshots/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/Keyspace/Table-34234234234234234234234234234234/backups/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/tests/Keyspace/Table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/Keyspace/Table-34234234234234234234234234234234/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db"
        };

        testKeyspaceTableParsing(outsideOfCassandraUppercaseKeyspaceAndTable, "Keyspace", "Table");

        String[] outsideOfCassandraIndexes = new String[]{
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/.index/na-1-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/snapshots/snapshots/.index/na-1-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/backups/.index/na-1-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/snapshots/snapshots/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db",
        "/testroot/some/path/tests/keyspace/table-34234234234234234234234234234234/backups/.index/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db"
        };

        testKeyspaceTableParsing(outsideOfCassandraIndexes, "keyspace", "table.index");

        String[] counterFiles = new String[] {
        "/path/to/cassandra/data/dir2/dir6_other/Keyspace1/counter1-246467e01ea111ebbeafc3f73b4a4f2e/na-3-big-CRC.db",
        "/path/to/cassandra/data/dir2/dir6_other/Keyspace1/counter1-246467e01ea111ebbeafc3f73b4a4f2e/nb-3g1m_0nuf_3vj5m2k1125165rxa7-big-Index.db"
        };

        testKeyspaceTableParsing(counterFiles, "Keyspace1", "counter1");
    }

    private void testKeyspaceTableParsing(String[] filePaths, String expectedKeyspace, String expectedTable)
    {
        for (String filePath : filePaths)
        {
            Descriptor descriptor = Descriptor.fromFilename(filePath);
            Assert.assertNotNull(descriptor);
            Assert.assertEquals(expectedKeyspace, descriptor.ksname);
            Assert.assertEquals(expectedTable, descriptor.cfname);
        }
    }
}
