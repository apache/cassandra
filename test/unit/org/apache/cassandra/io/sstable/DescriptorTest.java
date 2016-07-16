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
import org.junit.Test;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
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
        tempDataDir = File.createTempFile("DescriptorTest", null).getParentFile();
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
        // normal
        checkFromFilename(new Descriptor(dir, ksname, cfname, 1, SSTableFormat.Type.BIG), false);
        // skip component (for streaming lock file)
        checkFromFilename(new Descriptor(dir, ksname, cfname, 2, SSTableFormat.Type.BIG), true);

        // secondary index
        String idxName = "myidx";
        File idxDir = new File(dir.getAbsolutePath() + File.separator + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName);
        checkFromFilename(new Descriptor(idxDir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, 4, SSTableFormat.Type.BIG), false);

        // legacy version
        checkFromFilename(new Descriptor("ja", dir, ksname, cfname, 1, SSTableFormat.Type.LEGACY), false);
        // legacy secondary index
        checkFromFilename(new Descriptor("ja", dir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, 3, SSTableFormat.Type.LEGACY), false);
    }

    private void checkFromFilename(Descriptor original, boolean skipComponent)
    {
        File file = new File(skipComponent ? original.baseFilename() : original.filenameFor(Component.DATA));

        Pair<Descriptor, String> pair = Descriptor.fromFilename(file.getParentFile(), file.getName(), skipComponent);
        Descriptor desc = pair.left;

        assertEquals(original.directory, desc.directory);
        assertEquals(original.ksname, desc.ksname);
        assertEquals(original.cfname, desc.cfname);
        assertEquals(original.version, desc.version);
        assertEquals(original.generation, desc.generation);

        if (skipComponent)
        {
            assertNull(pair.right);
        }
        else
        {
            assertEquals(Component.DATA.name(), pair.right);
        }
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
        // TODO tmp file name probably is not handled correctly after CASSANDRA-7066
        String[] names = {
             // old formats
             "system-schema_keyspaces-jb-1-Data.db",
             //"system-schema_keyspaces-tmp-jb-1-Data.db",
             "system-schema_keyspaces-ka-1-big-Data.db",
             //"system-schema_keyspaces-tmp-ka-1-big-Data.db",
             // 2ndary index
             "keyspace1-standard1.idx1-ka-1-big-Data.db",
             // new formats
             "la-1-big-Data.db",
             //"tmp-la-1-big-Data.db",
             // 2ndary index
             ".idx1" + File.separator + "la-1-big-Data.db",
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


}
