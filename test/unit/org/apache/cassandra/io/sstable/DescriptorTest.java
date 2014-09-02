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

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.Directories;
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
        checkFromFilename(new Descriptor(dir, ksname, cfname, 1, Descriptor.Type.FINAL), false);
        // skip component (for streaming lock file)
        checkFromFilename(new Descriptor(dir, ksname, cfname, 2, Descriptor.Type.FINAL), true);
        // tmp
        checkFromFilename(new Descriptor(dir, ksname, cfname, 3, Descriptor.Type.TEMP), false);
        // secondary index
        String idxName = "myidx";
        File idxDir = new File(dir.getAbsolutePath() + File.separator + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName);
        checkFromFilename(new Descriptor(idxDir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, 4, Descriptor.Type.FINAL), false);
        // secondary index tmp
        checkFromFilename(new Descriptor(idxDir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, 5, Descriptor.Type.TEMP), false);

        // legacy version
        checkFromFilename(new Descriptor("ja", dir, ksname, cfname, 1, Descriptor.Type.FINAL, SSTableFormat.Type.LEGACY), false);
        // legacy tmp
        checkFromFilename(new Descriptor("ja", dir, ksname, cfname, 2, Descriptor.Type.TEMP, SSTableFormat.Type.LEGACY), false);
        // legacy secondary index
        checkFromFilename(new Descriptor("ja", dir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, 3, Descriptor.Type.FINAL, SSTableFormat.Type.LEGACY), false);
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
        assertEquals(original.type, desc.type);

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
    public void validateNames()
    {

        String names[] = {
            /*"system-schema_keyspaces-ka-1-CompressionInfo.db",  "system-schema_keyspaces-ka-1-Summary.db",
            "system-schema_keyspaces-ka-1-Data.db",             "system-schema_keyspaces-ka-1-TOC.txt",
            "system-schema_keyspaces-ka-1-Digest.sha1",         "system-schema_keyspaces-ka-2-CompressionInfo.db",
            "system-schema_keyspaces-ka-1-Filter.db",           "system-schema_keyspaces-ka-2-Data.db",
            "system-schema_keyspaces-ka-1-Index.db",            "system-schema_keyspaces-ka-2-Digest.sha1",
            "system-schema_keyspaces-ka-1-Statistics.db",
            "system-schema_keyspacest-tmp-ka-1-Data.db",*/
            "system-schema_keyspace-ka-1-"+ SSTableFormat.Type.BIG.name+"-Data.db"
        };

        for (String name : names)
        {
            Descriptor d = Descriptor.fromFilename(name);
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
