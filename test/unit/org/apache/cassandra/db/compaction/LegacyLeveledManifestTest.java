package org.apache.cassandra.db.compaction;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.db.compaction.LegacyLeveledManifestTestHelper.*;

public class LegacyLeveledManifestTest
{
    private final static String LEGACY_VERSION = "ic";

    private File destDir;
    @Before
    public void setup()
    {
        destDir = Directories.create(KS, CF).getDirectoryForNewSSTables();
        FileUtils.createDirectory(destDir);
        for (File srcFile : getLegacySSTableDir(LEGACY_VERSION).listFiles())
        {
            File destFile = new File(destDir, srcFile.getName());
            FileUtils.createHardLink(srcFile,destFile);
            assert destFile.exists() : destFile.getAbsoluteFile();
        }
    }
    @After
    public void tearDown()
    {
        FileUtils.deleteRecursive(destDir);
    }

    @Test
    public void migrateTest() throws IOException
    {
        assertTrue(LegacyLeveledManifest.manifestNeedsMigration(KS, CF));
    }

    @Test
    public void doMigrationTest() throws IOException, InterruptedException
    {
        LegacyLeveledManifest.migrateManifests(KS, CF);

        for (int i = 0; i <= 2; i++)
        {
            Descriptor descriptor = Descriptor.fromFilename(destDir+File.separator+KS+"-"+CF+"-"+LEGACY_VERSION+"-"+i+"-Statistics.db");
            SSTableMetadata metadata = SSTableMetadata.serializer.deserialize(descriptor).left;
            assertEquals(metadata.sstableLevel, i);
        }
    }

    /**
     * Validate that the rewritten stats file is the same as the original one.
     * @throws IOException
     */
    @Test
    public void validateSSTableMetadataTest() throws IOException
    {
        Map<Descriptor, Pair<SSTableMetadata, Set<Integer>>> beforeMigration = new HashMap<>();
        for (int i = 0; i <= 2; i++)
        {
            Descriptor descriptor = Descriptor.fromFilename(destDir+File.separator+KS+"-"+CF+"-"+LEGACY_VERSION+"-"+i+"-Statistics.db");
            beforeMigration.put(descriptor, SSTableMetadata.serializer.deserialize(descriptor, false));
        }

        LegacyLeveledManifest.migrateManifests(KS, CF);

        for (Map.Entry<Descriptor, Pair<SSTableMetadata, Set<Integer>>> entry : beforeMigration.entrySet())
        {
            Pair<SSTableMetadata, Set<Integer>> newMetaPair = SSTableMetadata.serializer.deserialize(entry.getKey());
            SSTableMetadata newMetadata = newMetaPair.left;
            SSTableMetadata oldMetadata = entry.getValue().left;
            assertEquals(newMetadata.estimatedRowSize, oldMetadata.estimatedRowSize);
            assertEquals(newMetadata.estimatedColumnCount, oldMetadata.estimatedColumnCount);
            assertEquals(newMetadata.replayPosition, oldMetadata.replayPosition);
            assertEquals(newMetadata.minTimestamp, oldMetadata.minTimestamp);
            assertEquals(newMetadata.maxTimestamp, oldMetadata.maxTimestamp);
            assertEquals(newMetadata.compressionRatio, oldMetadata.compressionRatio, 0.01);
            assertEquals(newMetadata.partitioner, oldMetadata.partitioner);
            assertEquals(newMetadata.estimatedTombstoneDropTime, oldMetadata.estimatedTombstoneDropTime);
            assertEquals(entry.getValue().right, newMetaPair.right);
        }
    }

}
