/**
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
package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.*;

public class DirectoriesTest
{
    private static File tempDataDir;
    private static String KS = "ks";
    private static String[] CFS = new String[] { "cf1", "ks" };

    private static Map<String, List<File>> files = new HashMap<String, List<File>>();

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        tempDataDir = File.createTempFile("cassandra", "unittest");
        tempDataDir.delete(); // hack to create a temp dir
        tempDataDir.mkdir();

        Directories.overrideDataDirectoriesForTest(tempDataDir.getPath());
        // Create two fake data dir for tests, one using CF directories, one that do not.
        createTestFiles();
    }

    @AfterClass
    public static void afterClass() throws IOException
    {
        Directories.resetDataDirectoriesAfterTest();
        FileUtils.deleteRecursive(tempDataDir);
    }

    private static void createTestFiles() throws IOException
    {
        for (String cf : CFS)
        {
            List<File> fs = new ArrayList<File>();
            files.put(cf, fs);
            File dir = cfDir(cf);
            dir.mkdirs();

            createFakeSSTable(dir, cf, 1, false, false, fs);
            createFakeSSTable(dir, cf, 2, true, false, fs);
            createFakeSSTable(dir, cf, 3, false, true, fs);
            // leveled manifest
            new File(dir, cf + LeveledManifest.EXTENSION).createNewFile();

            File backupDir = new File(dir, Directories.BACKUPS_SUBDIR);
            backupDir.mkdir();
            createFakeSSTable(backupDir, cf, 1, false, false, fs);

            File snapshotDir = new File(dir, Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            snapshotDir.mkdirs();
            createFakeSSTable(snapshotDir, cf, 1, false, false, fs);
        }
    }

    private static void createFakeSSTable(File dir, String cf, int gen, boolean temp, boolean compacted, List<File> addTo) throws IOException
    {
        Descriptor desc = new Descriptor(dir, KS, cf, gen, temp);
        for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER })
        {
            File f = new File(desc.filenameFor(c));
            f.createNewFile();
            addTo.add(f);
        }
        if (compacted)
        {
            File f = new File(desc.filenameFor(Component.COMPACTED_MARKER));
            f.createNewFile();
            addTo.add(f);
        }
    }

    private static File cfDir(String cf)
    {
        return new File(tempDataDir, KS + File.separator + cf);
    }

    @Test
    public void testStandardDirs()
    {
        for (String cf : CFS)
        {
            Directories directories = Directories.create(KS, cf);
            assertEquals(cfDir(cf), directories.getDirectoryForNewSSTables(0));

            Descriptor desc = new Descriptor(cfDir(cf), KS, cf, 1, false);
            File snapshotDir = new File(cfDir(cf),  File.separator + Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            assertEquals(snapshotDir, directories.getSnapshotDirectory(desc, "42"));

            File backupsDir = new File(cfDir(cf),  File.separator + Directories.BACKUPS_SUBDIR);
            assertEquals(backupsDir, directories.getBackupsDirectory(desc));
        }
    }

    @Test
    public void testSSTableLister()
    {
        for (String cf : CFS)
        {
            Directories directories = Directories.create(KS, cf);
            Directories.SSTableLister lister;
            Set<File> listed;

            // List all but no snapshot, backup
            lister = directories.sstableLister();
            listed = new HashSet<File>(lister.listFiles());
            for (File f : files.get(cf))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }

            // List all but including backup (but no snapshot)
            lister = directories.sstableLister().includeBackups(true);
            listed = new HashSet<File>(lister.listFiles());
            for (File f : files.get(cf))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }

            // Skip temporary and compacted
            lister = directories.sstableLister().skipTemporary(true).skipCompacted(true);
            listed = new HashSet<File>(lister.listFiles());
            for (File f : files.get(cf))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else if (f.getName().contains("-tmp-"))
                    assert !listed.contains(f) : f + " should not be listed";
                else if (f.getName().endsWith("Compacted") || new File(f.getPath().replaceFirst("-[a-zA-Z]+.db", "-Compacted")).exists())
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }
        }
    }

    @Test
    public void testLeveledManifestPath()
    {
        for (String cf : CFS)
        {
            Directories directories = Directories.create(KS, cf);
            File manifest = new File(cfDir(cf), cf + LeveledManifest.EXTENSION);
            assertEquals(manifest, directories.tryGetLeveledManifest());
        }
    }
}
