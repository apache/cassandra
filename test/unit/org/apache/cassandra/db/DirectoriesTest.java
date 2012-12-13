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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;

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
    public static void afterClass()
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

            createFakeSSTable(dir, cf, 1, false, fs);
            createFakeSSTable(dir, cf, 2, true, fs);
            // leveled manifest
            new File(dir, cf + LeveledManifest.EXTENSION).createNewFile();

            File backupDir = new File(dir, Directories.BACKUPS_SUBDIR);
            backupDir.mkdir();
            createFakeSSTable(backupDir, cf, 1, false, fs);

            File snapshotDir = new File(dir, Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            snapshotDir.mkdirs();
            createFakeSSTable(snapshotDir, cf, 1, false, fs);
        }
    }

    private static void createFakeSSTable(File dir, String cf, int gen, boolean temp, List<File> addTo) throws IOException
    {
        Descriptor desc = new Descriptor(dir, KS, cf, gen, temp);
        for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER })
        {
            File f = new File(desc.filenameFor(c));
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
            Assert.assertEquals(cfDir(cf), directories.getDirectoryForNewSSTables(0));

            Descriptor desc = new Descriptor(cfDir(cf), KS, cf, 1, false);
            File snapshotDir = new File(cfDir(cf),  File.separator + Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            Assert.assertEquals(snapshotDir, directories.getSnapshotDirectory(desc, "42"));

            File backupsDir = new File(cfDir(cf),  File.separator + Directories.BACKUPS_SUBDIR);
            Assert.assertEquals(backupsDir, directories.getBackupsDirectory(desc));
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
            lister = directories.sstableLister().skipTemporary(true);
            listed = new HashSet<File>(lister.listFiles());
            for (File f : files.get(cf))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else if (f.getName().contains("-tmp-"))
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
            Assert.assertEquals(manifest, directories.tryGetLeveledManifest());
        }
    }

    @Test
    public void testHandleBadFiles() throws IOException
    {
        /* files not matching the pattern should just be ignored, with a log warning */
        Directories directories = Directories.create(KS, "bad");
        File dir = directories.getDirectoryForNewSSTables(1);
        File f = File.createTempFile("bad", "file", dir.getParentFile());
        Directories.migrateSSTables();
        Assert.assertTrue(f.isFile());

        /* real failures should throw an exception with informational message */
        f = File.createTempFile("locked", ".json", dir.getParentFile());
        File targetDir = new File(dir.getParentFile(), f.getName().substring(0, f.getName().length() - ".json".length()));
        targetDir.mkdirs();
        targetDir.setReadOnly();

        try
        {
            Directories.migrateSSTables();
            Assert.assertFalse(true);
        }
        catch (Exception e)
        {
            Assert.assertTrue(e.getMessage().contains(f.getPath()));
        }
    }

    @Test
    public void testDiskFailurePolicy_best_effort() throws IOException
    {
        DiskFailurePolicy origPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        
        try 
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.best_effort);
            
            for (DataDirectory dd : Directories.dataFileLocations)
            {
                dd.location.setExecutable(false);
                dd.location.setWritable(false);
            }
            
            Directories.create(KS, "bad");
            
            for (DataDirectory dd : Directories.dataFileLocations)
            {
                File file = new File(dd.location, new File(KS, "bad").getPath());
                Assert.assertTrue(BlacklistedDirectories.isUnwritable(file));
            }
        } 
        finally 
        {
            for (DataDirectory dd : Directories.dataFileLocations)
            {
                dd.location.setExecutable(true);
                dd.location.setWritable(true);
            }
            
            DatabaseDescriptor.setDiskFailurePolicy(origPolicy);
        }
    }
}
