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
import java.util.concurrent.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DirectoriesTest
{
    private static File tempDataDir;
    private static final String KS = "ks";
    private static final String[] CFS = new String[] { "cf1", "ks" };

    private static final Set<CFMetaData> CFM = new HashSet<>(CFS.length);
    private static Map<String, List<File>> files = new HashMap<String, List<File>>();

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        for (String cf : CFS)
        {
            CFM.add(new CFMetaData(KS, cf, ColumnFamilyType.Standard, null));
        }

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
        for (CFMetaData cfm : CFM)
        {
            List<File> fs = new ArrayList<>();
            files.put(cfm.cfName, fs);
            File dir = cfDir(cfm);
            dir.mkdirs();

            createFakeSSTable(dir, cfm.cfName, 1, false, fs);
            createFakeSSTable(dir, cfm.cfName, 2, true, fs);

            File backupDir = new File(dir, Directories.BACKUPS_SUBDIR);
            backupDir.mkdir();
            createFakeSSTable(backupDir, cfm.cfName, 1, false, fs);

            File snapshotDir = new File(dir, Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            snapshotDir.mkdirs();
            createFakeSSTable(snapshotDir, cfm.cfName, 1, false, fs);
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

    private static File cfDir(CFMetaData metadata)
    {
        String cfId = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(metadata.cfId));
        return new File(tempDataDir, metadata.ksName + File.separator + metadata.cfName + "-" + cfId);
    }

    @Test
    public void testStandardDirs()
    {
        for (CFMetaData cfm : CFM)
        {
            Directories directories = new Directories(cfm);
            assertEquals(cfDir(cfm), directories.getDirectoryForCompactedSSTables());

            Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.cfName, 1, false);
            File snapshotDir = new File(cfDir(cfm),  File.separator + Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            assertEquals(snapshotDir, Directories.getSnapshotDirectory(desc, "42"));

            File backupsDir = new File(cfDir(cfm),  File.separator + Directories.BACKUPS_SUBDIR);
            assertEquals(backupsDir, Directories.getBackupsDirectory(desc));
        }
    }

    @Test
    public void testSSTableLister()
    {
        for (CFMetaData cfm : CFM)
        {
            Directories directories = new Directories(cfm);
            Directories.SSTableLister lister;
            Set<File> listed;

            // List all but no snapshot, backup
            lister = directories.sstableLister();
            listed = new HashSet<>(lister.listFiles());
            for (File f : files.get(cfm.cfName))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }

            // List all but including backup (but no snapshot)
            lister = directories.sstableLister().includeBackups(true);
            listed = new HashSet<>(lister.listFiles());
            for (File f : files.get(cfm.cfName))
            {
                if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR))
                    assert !listed.contains(f) : f + " should not be listed";
                else
                    assert listed.contains(f) : f + " is missing";
            }

            // Skip temporary and compacted
            lister = directories.sstableLister().skipTemporary(true);
            listed = new HashSet<>(lister.listFiles());
            for (File f : files.get(cfm.cfName))
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
    public void testDiskFailurePolicy_best_effort()
    {
        DiskFailurePolicy origPolicy = DatabaseDescriptor.getDiskFailurePolicy();

        List<DataDirectory> directories = Lists.asList(Directories.flushDirectory, Directories.dataDirectories);
        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.best_effort);

            for (DataDirectory dd : directories)
            {
                dd.location.setExecutable(false);
                dd.location.setWritable(false);
            }

            CFMetaData cfm = new CFMetaData(KS, "bad", ColumnFamilyType.Standard, null);
            Directories dir = new Directories(cfm);

            for (File file : dir.getCFDirectories())
            {
                assertTrue(BlacklistedDirectories.isUnwritable(file));
            }
        } 
        finally 
        {
            for (DataDirectory dd : directories)
            {
                dd.location.setExecutable(true);
                dd.location.setWritable(true);
            }
            
            DatabaseDescriptor.setDiskFailurePolicy(origPolicy);
        }
    }

    @Test
    public void testMTSnapshots() throws Exception
    {
        for (final CFMetaData cfm : CFM)
        {
            final Directories directories = new Directories(cfm);
            assertEquals(cfDir(cfm), directories.getDirectoryForCompactedSSTables());
            final String n = Long.toString(System.nanoTime());
            Callable<File> directoryGetter = new Callable<File>() {
                public File call() throws Exception {
                    Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.cfName, 1, false);
                    return Directories.getSnapshotDirectory(desc, n);
                }
            };
            List<Future<File>> invoked = Executors.newFixedThreadPool(2).invokeAll(Arrays.asList(directoryGetter, directoryGetter));
            for(Future<File> fut:invoked) {
                assertTrue(fut.get().exists());
            }
        }
    }
}
