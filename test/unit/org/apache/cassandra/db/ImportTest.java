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

package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImportTest extends CQLTester
{
    @Test
    public void basicImportTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir = moveToBackupDir(sstables);

        assertEquals(0, execute("select * from %s").size());

        SSTableImporter.Options options = SSTableImporter.Options.options(backupdir.toString()).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        importer.importNewSSTables(options);

        assertEquals(10, execute("select * from %s").size());
    }

    @Test
    public void basicImportMultiDirTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir = moveToBackupDir(sstables);
        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir2 = moveToBackupDir(sstables);

        assertEquals(0, execute("select * from %s").size());

        SSTableImporter.Options options = SSTableImporter.Options.options(Sets.newHashSet(backupdir.toString(), backupdir2.toString())).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        importer.importNewSSTables(options);

        assertEquals(20, execute("select * from %s").size());

    }


    @Test
    @Deprecated
    public void refreshTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        sstables.forEach(s -> s.selfRef().release());
        assertEquals(0, execute("select * from %s").size());
        getCurrentColumnFamilyStore().loadNewSSTables();
        assertEquals(10, execute("select * from %s").size());
    }

    @Test
    public void importResetLevelTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        for (SSTableReader sstable : sstables)
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 8);
        File backupdir = moveToBackupDir(sstables);
        assertEquals(0, execute("select * from %s").size());

        SSTableImporter.Options options = SSTableImporter.Options.options(backupdir.toString()).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        importer.importNewSSTables(options);

        assertEquals(10, execute("select * from %s").size());
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : sstables)
            assertEquals(8, sstable.getSSTableLevel());

        getCurrentColumnFamilyStore().clearUnsafe();
        backupdir = moveToBackupDir(sstables);

        options = SSTableImporter.Options.options(backupdir.toString()).resetLevel(true).build();
        importer.importNewSSTables(options);

        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertEquals(0, sstable.getSSTableLevel());
    }


    @Test
    public void importClearRepairedTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        for (SSTableReader sstable : sstables)
            sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, 111, null);

        File backupdir = moveToBackupDir(sstables);
        assertEquals(0, execute("select * from %s").size());

        SSTableImporter.Options options = SSTableImporter.Options.options(backupdir.toString()).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        importer.importNewSSTables(options);

        assertEquals(10, execute("select * from %s").size());
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : sstables)
            assertTrue(sstable.isRepaired());

        getCurrentColumnFamilyStore().clearUnsafe();
        backupdir = moveToBackupDir(sstables);

        options = SSTableImporter.Options.options(backupdir.toString()).clearRepaired(true).build();
        importer.importNewSSTables(options);
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertFalse(sstable.isRepaired());
    }

    private File moveToBackupDir(Set<SSTableReader> sstables) throws IOException
    {
        Path temp = Files.createTempDirectory("importtest");
        SSTableReader sst = sstables.iterator().next();
        String tabledir = sst.descriptor.directory.getName();
        String ksdir = sst.descriptor.directory.getParentFile().getName();
        Path backupdir = createDirectories(temp.toString(), ksdir, tabledir);
        for (SSTableReader sstable : sstables)
        {
            sstable.selfRef().release();
            for (File f : sstable.descriptor.directory.listFiles())
            {
                if (f.toString().contains(sstable.descriptor.baseFilename()))
                {
                    System.out.println("move " + f.toPath() + " to " + backupdir);
                    File moveFileTo = new File(backupdir.toFile(), f.getName());
                    moveFileTo.deleteOnExit();
                    Files.move(f.toPath(), moveFileTo.toPath());
                }
            }
        }
        return backupdir.toFile();
    }

    private Path createDirectories(String base, String ... subdirs)
    {
        File b = new File(base);
        b.mkdir();
        System.out.println("mkdir "+b);
        b.deleteOnExit();
        for (String subdir : subdirs)
        {
            b = new File(b, subdir);
            b.mkdir();
            System.out.println("mkdir "+b);
            b.deleteOnExit();
        }
        return b.toPath();
    }

    @Test
    public void testGetCorrectDirectory() throws Throwable
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());
        createTable("create table %s (id int primary key, d int)");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        // generate sstables with different first tokens
        for (int i = 0; i < 10; i++)
        {
            execute("insert into %s (id, d) values (?, ?)", i, i);
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }

        Set<SSTableReader> toMove = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        File dir = moveToBackupDir(toMove);

        Directories dirs = new Directories(getCurrentColumnFamilyStore().metadata(), Lists.newArrayList(new Directories.DataDirectory(new File("/tmp/1")),
                                                                                                        new Directories.DataDirectory(new File("/tmp/2")),
                                                                                                        new Directories.DataDirectory(new File("/tmp/3"))));
        MockCFS mock = new MockCFS(getCurrentColumnFamilyStore(), dirs);
        SSTableImporter importer = new SSTableImporter(mock);

        importer.importNewSSTables(SSTableImporter.Options.options(dir.toString()).build());
        for (SSTableReader sstable : mock.getLiveSSTables())
        {
            File movedDir = sstable.descriptor.directory.getCanonicalFile();
            File correctDir = mock.getDiskBoundaries().getCorrectDiskForSSTable(sstable).location.getCanonicalFile();
            assertTrue(movedDir.toString().startsWith(correctDir.toString()));
        }
        for (SSTableReader sstable : mock.getLiveSSTables())
            sstable.selfRef().release();
    }

    private void testCorruptHelper(boolean verify) throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        SSTableReader sstableToCorrupt = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i + 10, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();

        String filenameToCorrupt = sstableToCorrupt.descriptor.filenameFor(Component.STATS);
        try (RandomAccessFile file = new RandomAccessFile(filenameToCorrupt, "rw"))
        {
            file.seek(0);
            file.writeBytes(StringUtils.repeat('z', 2));
        }

        File backupdir = moveToBackupDir(sstables);

        // now move a correct sstable to another directory to make sure that directory gets properly imported
        for (int i = 100; i < 130; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> correctSSTables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();
        File backupdirCorrect = moveToBackupDir(correctSSTables);

        File [] beforeImport = backupdir.listFiles();
        // first we moved out 2 sstables, one correct and one corrupt in to a single directory (backupdir)
        // then we moved out 1 sstable, a correct one (in backupdirCorrect).
        // now import should fail import on backupdir, but import the one in backupdirCorrect.
        SSTableImporter.Options options = SSTableImporter.Options.options(Sets.newHashSet(backupdir.toString(), backupdirCorrect.toString())).verifySSTables(verify).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        List<String> failedDirectories = importer.importNewSSTables(options);
        assertEquals(Collections.singletonList(backupdir.toString()), failedDirectories);
        UntypedResultSet res = execute("SELECT * FROM %s");
        for (UntypedResultSet.Row r : res)
        {
            int pk = r.getInt("id");
            assertTrue("pk = "+pk, pk >= 100 && pk < 130);
        }
        assertEquals("Data dir should contain one file", 1, countFiles(getCurrentColumnFamilyStore().getDirectories().getDirectoryForNewSSTables()));
        assertArrayEquals("backupdir contained 2 files before import, should still contain 2 after failing to import it", beforeImport, backupdir.listFiles());
        assertEquals("backupdirCorrect contained 1 file before import, should be empty after import", 0, countFiles(backupdirCorrect));
    }

    private int countFiles(File dir)
    {
        int fileCount = 0;

        for (File f : dir.listFiles())
        {
            if (f.isFile() && f.toString().contains("-Data.db"))
            {
                fileCount++;
            }
        }
        return fileCount;
    }

    @Test
    public void testImportCorrupt() throws Throwable
    {
        testCorruptHelper(true);
    }

    @Test
    public void testImportCorruptWithoutValidation() throws Throwable
    {
        testCorruptHelper(false);
    }

    @Test
    public void testImportOutOfRange() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 1000; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.2"));
        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.3"));


        File backupdir = moveToBackupDir(sstables);
        try
        {
            SSTableImporter.Options options = SSTableImporter.Options.options(backupdir.toString()).verifySSTables(true).verifyTokens(true).build();
            SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
            List<String> failed = importer.importNewSSTables(options);
            assertEquals(Collections.singletonList(backupdir.toString()), failed);

            // verify that we check the tokens if verifySSTables == false but verifyTokens == true:
            options = SSTableImporter.Options.options(backupdir.toString()).verifySSTables(false).verifyTokens(true).build();
            importer = new SSTableImporter(getCurrentColumnFamilyStore());
            failed = importer.importNewSSTables(options);
            assertEquals(Collections.singletonList(backupdir.toString()), failed);

            // and that we can import with it disabled:
            options = SSTableImporter.Options.options(backupdir.toString()).verifySSTables(true).verifyTokens(false).build();
            importer = new SSTableImporter(getCurrentColumnFamilyStore());
            failed = importer.importNewSSTables(options);
            assertTrue(failed.isEmpty());

        }
        finally
        {
            tmd.clearUnsafe();
        }
    }

    @Test
    public void testImportOutOfRangeExtendedVerify() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 1000; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.2"));
        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.3"));


        File backupdir = moveToBackupDir(sstables);
        try
        {
            SSTableImporter.Options options = SSTableImporter.Options.options(backupdir.toString())
                                                                                     .verifySSTables(true)
                                                                                     .verifyTokens(true)
                                                                                     .extendedVerify(true).build();
            SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
            List<String> failedDirectories = importer.importNewSSTables(options);
            assertEquals(Collections.singletonList(backupdir.toString()), failedDirectories);
        }
        finally
        {
            tmd.clearUnsafe();
        }
    }


    @Test
    public void testImportInvalidateCache() throws Throwable
    {
        createTable("create table %s (id int primary key, d int) WITH caching = { 'keys': 'NONE', 'rows_per_partition': 'ALL' }");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        CacheService.instance.setRowCacheCapacityInMB(1);

        Set<RowCacheKey> keysToInvalidate = new HashSet<>();

        // populate the row cache with keys from the sstable we are about to remove
        for (int i = 0; i < 10; i++)
        {
            execute("SELECT * FROM %s WHERE id = ?", i);
        }
        Iterator<RowCacheKey> it = CacheService.instance.rowCache.keyIterator();
        while (it.hasNext())
        {
            keysToInvalidate.add(it.next());
        }
        SSTableReader sstableToImport = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        getCurrentColumnFamilyStore().clearUnsafe();


        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();

        Set<RowCacheKey> allCachedKeys = new HashSet<>();

        // populate row cache with sstable we are keeping
        for (int i = 10; i < 20; i++)
        {
            execute("SELECT * FROM %s WHERE id = ?", i);
        }
        it = CacheService.instance.rowCache.keyIterator();
        while (it.hasNext())
        {
            allCachedKeys.add(it.next());
        }
        assertEquals(20, CacheService.instance.rowCache.size());
        File backupdir = moveToBackupDir(Collections.singleton(sstableToImport));
        // make sure we don't wipe caches with invalidateCaches = false:
        Set<SSTableReader> beforeFirstImport = getCurrentColumnFamilyStore().getLiveSSTables();

        SSTableImporter.Options options = SSTableImporter.Options.options(backupdir.toString()).verifySSTables(true).verifyTokens(true).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        importer.importNewSSTables(options);
        assertEquals(20, CacheService.instance.rowCache.size());
        Set<SSTableReader> toMove = Sets.difference(getCurrentColumnFamilyStore().getLiveSSTables(), beforeFirstImport);
        getCurrentColumnFamilyStore().clearUnsafe();
        // move away the sstable we just imported again:
        backupdir = moveToBackupDir(toMove);
        beforeFirstImport.forEach(s -> s.selfRef().release());
        options = SSTableImporter.Options.options(backupdir.toString()).verifySSTables(true).verifyTokens(true).invalidateCaches(true).build();
        importer.importNewSSTables(options);
        assertEquals(10, CacheService.instance.rowCache.size());
        it = CacheService.instance.rowCache.keyIterator();
        while (it.hasNext())
        {
            // make sure the keys from the sstable we are importing are invalidated and that the other one is still there
            RowCacheKey rck = it.next();
            assertTrue(allCachedKeys.contains(rck));
            assertFalse(keysToInvalidate.contains(rck));
        }
    }

    @Test
    public void testImportCacheEnabledWithoutSrcDir() throws Throwable
    {
        createTable("create table %s (id int primary key, d int) WITH caching = { 'keys': 'NONE', 'rows_per_partition': 'ALL' }");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        CacheService.instance.setRowCacheCapacityInMB(1);
        getCurrentColumnFamilyStore().clearUnsafe();
        sstables.forEach(s -> s.selfRef().release());
        SSTableImporter.Options options = SSTableImporter.Options.options().invalidateCaches(true).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        importer.importNewSSTables(options);
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    @Test
    public void testRefreshCorrupt() throws Throwable
    {
        createTable("create table %s (id int primary key, d int) WITH caching = { 'keys': 'NONE', 'rows_per_partition': 'ALL' }");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        sstables.forEach(s -> s.selfRef().release());
        // corrupt the sstable which is still in the data directory
        SSTableReader sstableToCorrupt = sstables.iterator().next();
        String filenameToCorrupt = sstableToCorrupt.descriptor.filenameFor(Component.STATS);
        try (RandomAccessFile file = new RandomAccessFile(filenameToCorrupt, "rw"))
        {
            file.seek(0);
            file.writeBytes(StringUtils.repeat('z', 2));
        }

        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        for (int i = 20; i < 30; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();

        Set<SSTableReader> expectedFiles = new HashSet<>(getCurrentColumnFamilyStore().getLiveSSTables());

        SSTableImporter.Options options = SSTableImporter.Options.options().build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        boolean gotException = false;
        try
        {
            importer.importNewSSTables(options);
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
        assertEquals(2, getCurrentColumnFamilyStore().getLiveSSTables().size());
        // for nodetool refresh we leave corrupt sstables in the data directory
        assertEquals(3, countFiles(sstableToCorrupt.descriptor.directory));
        int rowCount = 0;
        for (UntypedResultSet.Row r : execute("SELECT * FROM %s"))
        {
            rowCount++;
            int pk = r.getInt("id");
            assertTrue("pk = "+pk, pk >= 10 && pk < 30);
        }
        assertEquals(20, rowCount);
        assertEquals(expectedFiles, getCurrentColumnFamilyStore().getLiveSSTables());
        for (SSTableReader sstable : expectedFiles)
            assertTrue(new File(sstable.descriptor.filenameFor(Component.DATA)).exists());
        getCurrentColumnFamilyStore().truncateBlocking();
        LifecycleTransaction.waitForDeletions();
        for (File f : sstableToCorrupt.descriptor.directory.listFiles()) // clean up the corrupt files which truncate does not handle
            f.delete();

    }

    /**
     * If a user gives a bad directory we don't import any directories - we should let the user correct the directories
     */
    @Test
    public void importBadDirectoryTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir = moveToBackupDir(sstables);
        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir2 = moveToBackupDir(sstables);

        assertEquals(0, execute("select * from %s").size());

        SSTableImporter.Options options = SSTableImporter.Options.options(Sets.newHashSet(backupdir.toString(), backupdir2.toString(), "/tmp/DOESNTEXIST")).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        boolean gotException = false;
        try
        {
            importer.importNewSSTables(options);
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
        assertEquals(0, execute("select * from %s").size());
        assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    private static class MockCFS extends ColumnFamilyStore
    {
        public MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), 0, cfs.metadata, dirs, false, false, true);
        }
    }
}
