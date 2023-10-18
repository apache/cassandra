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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImportTest extends CQLTester
{

    @Test
    public void basicImportByMovingTest() throws Throwable
    {
        File backupDir = prepareBasicImporting();
        // copy is false - so importing will be done by moving
        importSSTables(SSTableImporter.Options.options(backupDir.toString()).copyData(false).build(), 10);
        // files were moved
        Assert.assertEquals(0, countFiles(backupDir));
    }

    @Test
    public void basicImportByCopyingTest() throws Throwable
    {
        File backupDir = prepareBasicImporting();
        // copy is true - so importing will be done by copying
        importSSTables(SSTableImporter.Options.options(backupDir.toString()).copyData(true).build(), 10);
        // files are left there as they were just copied
        Assert.assertNotEquals(0, countFiles(backupDir));
    }

    private File prepareBasicImporting() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");


        for (int i = 0; i < 10; i++)
        {
            execute("insert into %s (id, d) values (?, ?)", i, i);
        }
        flush();

        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir = moveToBackupDir(sstables);

        assertEquals(0, execute("select * from %s").size());

        return backupdir;
    }

    private List<String> importSSTables(SSTableImporter.Options options, int expectedRows) throws Throwable {
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        List<String> failedDirectories = importer.importNewSSTables(options);
        assertEquals(expectedRows, execute("select * from %s").size());
        return failedDirectories;
    }

    @Test
    public void basicImportMultiDirTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir = moveToBackupDir(sstables);
        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir2 = moveToBackupDir(sstables);

        assertEquals(0, execute("select * from %s").size());

        SSTableImporter.Options options = SSTableImporter.Options.options(Sets.newHashSet(backupdir.toString(), backupdir2.toString())).build();
        SSTableImporter importer = new SSTableImporter(getCurrentColumnFamilyStore());
        importer.importNewSSTables(options);

        assertEquals(20, execute("select * from %s").size());

    }


    /** @deprecated See CASSANDRA-6719 */
    @Test
    @Deprecated(since = "4.0")
    public void refreshTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
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
        flush();
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
        flush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        for (SSTableReader sstable : sstables)
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 111, null, false);

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
        String tabledir = sst.descriptor.directory.name();
        String ksdir = sst.descriptor.directory.parent().name();
        Path backupdir = createDirectories(temp.toString(), ksdir, tabledir);
        for (SSTableReader sstable : sstables)
        {
            sstable.selfRef().release();
            for (File f : sstable.descriptor.directory.tryList())
            {
                if (f.toString().contains(sstable.descriptor.baseFile().toString()))
                {
                    System.out.println("move " + f.toPath() + " to " + backupdir);
                    File moveFileTo = new File(backupdir, f.name());
                    moveFileTo.deleteOnExit();
                    Files.move(f.toPath(), moveFileTo.toPath());
                }
            }
        }
        PathUtils.deleteRecursiveOnExit(temp);
        return new File(backupdir);
    }

    private Path createDirectories(String base, String ... subdirs)
    {
        File b = new File(base);
        b.tryCreateDirectory();
        System.out.println("mkdir "+b);
        b.deleteOnExit();
        for (String subdir : subdirs)
        {
            b = new File(b, subdir);
            b.tryCreateDirectory();
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
            flush();
        }

        Set<SSTableReader> toMove = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        File dir = moveToBackupDir(toMove);

        Path tmpDir = Files.createTempDirectory("ImportTest");

        Directories dirs = new Directories(getCurrentColumnFamilyStore().metadata(), Lists.newArrayList(new Directories.DataDirectory(new File(tmpDir, "1")),
                                                                                                        new Directories.DataDirectory(new File(tmpDir, "2")),
                                                                                                        new Directories.DataDirectory(new File(tmpDir, "3"))));
        MockCFS mock = new MockCFS(getCurrentColumnFamilyStore(), dirs);
        SSTableImporter importer = new SSTableImporter(mock);

        importer.importNewSSTables(SSTableImporter.Options.options(dir.toString()).build());
        for (SSTableReader sstable : mock.getLiveSSTables())
        {
            File movedDir = sstable.descriptor.directory.toCanonical();
            File correctDir = mock.getDiskBoundaries().getCorrectDiskForSSTable(sstable).location.toCanonical();
            assertTrue(movedDir.toString().startsWith(correctDir.toString()));
        }
        for (SSTableReader sstable : mock.getLiveSSTables())
            sstable.selfRef().release();
    }

    private void testCorruptHelper(boolean verify, boolean copy) throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
        SSTableReader sstableToCorrupt = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i + 10, i);
        flush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();

        File fileToCorrupt = sstableToCorrupt.descriptor.fileFor(Components.STATS);
        try (FileChannel fileChannel = fileToCorrupt.newReadWriteChannel())
        {
            fileChannel.position(0);
            fileChannel.write(ByteBufferUtil.bytes(StringUtils.repeat('z', 2)));
        }

        File backupdir = moveToBackupDir(sstables);

        // now move a correct sstable to another directory to make sure that directory gets properly imported
        for (int i = 100; i < 130; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
        Set<SSTableReader> correctSSTables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();
        File backupdirCorrect = moveToBackupDir(correctSSTables);

        Set<File> beforeImport = Sets.newHashSet(backupdir.tryList());
        // first we moved out 2 sstables, one correct and one corrupt in to a single directory (backupdir)
        // then we moved out 1 sstable, a correct one (in backupdirCorrect).
        // now import should fail import on backupdir, but import the one in backupdirCorrect.
        SSTableImporter.Options options = SSTableImporter.Options.options(Sets.newHashSet(backupdir.toString(), backupdirCorrect.toString())).copyData(copy).verifySSTables(verify).build();
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
        assertEquals("backupdir contained 2 files before import, should still contain 2 after failing to import it", beforeImport, Sets.newHashSet(backupdir.tryList()));
        if (copy)
        {
            assertEquals("backupdirCorrect contained 1 file before import, should contain 1 after import too", 1, countFiles(backupdirCorrect));
        }
        else
        {
            assertEquals("backupdirCorrect contained 1 file before import, should be empty after import", 0, countFiles(backupdirCorrect));
        }
    }

    private int countFiles(File dir)
    {
        int fileCount = 0;

        for (File f : dir.tryList())
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
        testCorruptHelper(true, false);
    }

    @Test
    public void testImportCorruptWithCopying() throws Throwable
    {
        testCorruptHelper(true, true);
    }

    @Test
    public void testImportCorruptWithoutValidation() throws Throwable
    {
        testCorruptHelper(false, false);
    }

    @Test
    public void testImportCorruptWithoutValidationWithCopying() throws Throwable
    {
        testCorruptHelper(false, true);
    }

    @Test
    public void testImportOutOfRange() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 1000; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
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
        flush();
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
        flush();
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
        flush();

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
        flush();
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
        flush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        sstables.forEach(s -> s.selfRef().release());
        // corrupt the sstable which is still in the data directory
        SSTableReader sstableToCorrupt = sstables.iterator().next();
        File fileToCorrupt = sstableToCorrupt.descriptor.fileFor(Components.STATS);
        try (FileChannel fileChannel = fileToCorrupt.newReadWriteChannel())
        {
            fileChannel.position(0);
            fileChannel.write(ByteBufferUtil.bytes(StringUtils.repeat('z', 2)));
        }

        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
        for (int i = 20; i < 30; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();

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
            assertTrue(sstable.descriptor.fileFor(Components.DATA).exists());
        getCurrentColumnFamilyStore().truncateBlocking();
        LifecycleTransaction.waitForDeletions();
        for (File f : sstableToCorrupt.descriptor.directory.tryList()) // clean up the corrupt files which truncate does not handle
            f.tryDelete();

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
        flush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir = moveToBackupDir(sstables);
        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        flush();
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

    @Test
    public void importExoticTableNamesTest() throws Throwable
    {
        for (String table : new String[] { "snapshot", "snapshots", "backup", "backups",
                                           "\"Snapshot\"", "\"Snapshots\"", "\"Backups\"", "\"Backup\""})
        {
            try
            {
                String unquotedTableName = table.replaceAll("\"", "");
                schemaChange(String.format("CREATE TABLE %s.%s (id int primary key, d int)", KEYSPACE, table));
                for (int i = 0; i < 10; i++)
                    execute(String.format("INSERT INTO %s.%s (id, d) values (?, ?)", KEYSPACE, table), i, i);

                ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, unquotedTableName);

                Util.flush(cfs);

                Set<SSTableReader> sstables = cfs.getLiveSSTables();
                cfs.clearUnsafe();

                File backupDir = moveToBackupDir(sstables);

                assertEquals(0, execute(String.format("SELECT * FROM %s.%s", KEYSPACE, table)).size());

                // copy is true - so importing will be done by copying

                SSTableImporter importer = new SSTableImporter(cfs);
                SSTableImporter.Options options = SSTableImporter.Options.options(backupDir.toString()).copyData(true).build();
                List<String> failedDirectories = importer.importNewSSTables(options);
                assertTrue(failedDirectories.isEmpty());
                assertEquals(10, execute(String.format("select * from %s.%s", KEYSPACE, table)).size());

                // files are left there as they were just copied
                Assert.assertNotEquals(0, countFiles(backupDir));
            }
            finally
            {
                execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, table));
            }
        }
    }

    private static class MockCFS extends ColumnFamilyStore
    {
        public MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), Util.newSeqGen(), cfs.metadata, dirs, false, false, true);
        }
    }
}
