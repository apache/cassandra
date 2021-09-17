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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Directories.DataDirectories;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.ULIDBasedSSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class DirectoriesTest
{
    private static File tempDataDir;
    private static final String KS = "ks";
    private static String[] TABLES;
    private static Set<TableMetadata> CFM;
    private static Map<String, List<File>> files;

    private ConcurrentMap<Integer, SSTableUniqueIdentifier> ids = new ConcurrentHashMap<>();

    @Parameterized.Parameter(0)
    public SSTableUniqueIdentifier.Builder<? extends SSTableUniqueIdentifier> idBuilder;

    @Parameterized.Parameter(1)
    public Supplier<? extends SSTableUniqueIdentifier> idGenerator;

    @Parameterized.Parameters
    public static Collection<Object[]> idBuilders()
    {
        return Arrays.asList(new Object[]{ SequenceBasedSSTableUniqueIdentifier.Builder.instance, SequenceBasedSSTableUniqueIdentifier.Builder.instance.generator(Stream.empty()) },
                             new Object[]{ ULIDBasedSSTableUniqueIdentifier.Builder.instance, ULIDBasedSSTableUniqueIdentifier.Builder.instance.generator(Stream.empty()) });
    }

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
    }

    public SSTableUniqueIdentifier getId(int idx)
    {
        return ids.computeIfAbsent(idx, ignored -> idGenerator.get());
    }

    @Before
    public void beforeTest() throws IOException
    {
        TABLES = new String[] { "cf1", "ks" };
        CFM = new HashSet<>(TABLES.length);
        files = new HashMap<>();

        for (String table : TABLES)
        {
            CFM.add(TableMetadata.builder(KS, table)
                                 .addPartitionKeyColumn("thekey", UTF8Type.instance)
                                 .addClusteringColumn("thecolumn", UTF8Type.instance)
                                 .build());
        }

        tempDataDir = FileUtils.createTempFile("cassandra", "unittest");
        tempDataDir.delete(); // hack to create a temp dir
        tempDataDir.mkdir();

        // Create two fake data dir for tests, one using CF directories, one that do not.
        createTestFiles();
    }

    @AfterClass
    public static void afterClass()
    {
        FileUtils.deleteRecursive(tempDataDir);
    }

    private static DataDirectory[] toDataDirectories(File location)
    {
        return new DataDirectory[] { new DataDirectory(location) };
    }

    private void createTestFiles() throws IOException
    {
        for (TableMetadata cfm : CFM)
        {
            List<File> fs = new ArrayList<>();
            files.put(cfm.name, fs);
            File dir = cfDir(cfm);
            dir.mkdirs();

            createFakeSSTable(dir, cfm.name, 1, fs);
            createFakeSSTable(dir, cfm.name, 2, fs);

            File backupDir = new File(dir, Directories.BACKUPS_SUBDIR);
            backupDir.mkdir();
            createFakeSSTable(backupDir, cfm.name, 1, fs);

            File snapshotDir = new File(dir, Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            snapshotDir.mkdirs();
            createFakeSSTable(snapshotDir, cfm.name, 1, fs);
        }
    }

    private void createFakeSSTable(File dir, String cf, int gen, List<File> addTo) throws IOException
    {
        Descriptor desc = new Descriptor(dir, KS, cf, getId(gen), SSTableFormat.Type.BIG);
        for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER })
        {
            File f = new File(desc.filenameFor(c));
            f.createNewFile();
            addTo.add(f);
        }
    }

    private static File cfDir(TableMetadata metadata)
    {
        String tableId = metadata.id.toHexString();
        int idx = metadata.name.indexOf(Directories.SECONDARY_INDEX_NAME_SEPARATOR);
        if (idx >= 0)
        {
            // secondary index
            return new File(tempDataDir,
                            metadata.keyspace + File.separator +
                            metadata.name.substring(0, idx) + '-' + tableId + File.separator +
                            metadata.name.substring(idx));
        }
        else
        {
            return new File(tempDataDir, metadata.keyspace + File.separator + metadata.name + '-' + tableId);
        }
    }

    @Test
    public void testStandardDirs() throws IOException
    {
        for (TableMetadata cfm : CFM)
        {
            Directories directories = new Directories(cfm, toDataDirectories(tempDataDir));
            assertEquals(cfDir(cfm), directories.getDirectoryForNewSSTables());

            Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.name, getId(1), SSTableFormat.Type.BIG);
            File snapshotDir = new File(cfDir(cfm),  File.separator + Directories.SNAPSHOT_SUBDIR + File.separator + "42");
            assertEquals(snapshotDir.getCanonicalFile(), Directories.getSnapshotDirectory(desc, "42"));

            File backupsDir = new File(cfDir(cfm),  File.separator + Directories.BACKUPS_SUBDIR);
            assertEquals(backupsDir.getCanonicalFile(), Directories.getBackupsDirectory(desc));

            Supplier<? extends SSTableUniqueIdentifier> uidGen = directories.getUIDGenerator(idBuilder);
            assertThat(Stream.generate(uidGen).limit(100).filter(generated -> ids.containsValue(generated)).collect(Collectors.toList())).isEmpty();
        }
    }

    @Test
    public void testSecondaryIndexDirectories()
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(KS, "cf")
                         .addPartitionKeyColumn("thekey", UTF8Type.instance)
                         .addClusteringColumn("col", UTF8Type.instance);

        ColumnIdentifier col = ColumnIdentifier.getInterned("col", true);
        IndexMetadata indexDef =
            IndexMetadata.fromIndexTargets(
            Collections.singletonList(new IndexTarget(col, IndexTarget.Type.VALUES)),
                                           "idx",
                                           IndexMetadata.Kind.KEYS,
                                           Collections.emptyMap());
        builder.indexes(Indexes.of(indexDef));

        TableMetadata PARENT_CFM = builder.build();
        TableMetadata INDEX_CFM = CassandraIndex.indexCfsMetadata(PARENT_CFM, indexDef);
        Directories parentDirectories = new Directories(PARENT_CFM, toDataDirectories(tempDataDir));
        Directories indexDirectories = new Directories(INDEX_CFM, toDataDirectories(tempDataDir));
        // secondary index has its own directory
        for (File dir : indexDirectories.getCFDirectories())
        {
            assertEquals(cfDir(INDEX_CFM), dir);
        }
        Descriptor parentDesc = new Descriptor(parentDirectories.getDirectoryForNewSSTables(), KS, PARENT_CFM.name, getId(0), SSTableFormat.Type.BIG);
        Descriptor indexDesc = new Descriptor(indexDirectories.getDirectoryForNewSSTables(), KS, INDEX_CFM.name, getId(0), SSTableFormat.Type.BIG);

        // snapshot dir should be created under its parent's
        File parentSnapshotDirectory = Directories.getSnapshotDirectory(parentDesc, "test");
        File indexSnapshotDirectory = Directories.getSnapshotDirectory(indexDesc, "test");
        assertEquals(parentSnapshotDirectory, indexSnapshotDirectory.getParentFile());

        // check if snapshot directory exists
        assertTrue(parentDirectories.snapshotExists("test"));
        assertTrue(indexDirectories.snapshotExists("test"));

        // check true snapshot size
        Descriptor parentSnapshot = new Descriptor(parentSnapshotDirectory, KS, PARENT_CFM.name, getId(0), SSTableFormat.Type.BIG);
        createFile(parentSnapshot.filenameFor(Component.DATA), 30);
        Descriptor indexSnapshot = new Descriptor(indexSnapshotDirectory, KS, INDEX_CFM.name, getId(0), SSTableFormat.Type.BIG);
        createFile(indexSnapshot.filenameFor(Component.DATA), 40);

        assertEquals(30, parentDirectories.trueSnapshotsSize());
        assertEquals(40, indexDirectories.trueSnapshotsSize());

        // check snapshot details
        Map<String, Directories.SnapshotSizeDetails> parentSnapshotDetail = parentDirectories.getSnapshotDetails();
        assertTrue(parentSnapshotDetail.containsKey("test"));
        assertEquals(30L, parentSnapshotDetail.get("test").dataSizeBytes);

        Map<String, Directories.SnapshotSizeDetails> indexSnapshotDetail = indexDirectories.getSnapshotDetails();
        assertTrue(indexSnapshotDetail.containsKey("test"));
        assertEquals(40L, indexSnapshotDetail.get("test").dataSizeBytes);

        // check backup directory
        File parentBackupDirectory = Directories.getBackupsDirectory(parentDesc);
        File indexBackupDirectory = Directories.getBackupsDirectory(indexDesc);
        assertEquals(parentBackupDirectory, indexBackupDirectory.getParentFile());
    }

    private File createFile(String fileName, int size)
    {
        File newFile = new File(fileName);
        try (FileOutputStream writer = new FileOutputStream(newFile))
        {
            writer.write(new byte[size]);
            writer.flush();
        }
        catch (IOException ignore) {}
        return newFile;
    }

    @Test
    public void testSSTableLister()
    {
        for (TableMetadata cfm : CFM)
        {
            Directories directories = new Directories(cfm, toDataDirectories(tempDataDir));
            checkFiles(cfm, directories);
        }
    }

    private void checkFiles(TableMetadata cfm, Directories directories)
    {
        Directories.SSTableLister lister;
        Set<File> listed;// List all but no snapshot, backup
        lister = directories.sstableLister(Directories.OnTxnErr.THROW);
        listed = new HashSet<>(lister.listFiles());
        for (File f : files.get(cfm.name))
        {
            if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else
                assertTrue(f + " is missing", listed.contains(f));
        }

        // List all but including backup (but no snapshot)
        lister = directories.sstableLister(Directories.OnTxnErr.THROW).includeBackups(true);
        listed = new HashSet<>(lister.listFiles());
        for (File f : files.get(cfm.name))
        {
            if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else
                assertTrue(f + " is missing", listed.contains(f));
        }

        // Skip temporary and compacted
        lister = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
        listed = new HashSet<>(lister.listFiles());
        for (File f : files.get(cfm.name))
        {
            if (f.getPath().contains(Directories.SNAPSHOT_SUBDIR) || f.getPath().contains(Directories.BACKUPS_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else if (f.getName().contains("tmp-"))
                assertFalse(f + " should not be listed", listed.contains(f));
            else
                assertTrue(f + " is missing", listed.contains(f));
        }
    }

    @Test
    public void testTemporaryFile() throws IOException
    {
        for (TableMetadata cfm : CFM)
        {
            Directories directories = new Directories(cfm, toDataDirectories(tempDataDir));

            File tempDir = directories.getTemporaryWriteableDirectoryAsFile(10);
            tempDir.mkdir();
            File tempFile = new File(tempDir, "tempFile");
            tempFile.createNewFile();

            assertTrue(tempDir.exists());
            assertTrue(tempFile.exists());

            //make sure temp dir/file will not affect existing sstable listing
            checkFiles(cfm, directories);

            directories.removeTemporaryDirectories();

            //make sure temp dir/file deletion will not affect existing sstable listing
            checkFiles(cfm, directories);

            assertFalse(tempDir.exists());
            assertFalse(tempFile.exists());
        }
    }

    @Test
    public void testDiskFailurePolicy_best_effort()
    {
        DiskFailurePolicy origPolicy = DatabaseDescriptor.getDiskFailurePolicy();

        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.best_effort);

            Set<DataDirectory> directories = Directories.dataDirectories.getAllDirectories();
            DataDirectory first = directories.iterator().next();

            // Fake a Directory creation failure
            if (!directories.isEmpty())
            {
                String[] path = new String[] {KS, "bad"};
                File dir = new File(first.location, StringUtils.join(path, File.separator));
                JVMStabilityInspector.inspectThrowable(new FSWriteError(new IOException("Unable to create directory " + dir), dir));
            }

            File file = new File(first.location, new File(KS, "bad").getPath());
            assertTrue(DisallowedDirectories.isUnwritable(file));

        } 
        finally 
        {
            DatabaseDescriptor.setDiskFailurePolicy(origPolicy);
        }
    }

    @Test
    public void testMTSnapshots() throws Exception
    {
        for (final TableMetadata cfm : CFM)
        {
            final Directories directories = new Directories(cfm, toDataDirectories(tempDataDir));
            assertEquals(cfDir(cfm), directories.getDirectoryForNewSSTables());
            final String n = Long.toString(System.nanoTime());
            Callable<File> directoryGetter = new Callable<File>() {
                public File call() throws Exception {
                    Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.name, getId(1), SSTableFormat.Type.BIG);
                    return Directories.getSnapshotDirectory(desc, n);
                }
            };
            List<Future<File>> invoked = Executors.newFixedThreadPool(2).invokeAll(Arrays.asList(directoryGetter, directoryGetter));
            for(Future<File> fut:invoked) {
                assertTrue(fut.get().exists());
            }
        }
    }

    @Test
    public void testDiskFreeSpace()
    {
        DataDirectory[] dataDirectories = new DataDirectory[]
                                          {
                                          new DataDirectory(new File("/nearlyFullDir1"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 11L;
                                              }
                                          },
                                          new DataDirectory(new File("/nearlyFullDir2"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 10L;
                                              }
                                          },
                                          new DataDirectory(new File("/uniformDir1"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 1000L;
                                              }
                                          },
                                          new DataDirectory(new File("/uniformDir2"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 999L;
                                              }
                                          },
                                          new DataDirectory(new File("/veryFullDir"))
                                          {
                                              public long getAvailableSpace()
                                              {
                                                  return 4L;
                                              }
                                          }
                                          };

        // directories should be sorted
        // 1. by their free space ratio
        // before weighted random is applied
        List<Directories.DataDirectoryCandidate> candidates = getWriteableDirectories(dataDirectories, 0L);
        assertSame(dataDirectories[2], candidates.get(0).dataDirectory); // available: 1000
        assertSame(dataDirectories[3], candidates.get(1).dataDirectory); // available: 999
        assertSame(dataDirectories[0], candidates.get(2).dataDirectory); // available: 11
        assertSame(dataDirectories[1], candidates.get(3).dataDirectory); // available: 10

        // check for writeSize == 5
        Map<DataDirectory, DataDirectory> testMap = new IdentityHashMap<>();
        for (int i=0; ; i++)
        {
            candidates = getWriteableDirectories(dataDirectories, 5L);
            assertEquals(4, candidates.size());

            DataDirectory dir = Directories.pickWriteableDirectory(candidates);
            testMap.put(dir, dir);

            assertFalse(testMap.size() > 4);
            if (testMap.size() == 4)
            {
                // at least (rule of thumb) 100 iterations to see whether there are more (wrong) directories returned
                if (i >= 100)
                    break;
            }

            // random weighted writeable directory algorithm fails to return all possible directories after
            // many tries
            if (i >= 10000000)
                fail();
        }

        // check for writeSize == 11
        testMap.clear();
        for (int i=0; ; i++)
        {
            candidates = getWriteableDirectories(dataDirectories, 11L);
            assertEquals(3, candidates.size());
            for (Directories.DataDirectoryCandidate candidate : candidates)
                assertTrue(candidate.dataDirectory.getAvailableSpace() >= 11L);

            DataDirectory dir = Directories.pickWriteableDirectory(candidates);
            testMap.put(dir, dir);

            assertFalse(testMap.size() > 3);
            if (testMap.size() == 3)
            {
                // at least (rule of thumb) 100 iterations
                if (i >= 100)
                    break;
            }

            // random weighted writeable directory algorithm fails to return all possible directories after
            // many tries
            if (i >= 10000000)
                fail();
        }
    }

    @Test
    public void testGetLocationForDisk()
    {
        Collection<DataDirectory> paths = new ArrayList<>();
        paths.add(new DataDirectory(new File("/tmp/aaa")));
        paths.add(new DataDirectory(new File("/tmp/aa")));
        paths.add(new DataDirectory(new File("/tmp/a")));

        for (TableMetadata cfm : CFM)
        {
            Directories dirs = new Directories(cfm, paths);
            for (DataDirectory dir : paths)
            {
                String p = dirs.getLocationForDisk(dir).getAbsolutePath() + File.separator;
                assertTrue(p.startsWith(dir.location.getAbsolutePath() + File.separator));
            }
        }
    }

    @Test
    public void testGetLocationWithSymlinks() throws IOException
    {
        Path p = Files.createTempDirectory("something");
        Path symlinktarget = Files.createDirectories(p.resolve("symlinktarget"));
        Path ddir = Files.createDirectories(p.resolve("datadir1"));

        Path p1 = Files.createDirectories(ddir.resolve("p1").resolve("ks")).getParent(); // the data dir does not include the keyspace dir
        Path p2 = Files.createDirectories(ddir.resolve("p2"));
        Path l1 = Files.createSymbolicLink(p2.resolve("ks"), symlinktarget);

        DataDirectory path1 = new DataDirectory(p1.toFile());
        DataDirectory path2 = new DataDirectory(p2.toFile());
        Directories dirs = new Directories(CFM.iterator().next(), new DataDirectory[] {path1, path2});
        dirs.getLocationForDisk(new DataDirectory(p1.toFile()));
        dirs.getLocationForDisk(new DataDirectory(p2.toFile()));

        assertTrue(dirs.getLocationForDisk(path2).toPath().startsWith(l1));
        assertTrue(dirs.getLocationForDisk(path1).toPath().startsWith(p1));
    }

    @Test
    public void getDataDirectoryForFile()
    {
        Collection<DataDirectory> paths = new ArrayList<>();
        paths.add(new DataDirectory("/tmp/a"));
        paths.add(new DataDirectory("/tmp/aa"));
        paths.add(new DataDirectory("/tmp/aaa"));

        for (TableMetadata cfm : CFM)
        {
            Directories dirs = new Directories(cfm, paths);
            for (DataDirectory dir : paths)
            {
                Descriptor d = Descriptor.fromFilename(new File(dir.location, getNewFilename(cfm, false)).toString());
                String p = dirs.getDataDirectoryForFile(d).location.getAbsolutePath() + File.separator;
                assertTrue(p.startsWith(dir.location.getAbsolutePath() + File.separator));
            }
        }
    }

    /**
     * Makes sure we can find the data directory when it is a symlink
     *
     * creates the following data directories:
     * <tempdir something>/datadir1
     * <tempdir something>/datadir11 (symlink to <tempdir something>/symlinktarget)
     *
     * and then makes sure that we get the correct directory back.
     */
    @Test
    public void testDirectoriesSymlinks() throws IOException
    {
        Path p = Files.createTempDirectory("something");
        Path symlinktarget = Files.createDirectories(p.resolve("symlinktarget"));
        Path ddir1 = Files.createDirectories(p.resolve("datadir1"));
        Path ddir2 = Files.createSymbolicLink(p.resolve("datadir11"), symlinktarget);
        DataDirectory dd1 = new DataDirectory(ddir1.toFile());
        DataDirectory dd2 = new DataDirectory(ddir2.toFile());

        for (TableMetadata tm : CFM)
        {
            Directories dirs = new Directories(tm, Sets.newHashSet(dd1, dd2));
            Descriptor desc = Descriptor.fromFilename(ddir1.resolve(getNewFilename(tm, false)).toFile());
            assertEquals(ddir1.toFile(), dirs.getDataDirectoryForFile(desc).location);
            desc = Descriptor.fromFilename(ddir2.resolve(getNewFilename(tm, false)).toFile());
            assertEquals(ddir2.toFile(), dirs.getDataDirectoryForFile(desc).location);
        }
    }

    @Test
    public void testDirectoriesOldTableSymlink() throws IOException
    {
        testDirectoriesSymlinksHelper(true);
    }

    @Test
    public void testDirectoriesTableSymlink() throws IOException
    {
        testDirectoriesSymlinksHelper(false);
    }

    /**
     * Makes sure we can find the data directory for a file when the table directory is a symlink
     *
     * if oldStyle is false we append the table id to the table directory
     *
     * creates the following structure
     * <tempdir>/datadir1/<ks>/<table>
     * <tempdir>/datadir11/<ks>/<table symlink to <tempdir>/symlinktarget>
     *
     * and then we create a fake descriptor to a file in the table directory and make sure we get the correct
     * data directory back.
     */
    private void testDirectoriesSymlinksHelper(boolean oldStyle) throws IOException
    {
        Path p = Files.createTempDirectory("something");
        Path symlinktarget = Files.createDirectories(p.resolve("symlinktarget"));
        Path ddir1 = Files.createDirectories(p.resolve("datadir1"));
        Path ddir2 = Files.createDirectories(p.resolve("datadir11"));

        for (TableMetadata tm : CFM)
        {
            Path keyspacedir = Files.createDirectories(ddir2.resolve(tm.keyspace));
            String tabledir = tm.name + (oldStyle ? "" : Component.separator + tm.id.toHexString());
            Files.createSymbolicLink(keyspacedir.resolve(tabledir), symlinktarget);
        }

        DataDirectory dd1 = new DataDirectory(ddir1.toFile());
        DataDirectory dd2 = new DataDirectory(ddir2.toFile());
        for (TableMetadata tm : CFM)
        {
            Directories dirs = new Directories(tm, Sets.newHashSet(dd1, dd2));
            Descriptor desc = Descriptor.fromFilename(ddir1.resolve(getNewFilename(tm, oldStyle)).toFile());
            assertEquals(ddir1.toFile(), dirs.getDataDirectoryForFile(desc).location);
            desc = Descriptor.fromFilename(ddir2.resolve(getNewFilename(tm, oldStyle)).toFile());
            assertEquals(ddir2.toFile(), dirs.getDataDirectoryForFile(desc).location);
        }
    }

    @Test
    public void testIsStoredInLocalSystemKeyspacesDataLocation()
    {
        for (String table : SystemKeyspace.TABLES_SPLIT_ACROSS_MULTIPLE_DISKS)
        {
            assertFalse(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.SYSTEM_KEYSPACE_NAME, table));
        }
        assertTrue(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PEERS_V2));
        assertTrue(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.TRANSFERRED_RANGES_V2));
        assertTrue(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.KEYSPACES));
        assertTrue(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES));
        assertFalse(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES));
        assertFalse(Directories.isStoredInLocalSystemKeyspacesDataLocation(KS, TABLES[0]));
    }

    @Test
    public void testDataDirectoriesIterator() throws IOException
    {
        Path tmpDir = Files.createTempDirectory(this.getClass().getSimpleName());
        Path subDir_1 = Files.createDirectory(tmpDir.resolve("a"));
        Path subDir_2 = Files.createDirectory(tmpDir.resolve("b"));
        Path subDir_3 = Files.createDirectory(tmpDir.resolve("c"));

        DataDirectories directories = new DataDirectories(new String[]{subDir_1.toString(), subDir_2.toString()},
                                                          new String[]{subDir_3.toString()});

        Iterator<DataDirectory> iter = directories.iterator();
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_1.toFile()), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_2.toFile()), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_3.toFile()), iter.next());
        assertFalse(iter.hasNext());

        directories = new DataDirectories(new String[]{subDir_1.toString(), subDir_2.toString()},
                                                          new String[]{subDir_1.toString()});

        iter = directories.iterator();
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_1.toFile()), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_2.toFile()), iter.next());
        assertFalse(iter.hasNext());
    }

    private String getNewFilename(TableMetadata tm, boolean oldStyle)
    {
        return tm.keyspace + File.separator + tm.name + (oldStyle ? "" : Component.separator + tm.id.toHexString()) + "/na-1-big-Data.db";
    }

    private List<Directories.DataDirectoryCandidate> getWriteableDirectories(DataDirectory[] dataDirectories, long writeSize)
    {
        // copied from Directories.getWriteableLocation(long)
        List<Directories.DataDirectoryCandidate> candidates = new ArrayList<>();

        long totalAvailable = 0L;

        for (DataDirectory dataDir : dataDirectories)
            {
                Directories.DataDirectoryCandidate candidate = new Directories.DataDirectoryCandidate(dataDir);
                // exclude directory if its total writeSize does not fit to data directory
                if (candidate.availableSpace < writeSize)
                    continue;
                candidates.add(candidate);
                totalAvailable += candidate.availableSpace;
            }

        Directories.sortWriteableCandidates(candidates, totalAvailable);

        return candidates;
    }

}
