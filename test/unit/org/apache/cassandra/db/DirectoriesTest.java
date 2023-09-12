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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
// Our version of Sfl4j seems to be missing the ListAppender class.
// Future sfl4j versions have one. At that time the below imports can be
// replaced with `org.slf4j.*` equivalents.
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Directories.DataDirectories;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.schema.MockSchema.sstableId;
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


    private static final String MDCID = "test-DirectoriesTest-id";
    private static AtomicInteger diyThreadId = new AtomicInteger(1);
    private int myDiyId = -1;
    private static Logger logger;
    private ListAppender<ILoggingEvent> listAppender;
    @Parameterized.Parameter(0)
    public SSTableId.Builder<? extends SSTableId> idBuilder;

    @Parameterized.Parameter(1)
    public Supplier<? extends SSTableId> idGenerator;

    @Parameterized.Parameters
    public static Collection<Object[]> idBuilders()
    {
        return Arrays.asList(new Object[]{ SequenceBasedSSTableId.Builder.instance, Util.newSeqGen() },
                             new Object[]{ UUIDBasedSSTableId.Builder.instance, Util.newUUIDGen() });
    }

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
    }

    @Before
    public void beforeTest() throws IOException
    {
        MockSchema.sstableIds.clear();
        MockSchema.sstableIdGenerator = idGenerator;

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
        tempDataDir.tryDelete(); // hack to create a temp dir
        tempDataDir.tryCreateDirectory();

        // Create two fake data dir for tests, one using CF directories, one that do not.
        createTestFiles();
        tailLogs();
    }

    @AfterClass
    public static void afterClass()
    {
        FileUtils.deleteRecursive(tempDataDir);
    }

    @After
    public void afterTest()
    {
        detachLogger();
    }

    private static DataDirectory[] toDataDirectories(File location)
    {
        return new DataDirectory[] { new DataDirectory(location) };
    }

    private static DataDirectory[] toDataDirectories(File[] locations)
    {
        DataDirectory[] dirs = new DataDirectory[locations.length];
        for (int i=0; i<locations.length; i++)
        {
            dirs[i] = new DataDirectory(locations[i]);
        }
        return dirs;
    }

    private void createTestFiles() throws IOException
    {
        for (TableMetadata cfm : CFM)
        {
            List<File> fs = new ArrayList<>();
            files.put(cfm.name, fs);
            File dir = cfDir(cfm);
            dir.tryCreateDirectories();

            createFakeSSTable(dir, cfm.name, 1, fs);
            createFakeSSTable(dir, cfm.name, 2, fs);

            File backupDir = new File(dir, Directories.BACKUPS_SUBDIR);
            backupDir.tryCreateDirectory();
            createFakeSSTable(backupDir, cfm.name, 1, fs);

            File snapshotDir = new File(dir, Directories.SNAPSHOT_SUBDIR + File.pathSeparator() + "42");
            snapshotDir.tryCreateDirectories();
            createFakeSSTable(snapshotDir, cfm.name, 1, fs);
        }
    }

    private void createFakeSSTable(File dir, String cf, int gen, List<File> addTo) throws IOException
    {
        Descriptor desc = new Descriptor(dir, KS, cf, sstableId(gen), SSTableFormat.Type.BIG);
        for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER })
        {
            File f = desc.fileFor(c);
            assert f.createFileIfNotExists();
            addTo.add(f);
        }
    }

    private List<File> createFakeSSTable(File dir, String cf, int gen) throws IOException
    {
        Descriptor desc = new Descriptor(dir, KS, cf, sstableId(gen), SSTableFormat.Type.BIG);
        return createFakeSSTable(desc);
    }

    private List<File> createFakeSSTable(Descriptor desc) throws IOException
    {
        List<File> components = new ArrayList<>(3);
        for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER })
        {
            File f = desc.fileFor(c);
            assert f.createFileIfNotExists();
            components.add(f);
        }
        return components;
    }

    private static File cfDir(TableMetadata metadata)
    {
        String tableId = metadata.id.toHexString();
        int idx = metadata.name.indexOf(Directories.SECONDARY_INDEX_NAME_SEPARATOR);
        if (idx >= 0)
        {
            // secondary index
            return new File(tempDataDir,
                            metadata.keyspace + File.pathSeparator() +
                            metadata.name.substring(0, idx) + '-' + tableId + File.pathSeparator() +
                            metadata.name.substring(idx));
        }
        else
        {
            return new File(tempDataDir, metadata.keyspace + File.pathSeparator() + metadata.name + '-' + tableId);
        }
    }

    @Test
    public void testStandardDirs() throws IOException
    {
        for (TableMetadata cfm : CFM)
        {
            Directories directories = new Directories(cfm, toDataDirectories(tempDataDir));
            assertEquals(cfDir(cfm), directories.getDirectoryForNewSSTables());

            Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.name, sstableId(1), SSTableFormat.Type.BIG);
            File snapshotDir = new File(cfDir(cfm),  File.pathSeparator() + Directories.SNAPSHOT_SUBDIR + File.pathSeparator() + "42");
            assertEquals(snapshotDir.toCanonical(), Directories.getSnapshotDirectory(desc, "42"));

            File backupsDir = new File(cfDir(cfm),  File.pathSeparator() + Directories.BACKUPS_SUBDIR);
            assertEquals(backupsDir.toCanonical(), Directories.getBackupsDirectory(desc));

            Supplier<? extends SSTableId> uidGen = directories.getUIDGenerator(idBuilder);
            assertThat(Stream.generate(uidGen).limit(100).filter(MockSchema.sstableIds::containsValue).collect(Collectors.toList())).isEmpty();
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
        Descriptor parentDesc = new Descriptor(parentDirectories.getDirectoryForNewSSTables(), KS, PARENT_CFM.name, sstableId(0), SSTableFormat.Type.BIG);
        Descriptor indexDesc = new Descriptor(indexDirectories.getDirectoryForNewSSTables(), KS, INDEX_CFM.name, sstableId(0), SSTableFormat.Type.BIG);

        // snapshot dir should be created under its parent's
        File parentSnapshotDirectory = Directories.getSnapshotDirectory(parentDesc, "test");
        File indexSnapshotDirectory = Directories.getSnapshotDirectory(indexDesc, "test");
        assertEquals(parentSnapshotDirectory, indexSnapshotDirectory.parent());

        // check if snapshot directory exists
        parentSnapshotDirectory.tryCreateDirectories();
        assertTrue(parentDirectories.snapshotExists("test"));
        assertTrue(indexDirectories.snapshotExists("test"));

        // check true snapshot size
        Descriptor parentSnapshot = new Descriptor(parentSnapshotDirectory, KS, PARENT_CFM.name, sstableId(0), SSTableFormat.Type.BIG);
        createFile(parentSnapshot.fileFor(Component.DATA), 30);
        Descriptor indexSnapshot = new Descriptor(indexSnapshotDirectory, KS, INDEX_CFM.name, sstableId(0), SSTableFormat.Type.BIG);
        createFile(indexSnapshot.fileFor(Component.DATA), 40);

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
        assertEquals(parentBackupDirectory, indexBackupDirectory.parent());
    }

    private File createFile(File file, int size)
    {
        try (FileOutputStreamPlus writer = new FileOutputStreamPlus(file);)
        {
            writer.write(new byte[size]);
            writer.flush();
        }
        catch (IOException ignore) {}
        return file;
    }

    @Test
    public void testVerifyFullPermissions() throws IOException
    {
        Assert.assertFalse(Directories.verifyFullPermissions(new File("non_directory.txt")));

        Path tmpDir = Files.createTempDirectory(this.getClass().getSimpleName());
        File dir = new File(tmpDir, "sub_dir");
        dir.tryCreateDirectories();
        Assert.assertTrue(Directories.verifyFullPermissions(dir));
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
            if (f.path().contains(Directories.SNAPSHOT_SUBDIR) || f.path().contains(Directories.BACKUPS_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else
                assertTrue(f + " is missing", listed.contains(f));
        }

        // List all but including backup (but no snapshot)
        lister = directories.sstableLister(Directories.OnTxnErr.THROW).includeBackups(true);
        listed = new HashSet<>(lister.listFiles());
        for (File f : files.get(cfm.name))
        {
            if (f.path().contains(Directories.SNAPSHOT_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else
                assertTrue(f + " is missing", listed.contains(f));
        }

        // Skip temporary and compacted
        lister = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
        listed = new HashSet<>(lister.listFiles());
        for (File f : files.get(cfm.name))
        {
            if (f.path().contains(Directories.SNAPSHOT_SUBDIR) || f.path().contains(Directories.BACKUPS_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else if (f.name().contains("tmp-"))
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
            tempDir.tryCreateDirectory();
            File tempFile = new File(tempDir, "tempFile");
            tempFile.createFileIfNotExists();

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
                File dir = new File(first.location, StringUtils.join(path, File.pathSeparator()));
                JVMStabilityInspector.inspectThrowable(new FSWriteError(new IOException("Unable to create directory " + dir), dir));
            }

            File file = new File(first.location, new File(KS, "bad").path());
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
                    Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.name, sstableId(1), SSTableFormat.Type.BIG);
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
        paths.add(new DataDirectory(new File("/tmp/a").toPath()));

        for (TableMetadata cfm : CFM)
        {
            Directories dirs = new Directories(cfm, paths);
            for (DataDirectory dir : paths)
            {
                String p = dirs.getLocationForDisk(dir).absolutePath() + File.pathSeparator();
                assertTrue(p.startsWith(dir.location.absolutePath() + File.pathSeparator()));
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

        DataDirectory path1 = new DataDirectory(new File(p1));
        DataDirectory path2 = new DataDirectory(new File(p2));
        Directories dirs = new Directories(CFM.iterator().next(), new DataDirectory[] {path1, path2});
        dirs.getLocationForDisk(new DataDirectory(new File(p1)));
        dirs.getLocationForDisk(new DataDirectory(new File(p2)));

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
                String p = dirs.getDataDirectoryForFile(d).location.absolutePath() + File.pathSeparator();
                assertTrue(p.startsWith(dir.location.absolutePath() + File.pathSeparator()));
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
        DataDirectory dd1 = new DataDirectory(new File(ddir1));
        DataDirectory dd2 = new DataDirectory(new File(ddir2));

        for (TableMetadata tm : CFM)
        {
            Directories dirs = new Directories(tm, Sets.newHashSet(dd1, dd2));
            Descriptor desc = Descriptor.fromFilename(new File(ddir1.resolve(getNewFilename(tm, false))));
            assertEquals(new File(ddir1), dirs.getDataDirectoryForFile(desc).location);
            desc = Descriptor.fromFilename(new File(ddir2.resolve(getNewFilename(tm, false))));
            assertEquals(new File(ddir2), dirs.getDataDirectoryForFile(desc).location);
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

    @Test
    public void testFileActionHasPrivilege() throws IOException
    {
        Path p = Files.createTempDirectory("something");
        File file = new File(p);
        assertTrue(Directories.FileAction.hasPrivilege(file, Directories.FileAction.X));
        assertTrue(Directories.FileAction.hasPrivilege(file, Directories.FileAction.W));
        assertTrue(Directories.FileAction.hasPrivilege(file, Directories.FileAction.XW));
        assertTrue(Directories.FileAction.hasPrivilege(file, Directories.FileAction.R));
        assertTrue(Directories.FileAction.hasPrivilege(file, Directories.FileAction.XR));
        assertTrue(Directories.FileAction.hasPrivilege(file, Directories.FileAction.RW));
        assertTrue(Directories.FileAction.hasPrivilege(file, Directories.FileAction.XRW));
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

        DataDirectory dd1 = new DataDirectory(new File(ddir1));
        DataDirectory dd2 = new DataDirectory(new File(ddir2));
        for (TableMetadata tm : CFM)
        {
            Directories dirs = new Directories(tm, Sets.newHashSet(dd1, dd2));
            Descriptor desc = Descriptor.fromFilename(new File(ddir1.resolve(getNewFilename(tm, oldStyle))));
            assertEquals(new File(ddir1), dirs.getDataDirectoryForFile(desc).location);
            desc = Descriptor.fromFilename(new File(ddir2.resolve(getNewFilename(tm, oldStyle))));
            assertEquals(new File(ddir2), dirs.getDataDirectoryForFile(desc).location);
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
        assertTrue(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.KEYSPACES));
        assertTrue(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES));
        assertFalse(Directories.isStoredInLocalSystemKeyspacesDataLocation(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES));
        assertFalse(Directories.isStoredInLocalSystemKeyspacesDataLocation(KS, TABLES[0]));
    }

    @Test
    public void testDataDirectoriesIterator() throws IOException
    {
        Path tmpDir = Files.createTempDirectory(this.getClass().getSimpleName());
        File subDir_1 = new File(Files.createDirectory(tmpDir.resolve("a")));
        File subDir_2 = new File(Files.createDirectory(tmpDir.resolve("b")));
        File subDir_3 = new File(Files.createDirectory(tmpDir.resolve("c")));

        DataDirectories directories = new DataDirectories(new File[]{subDir_1, subDir_2}, new File[]{subDir_3});

        Iterator<DataDirectory> iter = directories.iterator();
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_1), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_2), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_3), iter.next());
        assertFalse(iter.hasNext());

        directories = new DataDirectories(new File[]{subDir_1, subDir_2}, new File[]{subDir_1});

        iter = directories.iterator();
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_1), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(subDir_2), iter.next());
        assertFalse(iter.hasNext());
    }

    private String getNewFilename(TableMetadata tm, boolean oldStyle)
    {
        return tm.keyspace + File.pathSeparator() + tm.name + (oldStyle ? "" : Component.separator + tm.id.toHexString()) + "/na-1-big-Data.db";
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

    private int getDiyThreadId()
    {
        return myDiyId = diyThreadId.getAndIncrement();
    }

    private void detachLogger()
    {
        logger.detachAppender(listAppender);
        MDC.remove(this.MDCID);
    }

    private void tailLogs()
    {
        int diyId = getDiyThreadId();
        MDC.put(this.MDCID, String.valueOf(diyId));
        logger = (Logger) LoggerFactory.getLogger(Directories.class);

        // create and start a ListAppender
        listAppender = new ListAppender<>();
        listAppender.start();

        // add the appender to the logger
        logger.addAppender(listAppender);
    }

    private List<ILoggingEvent> filterLogByDiyId(List<ILoggingEvent> log)
    {
        ArrayList<ILoggingEvent> filteredLog = new ArrayList<>();
        for (ILoggingEvent event : log)
        {
            int mdcId = Integer.parseInt(event.getMDCPropertyMap().get(this.MDCID));
            if (mdcId == myDiyId)
            {
                filteredLog.add(event);
            }
        }
        return filteredLog;
    }

    private void checkFormattedMessage(List<ILoggingEvent> log, Level expectedLevel, String expectedMessage, int expectedCount)
    {
        int found=0;
        for(ILoggingEvent e: log)
        {
            System.err.println(e.getFormattedMessage());
            if(e.getFormattedMessage().endsWith(expectedMessage))
            {
                if (e.getLevel() == expectedLevel)
                    found++;
            }
        }

        assertEquals(expectedCount, found);
    }

    @Test
    public void testHasAvailableDiskSpace()
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

        Directories d = new Directories( ((TableMetadata) CFM.toArray()[0]), dataDirectories);

        assertTrue(d.hasAvailableDiskSpace(1,2));
        assertTrue(d.hasAvailableDiskSpace(10,99));
        assertFalse(d.hasAvailableDiskSpace(10,1024));
        assertFalse(d.hasAvailableDiskSpace(1024,1024*1024));

        List<ILoggingEvent> filteredLog = listAppender.list;
        //List<ILoggingEvent> filteredLog = filterLogByDiyId(listAppender.list);
        // Log messages can be out of order, even for the single thread. (e tui AsyncAppender?)
        // We can deal with it, it's sufficient to just check that all messages exist in the result
        assertEquals(23, filteredLog.size());
        String logMsgFormat = "DataDirectory %s has %d bytes available, checking if we can write %d bytes";
        String logMsg = String.format(logMsgFormat, "/nearlyFullDir1", 11, 2);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/uniformDir2", 999, 2);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/veryFullDir", 4, 2);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/nearlyFullDir1", 11, 2);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/uniformDir2", 999, 9);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/veryFullDir", 4, 9);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/nearlyFullDir1", 11, 102);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/uniformDir2", 999, 102);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/veryFullDir", 4, 102);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/nearlyFullDir1", 11, 1024);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/uniformDir2", 999, 1024);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/veryFullDir", 4, 1024);
        checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 1);

        logMsgFormat = "DataDirectory %s can't be used for compaction. Only %s is available, but %s is the minimum write size.";
        logMsg = String.format(logMsgFormat, "/veryFullDir", "4 bytes", "9 bytes");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/nearlyFullDir1", "11 bytes", "102 bytes");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/veryFullDir", "4 bytes", "102 bytes");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/nearlyFullDir1", "11 bytes", "1 KiB");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/uniformDir2", "999 bytes", "1 KiB");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
        logMsg = String.format(logMsgFormat, "/veryFullDir", "4 bytes", "1 KiB");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);

        logMsgFormat = "Across %s there's only %s available, but %s is needed.";
        logMsg = String.format(logMsgFormat, "[/nearlyFullDir1,/uniformDir2,/veryFullDir]", "999 bytes", "1 KiB");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
        logMsg = String.format(logMsgFormat, "[/nearlyFullDir1,/uniformDir2,/veryFullDir]", "0 bytes", "1 MiB");
        checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
    }
}
