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
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
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
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.ColumnIdentifier;
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
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.schema.MockSchema.sstableId;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class DirectoriesTest
{
    public static final String TABLE_NAME = "FakeTable";
    public static final String SNAPSHOT1 = "snapshot1";
    public static final String SNAPSHOT2 = "snapshot2";
    public static final String SNAPSHOT3 = "snapshot3";

    public static final String LEGACY_SNAPSHOT_NAME = "42";


    private static File tempDataDir;
    private static final String KS = "ks";
    private static String[] TABLES;
    private static Set<TableMetadata> CFM;
    private static Map<String, List<File>> sstablesByTableName;

    private static final String MDCID = "test-DirectoriesTest-id";
    private static AtomicInteger diyThreadId = new AtomicInteger(1);
    private int myDiyId = -1;
    private static Logger logger;
    private ListAppender<ILoggingEvent> listAppender;

    @Parameterized.Parameter
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
        sstablesByTableName = new HashMap<>();

        for (String table : TABLES)
        {
            CFM.add(createFakeTable(table));
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
        tempDataDir.deleteRecursive();
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

    private void createTestFiles()
    {
        for (TableMetadata cfm : CFM)
        {
            List<File> allSStables = new ArrayList<>();
            sstablesByTableName.put(cfm.name, allSStables);
            File tableDir = cfDir(cfm);
            tableDir.tryCreateDirectories();

            allSStables.addAll(createFakeSSTable(tableDir, cfm.name, 1));
            allSStables.addAll(createFakeSSTable(tableDir, cfm.name, 2));

            File backupDir = new File(tableDir, Directories.BACKUPS_SUBDIR);
            backupDir.tryCreateDirectory();
            allSStables.addAll(createFakeSSTable(backupDir, cfm.name, 1));

            File snapshotDir = new File(tableDir, Directories.SNAPSHOT_SUBDIR + File.pathSeparator() + LEGACY_SNAPSHOT_NAME);
            snapshotDir.tryCreateDirectories();
            allSStables.addAll(createFakeSSTable(snapshotDir, cfm.name, 1));
        }
    }

    static class FakeSnapshot {
        final TableMetadata table;
        final String tag;
        final File snapshotDir;
        final SnapshotManifest manifest;
        final boolean ephemeral;

        FakeSnapshot(TableMetadata table, String tag, File snapshotDir, SnapshotManifest manifest, boolean ephemeral)
        {
            this.table = table;
            this.tag = tag;
            this.snapshotDir = snapshotDir;
            this.manifest = manifest;
            this.ephemeral = ephemeral;
        }

        public TableSnapshot asTableSnapshot()
        {
            Instant createdAt = manifest == null ? null : manifest.createdAt;
            Instant expiresAt = manifest == null ? null : manifest.expiresAt;
            return new TableSnapshot(table.keyspace, table.name, table.id.asUUID(), tag, createdAt, expiresAt, Collections.singleton(snapshotDir), ephemeral);
        }
    }

    private TableMetadata createFakeTable(String table)
    {
        return TableMetadata.builder(KS, table)
                            .addPartitionKeyColumn("thekey", UTF8Type.instance)
                            .addClusteringColumn("thecolumn", UTF8Type.instance)
                            .build();
    }

    public FakeSnapshot createFakeSnapshot(TableMetadata table, String tag, boolean createManifest, boolean ephemeral) throws IOException
    {
        File tableDir = cfDir(table);
        tableDir.tryCreateDirectories();
        File snapshotDir = new File(tableDir, Directories.SNAPSHOT_SUBDIR + File.pathSeparator() + tag);
        snapshotDir.tryCreateDirectories();

        Descriptor sstableDesc = new Descriptor(snapshotDir, KS, table.name, sstableId(1), DatabaseDescriptor.getSelectedSSTableFormat());
        createFakeSSTable(sstableDesc);

        SnapshotManifest manifest = null;
        if (createManifest)
        {
            File manifestFile = Directories.getSnapshotManifestFile(snapshotDir);
            manifest = new SnapshotManifest(Collections.singletonList(sstableDesc.fileFor(Components.DATA).absolutePath()), new DurationSpec.IntSecondsBound("1m"), now(), ephemeral);
            manifest.serializeToJsonFile(manifestFile);
        }
        else if (ephemeral)
        {
            Files.createFile(snapshotDir.toPath().resolve("ephemeral.snapshot"));
        }

        return new FakeSnapshot(table, tag, snapshotDir, manifest, ephemeral);
    }

    private List<File> createFakeSSTable(File dir, String cf, int gen)
    {
        Descriptor desc = new Descriptor(dir, KS, cf, sstableId(gen), DatabaseDescriptor.getSelectedSSTableFormat());
        return createFakeSSTable(desc);
    }

    private List<File> createFakeSSTable(Descriptor desc)
    {
        List<File> components = new ArrayList<>(3);
        for (Component c : DatabaseDescriptor.getSelectedSSTableFormat().uploadComponents())
        {
            File f = desc.fileFor(c);
            f.createFileIfNotExists();
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
    public void testStandardDirs()
    {
        for (TableMetadata cfm : CFM)
        {
            Directories directories = new Directories(cfm, toDataDirectories(tempDataDir));
            assertEquals(cfDir(cfm), directories.getDirectoryForNewSSTables());

            Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.name, sstableId(1), DatabaseDescriptor.getSelectedSSTableFormat());
            File snapshotDir = new File(cfDir(cfm), File.pathSeparator() + Directories.SNAPSHOT_SUBDIR + File.pathSeparator() + LEGACY_SNAPSHOT_NAME);
            assertEquals(snapshotDir.toCanonical(), Directories.getSnapshotDirectory(desc, LEGACY_SNAPSHOT_NAME));

            File backupsDir = new File(cfDir(cfm), File.pathSeparator() + Directories.BACKUPS_SUBDIR);
            assertEquals(backupsDir.toCanonical(), Directories.getBackupsDirectory(desc));

            Supplier<? extends SSTableId> uidGen = directories.getUIDGenerator(idBuilder);
            assertThat(Stream.generate(uidGen).limit(100).filter(MockSchema.sstableIds::containsValue).collect(Collectors.toList())).isEmpty();
        }
    }

    @Test
    public void testListSnapshots() throws Exception {
        // Initial state
        TableMetadata fakeTable = createFakeTable(TABLE_NAME);
        Directories directories = new Directories(fakeTable, toDataDirectories(tempDataDir));
        assertThat(directories.listSnapshots()).isEmpty();

        // Create snapshot with and without manifest
        FakeSnapshot snapshot1 = createFakeSnapshot(fakeTable, SNAPSHOT1, true, false);
        FakeSnapshot snapshot2 = createFakeSnapshot(fakeTable, SNAPSHOT2, false, false);
        // ephemeral without manifst
        FakeSnapshot snapshot3 = createFakeSnapshot(fakeTable, SNAPSHOT3, false, true);

        // Both snapshots should be present
        Map<String, TableSnapshot> snapshots = directories.listSnapshots();
        assertThat(snapshots.keySet()).isEqualTo(Sets.newHashSet(SNAPSHOT1, SNAPSHOT2, SNAPSHOT3));
        assertThat(snapshots.get(SNAPSHOT1)).isEqualTo(snapshot1.asTableSnapshot());
        assertThat(snapshots.get(SNAPSHOT2)).isEqualTo(snapshot2.asTableSnapshot());
        assertThat(snapshots.get(SNAPSHOT3)).isEqualTo(snapshot3.asTableSnapshot());

        // Now remove snapshot1
        snapshot1.snapshotDir.deleteRecursive();

        // Only snapshot 2 and 3 should be present
        snapshots = directories.listSnapshots();
        assertThat(snapshots.keySet()).isEqualTo(Sets.newHashSet(SNAPSHOT2, SNAPSHOT3));
        assertThat(snapshots.get(SNAPSHOT2)).isEqualTo(snapshot2.asTableSnapshot());
        assertThat(snapshots.get(SNAPSHOT3)).isEqualTo(snapshot3.asTableSnapshot());
        assertThat(snapshots.get(SNAPSHOT3).isEphemeral()).isTrue();
    }

    @Test
    public void testListSnapshotDirsByTag() throws Exception {
        // Initial state
        TableMetadata fakeTable = createFakeTable("FakeTable");
        Directories directories = new Directories(fakeTable, toDataDirectories(tempDataDir));
        assertThat(directories.listSnapshotDirsByTag()).isEmpty();

        // Create snapshot with and without manifest
        FakeSnapshot snapshot1 = createFakeSnapshot(fakeTable, SNAPSHOT1, true, false);
        FakeSnapshot snapshot2 = createFakeSnapshot(fakeTable, SNAPSHOT2, false, false);
        FakeSnapshot snapshot3 = createFakeSnapshot(fakeTable, SNAPSHOT3, false, true);

        // Both snapshots should be present
        Map<String, Set<File>> snapshotDirs = directories.listSnapshotDirsByTag();
        assertThat(snapshotDirs.keySet()).isEqualTo(Sets.newHashSet(SNAPSHOT1, SNAPSHOT2, SNAPSHOT3));
        assertThat(snapshotDirs.get(SNAPSHOT1)).allMatch(snapshotDir -> snapshotDir.equals(snapshot1.snapshotDir));
        assertThat(snapshotDirs.get(SNAPSHOT2)).allMatch(snapshotDir -> snapshotDir.equals(snapshot2.snapshotDir));
        assertThat(snapshotDirs.get(SNAPSHOT3)).allMatch(snapshotDir -> snapshotDir.equals(snapshot3.snapshotDir));

        // Now remove snapshot1
        snapshot1.snapshotDir.deleteRecursive();

        // Only snapshot 2 and 3 should be present
        snapshotDirs = directories.listSnapshotDirsByTag();
        assertThat(snapshotDirs.keySet()).isEqualTo(Sets.newHashSet(SNAPSHOT2, SNAPSHOT3));
    }

    @Test
    public void testMaybeManifestLoading() throws Exception {
        for (TableMetadata cfm : CFM)
        {
            String tag = "test";
            Directories directories = new Directories(cfm, toDataDirectories(tempDataDir));
            Descriptor parentDesc = new Descriptor(directories.getDirectoryForNewSSTables(), KS, cfm.name, sstableId(0), DatabaseDescriptor.getSelectedSSTableFormat());
            File parentSnapshotDirectory = Directories.getSnapshotDirectory(parentDesc, tag);

            List<String> files = new LinkedList<>();
            files.add(parentSnapshotDirectory.toAbsolute().absolutePath());

            File manifestFile = directories.getSnapshotManifestFile(tag);

            SnapshotManifest manifest = new SnapshotManifest(files, new DurationSpec.IntSecondsBound("1m"), now(), false);
            manifest.serializeToJsonFile(manifestFile);

            Set<File> dirs = new HashSet<>();

            dirs.add(manifestFile.parent());
            dirs.add(new File("buzz"));
            SnapshotManifest loadedManifest = Directories.maybeLoadManifest(KS, cfm.name, tag, dirs);

            assertEquals(manifest, loadedManifest);
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
        Descriptor parentDesc = new Descriptor(parentDirectories.getDirectoryForNewSSTables(), KS, PARENT_CFM.name, sstableId(0), DatabaseDescriptor.getSelectedSSTableFormat());
        Descriptor indexDesc = new Descriptor(indexDirectories.getDirectoryForNewSSTables(), KS, INDEX_CFM.name, sstableId(0), DatabaseDescriptor.getSelectedSSTableFormat());

        // snapshot dir should be created under its parent's
        File parentSnapshotDirectory = Directories.getSnapshotDirectory(parentDesc, "test");
        File indexSnapshotDirectory = Directories.getSnapshotDirectory(indexDesc, "test");
        assertEquals(parentSnapshotDirectory, indexSnapshotDirectory.parent());

        // check if snapshot directory exists
        parentSnapshotDirectory.tryCreateDirectories();
        assertTrue(parentDirectories.snapshotExists("test"));
        assertTrue(indexDirectories.snapshotExists("test"));

        // check true snapshot size
        Descriptor parentSnapshot = new Descriptor(parentSnapshotDirectory, KS, PARENT_CFM.name, sstableId(0), DatabaseDescriptor.getSelectedSSTableFormat());
        createFile(parentSnapshot.fileFor(Components.DATA), 30);
        Descriptor indexSnapshot = new Descriptor(indexSnapshotDirectory, KS, INDEX_CFM.name, sstableId(0), DatabaseDescriptor.getSelectedSSTableFormat());
        createFile(indexSnapshot.fileFor(Components.DATA), 40);

        assertEquals(30, parentDirectories.trueSnapshotsSize());
        assertEquals(40, indexDirectories.trueSnapshotsSize());

        // check snapshot details
        Map<String, TableSnapshot> parentSnapshotDetail = parentDirectories.listSnapshots();
        assertTrue(parentSnapshotDetail.containsKey("test"));
        // CASSANDRA-17357: include indexes when computing true size of parent table
        assertEquals(70L, parentSnapshotDetail.get("test").computeTrueSizeBytes());

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
        for (File f : sstablesByTableName.get(cfm.name))
        {
            if (f.path().contains(Directories.SNAPSHOT_SUBDIR) || f.path().contains(Directories.BACKUPS_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else
                assertTrue(f + " is missing", listed.contains(f));
        }

        // List all but including backup (but no snapshot)
        lister = directories.sstableLister(Directories.OnTxnErr.THROW).includeBackups(true);
        listed = new HashSet<>(lister.listFiles());
        for (File f : sstablesByTableName.get(cfm.name))
        {
            if (f.path().contains(Directories.SNAPSHOT_SUBDIR))
                assertFalse(f + " should not be listed", listed.contains(f));
            else
                assertTrue(f + " is missing", listed.contains(f));
        }

        // Skip temporary and compacted
        lister = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
        listed = new HashSet<>(lister.listFiles());
        for (File f : sstablesByTableName.get(cfm.name))
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
    public void testTemporaryFile()
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
            final String n = Long.toString(nanoTime());
            Callable<File> directoryGetter = () ->
            {
                Descriptor desc = new Descriptor(cfDir(cfm), KS, cfm.name, sstableId(1), DatabaseDescriptor.getSelectedSSTableFormat());
                return Directories.getSnapshotDirectory(desc, n);
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
    public void testGetLocationForDisk() throws IOException
    {
        Collection<DataDirectory> paths = new ArrayList<>();

        Path tmpDir = Files.createTempDirectory("testGetLocationForDisk");
        paths.add(new DataDirectory(tmpDir.resolve("aaa")));
        paths.add(new DataDirectory(tmpDir.resolve("aa")));
        paths.add(new DataDirectory(tmpDir.resolve("a")));

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
    public void getDataDirectoryForFile() throws IOException
    {
        Collection<DataDirectory> paths = new ArrayList<>();

        Path tmpDir = Files.createTempDirectory("getDataDirectoryForFile");
        paths.add(new DataDirectory(tmpDir.resolve("a")));
        paths.add(new DataDirectory(tmpDir.resolve("aa")));
        paths.add(new DataDirectory(tmpDir.resolve("aaa")));

        for (TableMetadata cfm : CFM)
        {
            Directories dirs = new Directories(cfm, paths);
            for (DataDirectory dir : paths)
            {
                Descriptor d = Descriptor.fromFile(new File(dir.location, getNewFilename(cfm, false)));
                String p = dirs.getDataDirectoryForFile(d).location.absolutePath() + File.pathSeparator();
                assertTrue(p.startsWith(dir.location.absolutePath() + File.pathSeparator()));
            }
        }
    }

    /**
     * Makes sure we can find the data directory when it is a symlink
     *
     * creates the following data directories:
     * {@code <tempdir something>/datadir1}
     * {@code <tempdir something>/datadir11 (symlink to <tempdir something>/symlinktarget)}
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
            Descriptor desc = Descriptor.fromFile(new File(ddir1.resolve(getNewFilename(tm, false))));
            assertEquals(new File(ddir1), dirs.getDataDirectoryForFile(desc).location);
            desc = Descriptor.fromFile(new File(ddir2.resolve(getNewFilename(tm, false))));
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

    /**
     * Makes sure we can find the data directory for a file when the table directory is a symlink
     *
     * if oldStyle is false we append the table id to the table directory
     *
     * creates the following structure
     * {@code <tempdir>/datadir1/<ks>/<table>}
     * {@code <tempdir>/datadir11/<ks>/<table symlink to <tempdir>/symlinktarget>}
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
            Descriptor desc = Descriptor.fromFile(new File(ddir1.resolve(getNewFilename(tm, oldStyle))));
            assertEquals(new File(ddir1), dirs.getDataDirectoryForFile(desc).location);
            desc = Descriptor.fromFile(new File(ddir2.resolve(getNewFilename(tm, oldStyle))));
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
        Path subDir_1 = Files.createDirectory(tmpDir.resolve("a"));
        Path subDir_2 = Files.createDirectory(tmpDir.resolve("b"));
        Path subDir_3 = Files.createDirectory(tmpDir.resolve("c"));

        DataDirectories directories = new DataDirectories(new String[]{subDir_1.toString(), subDir_2.toString()},
                                                          new String[]{subDir_3.toString()});

        Iterator<DataDirectory> iter = directories.iterator();
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(new File(subDir_1)), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(new File(subDir_2)), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(new File(subDir_3)), iter.next());
        assertFalse(iter.hasNext());

        directories = new DataDirectories(new String[]{subDir_1.toString(), subDir_2.toString()},
                                                          new String[]{subDir_1.toString()});

        iter = directories.iterator();
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(new File(subDir_1)), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new DataDirectory(new File(subDir_2)), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testFreeCompactionSpace()
    {
        double oldMaxSpaceForCompactions = DatabaseDescriptor.getMaxSpaceForCompactionsPerDrive();
        long oldFreeSpace = DatabaseDescriptor.getMinFreeSpacePerDriveInMebibytes();
        DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(100.0);
        DatabaseDescriptor.setMinFreeSpacePerDriveInMebibytes(0);
        FileStore fstore = new FakeFileStore();
        try
        {
            DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(1);
            assertEquals(100, Directories.getAvailableSpaceForCompactions(fstore));
            DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(.5);
            assertEquals(50, Directories.getAvailableSpaceForCompactions(fstore));
            DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(0);
            assertEquals(0, Directories.getAvailableSpaceForCompactions(fstore));
        }
        finally
        {
            DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(oldMaxSpaceForCompactions);
            DatabaseDescriptor.setMinFreeSpacePerDriveInMebibytes(oldFreeSpace / FileUtils.ONE_MIB);
        }
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
        for(ILoggingEvent event : log)
        {
            int mdcId = Integer.parseInt(event.getMDCPropertyMap().get(this.MDCID));
            if(mdcId == myDiyId){
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
            if(e.getFormattedMessage().endsWith(expectedMessage))
                if (e.getLevel() == expectedLevel)
                    found++;
        }

        assertEquals(expectedCount, found);
    }

    @Test
    public void testHasAvailableSpace()
    {
        double oldMaxSpaceForCompactions = DatabaseDescriptor.getMaxSpaceForCompactionsPerDrive();
        long oldFreeSpace = DatabaseDescriptor.getMinFreeSpacePerDriveInMebibytes();
        DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(1.0);
        DatabaseDescriptor.setMinFreeSpacePerDriveInMebibytes(0);
        try
        {
            FakeFileStore fs1 = new FakeFileStore();
            FakeFileStore fs2 = new FakeFileStore();
            FakeFileStore fs3 = new FakeFileStore();
            Map<FileStore, Long> writes = new HashMap<>();

            fs1.usableSpace = 30;
            fs2.usableSpace = 30;
            fs3.usableSpace = 30;

            writes.put(fs1, 20L);
            writes.put(fs2, 20L);
            writes.put(fs3, 20L);
            assertTrue(Directories.hasDiskSpaceForCompactionsAndStreams(writes));

            fs1.usableSpace = 19;
            assertFalse(Directories.hasDiskSpaceForCompactionsAndStreams(writes));

            writes.put(fs2, 25L*1024*1024+9);
            fs2.usableSpace = 20L*1024*1024-9;
            writes.put(fs3, 999L*1024*1024*1024+9);
            fs2.usableSpace = 20L*1024+99;
            assertFalse(Directories.hasDiskSpaceForCompactionsAndStreams(writes));

            fs1.usableSpace = 30;
            fs2.usableSpace = 30;
            fs3.usableSpace = 30L*1024*1024*1024*1024;

            writes.put(fs1, 20L);
            writes.put(fs2, 20L);
            writes.put(fs3, 30L*1024*1024*1024*1024+1);
            assertFalse(Directories.hasDiskSpaceForCompactionsAndStreams(writes));

            List<ILoggingEvent> filteredLog = filterLogByDiyId(listAppender.list);
            // Log messages can be out of order, even for the single thread. (e tui AsyncAppender?)
            // We can deal with it, it's sufficient to just check that all messages exist in the result
            assertEquals(17, filteredLog.size());

            String logMsg = "30 bytes available, checking if we can write 20 bytes";
            checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 7);
            logMsg = "19 bytes available, checking if we can write 20 bytes";
            checkFormattedMessage(filteredLog, Level.DEBUG, logMsg, 2);


            logMsg = "19 bytes available, but 20 bytes is needed";
            checkFormattedMessage(filteredLog, Level.WARN, logMsg, 2);
            logMsg = "has only 20.1 KiB available, but 25 MiB is needed";
            checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
            logMsg = "has only 30 bytes available, but 999 GiB is needed";
            checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
            logMsg = "has only 30 TiB available, but 30 TiB is needed";
            checkFormattedMessage(filteredLog, Level.WARN, logMsg, 1);
        }
        finally
        {
            DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(oldMaxSpaceForCompactions);
            DatabaseDescriptor.setMinFreeSpacePerDriveInMebibytes(oldFreeSpace);
        }
    }

    @Test
    public void testHasAvailableSpaceSumming()
    {
        double oldMaxSpaceForCompactions = DatabaseDescriptor.getMaxSpaceForCompactionsPerDrive();
        long oldFreeSpace = DatabaseDescriptor.getMinFreeSpacePerDriveInMebibytes();
        DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(1.0);
        DatabaseDescriptor.setMinFreeSpacePerDriveInMebibytes(0);
        try
        {
            FakeFileStore fs1 = new FakeFileStore();
            FakeFileStore fs2 = new FakeFileStore();
            Map<File, Long> expectedNewWriteSizes = new HashMap<>();
            Map<File, Long> totalCompactionWriteRemaining = new HashMap<>();

            fs1.usableSpace = 100;
            fs2.usableSpace = 100;

            File f1 = new File("f1");
            File f2 = new File("f2");
            File f3 = new File("f3");

            expectedNewWriteSizes.put(f1, 20L);
            expectedNewWriteSizes.put(f2, 20L);
            expectedNewWriteSizes.put(f3, 20L);

            totalCompactionWriteRemaining.put(f1, 20L);
            totalCompactionWriteRemaining.put(f2, 20L);
            totalCompactionWriteRemaining.put(f3, 20L);
            Function<File, FileStore> filestoreMapper = (f) -> {
                if (f == f1 || f == f2)
                    return fs1;
                return fs2;
            };
            assertTrue(Directories.hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSizes, totalCompactionWriteRemaining, filestoreMapper));
            fs1.usableSpace = 79;
            assertFalse(Directories.hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSizes, totalCompactionWriteRemaining, filestoreMapper));
            fs1.usableSpace = 81;
            assertTrue(Directories.hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSizes, totalCompactionWriteRemaining, filestoreMapper));

            expectedNewWriteSizes.clear();
            expectedNewWriteSizes.put(f1, 100L);
            totalCompactionWriteRemaining.clear();
            totalCompactionWriteRemaining.put(f2, 100L);
            fs1.usableSpace = 150;

            assertFalse(Directories.hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSizes, totalCompactionWriteRemaining, filestoreMapper));
            expectedNewWriteSizes.clear();
            expectedNewWriteSizes.put(f1, 100L);
            totalCompactionWriteRemaining.clear();
            totalCompactionWriteRemaining.put(f3, 500L);
            fs1.usableSpace = 150;
            fs2.usableSpace = 400; // too little space for the ongoing compaction, but this filestore does not affect the new compaction so it should be allowed

            assertTrue(Directories.hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSizes, totalCompactionWriteRemaining, filestoreMapper));
        }
        finally
        {
            DatabaseDescriptor.setMaxSpaceForCompactionsPerDrive(oldMaxSpaceForCompactions);
            DatabaseDescriptor.setMinFreeSpacePerDriveInMebibytes(oldFreeSpace / FileUtils.ONE_MIB);
        }
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

    public static class FakeFileStore extends FileStore
    {
        public long usableSpace = 100;
        public long getUsableSpace()
        {
            return usableSpace;
        }
        public String name() {return null;}
        public String type() {return null;}
        public boolean isReadOnly() {return false;}
        public long getTotalSpace() {return 0;}
        public long getUnallocatedSpace() {return 0;}
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {return false;}
        public boolean supportsFileAttributeView(String name) {return false;}
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {return null;}
        public Object getAttribute(String attribute) {return null;}

        public String toString()
        {
            return "MockFileStore";
        }
    }
}
