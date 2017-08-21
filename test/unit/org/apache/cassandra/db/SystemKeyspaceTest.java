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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.CassandraVersion;

import static org.junit.Assert.*;

public class SystemKeyspaceTest
{
    private static final String MIGRATION_SSTABLES_ROOT = "migration-sstable-root";

    // any file name will do but unrelated files in our folders tend to be log files or very old data files
    private static final String UNRELATED_FILE_NAME = "system.log";
    private static final String UNRELATED_FOLDER_NAME = "snapshot-abc";

    @BeforeClass
    public static void prepSnapshotTracker()
    {
        DatabaseDescriptor.daemonInitialization();

        if (FBUtilities.isWindows)
            WindowsFailedSnapshotTracker.deleteOldSnapshots();
    }

    @Test
    public void testLocalTokens()
    {
        // Remove all existing tokens
        Collection<Token> current = SystemKeyspace.loadTokens().asMap().get(FBUtilities.getLocalAddress());
        if (current != null && !current.isEmpty())
            SystemKeyspace.updateTokens(current);

        List<Token> tokens = new ArrayList<Token>()
        {{
            for (int i = 0; i < 9; i++)
                add(new BytesToken(ByteBufferUtil.bytes(String.format("token%d", i))));
        }};

        SystemKeyspace.updateTokens(tokens);
        int count = 0;

        for (Token tok : SystemKeyspace.getSavedTokens())
            assert tokens.get(count++).equals(tok);
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        BytesToken token = new BytesToken(ByteBufferUtil.bytes("token3"));
        InetAddress address = InetAddress.getByName("127.0.0.2");
        Future<?> future = SystemKeyspace.updateTokens(address, Collections.singletonList(token), StageManager.getStage(Stage.MUTATION));
        FBUtilities.waitOnFuture(future);
        assert SystemKeyspace.loadTokens().get(address).contains(token);
        SystemKeyspace.removeEndpoint(address);
        assert !SystemKeyspace.loadTokens().containsValue(token);
    }

    @Test
    public void testLocalHostID()
    {
        UUID firstId = SystemKeyspace.getLocalHostId();
        UUID secondId = SystemKeyspace.getLocalHostId();
        assert firstId.equals(secondId) : String.format("%s != %s%n", firstId.toString(), secondId.toString());
    }

    private void assertDeletedOrDeferred(int expectedCount)
    {
        if (FBUtilities.isWindows)
            assertEquals(expectedCount, getDeferredDeletionCount());
        else
            assertTrue(getSystemSnapshotFiles().isEmpty());
    }

    private int getDeferredDeletionCount()
    {
        try
        {
            Class c = Class.forName("java.io.DeleteOnExitHook");
            LinkedHashSet<String> files = (LinkedHashSet<String>)FBUtilities.getProtectedField(c, "files").get(c);
            return files.size();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void snapshotSystemKeyspaceIfUpgrading() throws IOException
    {
        // First, check that in the absence of any previous installed version, we don't create snapshots
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            cfs.clearUnsafe();
        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);

        int baseline = getDeferredDeletionCount();

        SystemKeyspace.snapshotOnVersionChange();
        assertDeletedOrDeferred(baseline);

        // now setup system.local as if we're upgrading from a previous version
        setupReleaseVersion(getOlderVersionString());
        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertDeletedOrDeferred(baseline);

        // Compare versions again & verify that snapshots were created for all tables in the system ks
        SystemKeyspace.snapshotOnVersionChange();
        assertEquals(SystemKeyspace.metadata().tables.size(), getSystemSnapshotFiles().size());

        // clear out the snapshots & set the previous recorded version equal to the latest, we shouldn't
        // see any new snapshots created this time.
        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        setupReleaseVersion(FBUtilities.getReleaseVersionString());

        SystemKeyspace.snapshotOnVersionChange();

        // snapshotOnVersionChange for upgrade case will open a SSTR when the CFS is flushed. On Windows, we won't be
        // able to delete hard-links to that file while segments are memory-mapped, so they'll be marked for deferred deletion.
        // 10 files expected.
        assertDeletedOrDeferred(baseline + 10);

        Keyspace.clearSnapshot(null, SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    @Test
    public void testMigrateEmptyDataDirs() throws IOException
    {
        File dataDir = Paths.get(DatabaseDescriptor.getAllDataFileLocations()[0]).toFile();
        if (new File(dataDir, "Emptykeyspace1").exists())
            FileUtils.deleteDirectory(new File(dataDir, "Emptykeyspace1"));
        assertTrue(new File(dataDir, "Emptykeyspace1").mkdirs());
        assertEquals(0, numLegacyFiles());
        SystemKeyspace.migrateDataDirs();
        assertEquals(0, numLegacyFiles());

        assertTrue(new File(dataDir, "Emptykeyspace1/table1").mkdirs());
        assertEquals(0, numLegacyFiles());
        SystemKeyspace.migrateDataDirs();
        assertEquals(0, numLegacyFiles());

        assertTrue(new File(dataDir, "Emptykeyspace1/wrong_file").createNewFile());
        assertEquals(0, numLegacyFiles());
        SystemKeyspace.migrateDataDirs();
        assertEquals(0, numLegacyFiles());

    }

    @Test
    public void testMigrateDataDirs_2_1() throws IOException
    {
        testMigrateDataDirs("2.1", 5); // see test data for num legacy files
    }

    @Test
    public void testMigrateDataDirs_2_2() throws IOException
    {
        testMigrateDataDirs("2.2", 7); // see test data for num legacy files
    }

    private void testMigrateDataDirs(String version, int numLegacyFiles) throws IOException
    {
        Path migrationSSTableRoot = Paths.get(System.getProperty(MIGRATION_SSTABLES_ROOT), version);
        Path dataDir = Paths.get(DatabaseDescriptor.getAllDataFileLocations()[0]);

        FileUtils.copyDirectory(migrationSSTableRoot.toFile(), dataDir.toFile());

        assertEquals(numLegacyFiles, numLegacyFiles());

        SystemKeyspace.migrateDataDirs();

        assertEquals(0, numLegacyFiles());
    }

    private static int numLegacyFiles()
    {
        int ret = 0;
        Iterable<String> dirs = Arrays.asList(DatabaseDescriptor.getAllDataFileLocations());
        for (String dataDir : dirs)
        {
            File dir = new File(dataDir);
            for (File ksdir : dir.listFiles((d, n) -> new File(d, n).isDirectory()))
            {
                for (File cfdir : ksdir.listFiles((d, n) -> new File(d, n).isDirectory()))
                {
                    if (Descriptor.isLegacyFile(cfdir))
                    {
                        ret++;
                    }
                    else
                    {
                        File[] legacyFiles = cfdir.listFiles((d, n) -> Descriptor.isLegacyFile(new File(d, n)));
                        if (legacyFiles != null)
                            ret += legacyFiles.length;
                    }
                }
            }
        }
        return ret;
    }

    @Test
    public void testMigrateDataDirs_UnrelatedFiles_2_1() throws IOException
    {
        testMigrateDataDirsWithUnrelatedFiles("2.1");
    }

    @Test
    public void testMigrateDataDirs_UnrelatedFiles_2_2() throws IOException
    {
        testMigrateDataDirsWithUnrelatedFiles("2.2");
    }

    private void testMigrateDataDirsWithUnrelatedFiles(String version) throws IOException
    {
        Path migrationSSTableRoot = Paths.get(System.getProperty(MIGRATION_SSTABLES_ROOT), version);
        Path dataDir = Paths.get(DatabaseDescriptor.getAllDataFileLocations()[0]);

        FileUtils.copyDirectory(migrationSSTableRoot.toFile(), dataDir.toFile());

        addUnRelatedFiles(dataDir);

        SystemKeyspace.migrateDataDirs();

        checkUnrelatedFiles(dataDir);
    }

    /**
     * Add some extra and totally unrelated files to the data dir and its sub-folders
     */
    private void addUnRelatedFiles(Path dataDir) throws IOException
    {
        File dir = new File(dataDir.toString());
        createAndCheck(dir, UNRELATED_FILE_NAME, false);
        createAndCheck(dir, UNRELATED_FOLDER_NAME, true);

        for (File ksdir : dir.listFiles((d, n) -> new File(d, n).isDirectory()))
        {
            createAndCheck(ksdir, UNRELATED_FILE_NAME, false);
            createAndCheck(ksdir, UNRELATED_FOLDER_NAME, true);

            for (File cfdir : ksdir.listFiles((d, n) -> new File(d, n).isDirectory()))
            {
                createAndCheck(cfdir, UNRELATED_FILE_NAME, false);
                createAndCheck(cfdir, UNRELATED_FOLDER_NAME, true);
            }
        }
    }

    /**
     * Make sure the extra files are still in the data dir and its sub-folders, then
     * remove them.
     */
    private void checkUnrelatedFiles(Path dataDir) throws IOException
    {
        File dir = new File(dataDir.toString());
        checkAndDelete(dir, UNRELATED_FILE_NAME, false);
        checkAndDelete(dir, UNRELATED_FOLDER_NAME, true);

        for (File ksdir : dir.listFiles((d, n) -> new File(d, n).isDirectory()))
        {
            checkAndDelete(ksdir, UNRELATED_FILE_NAME, false);
            checkAndDelete(ksdir, UNRELATED_FOLDER_NAME, true);

            for (File cfdir : ksdir.listFiles((d, n) -> new File(d, n).isDirectory()))
            {
                checkAndDelete(cfdir, UNRELATED_FILE_NAME, false);
                checkAndDelete(cfdir, UNRELATED_FOLDER_NAME, true);
            }
        }
    }

    private void createAndCheck(File dir, String fileName, boolean isDir) throws IOException
    {
        File f = new File(dir, fileName);

        if (isDir)
            f.mkdir();
        else
            f.createNewFile();

        assertTrue(f.exists());
    }

    private void checkAndDelete(File dir, String fileName, boolean isDir) throws IOException
    {
        File f = new File(dir, fileName);
        assertTrue(f.exists());

        if (isDir)
            FileUtils.deleteDirectory(f);
        else
            f.delete();
    }

    private String getOlderVersionString()
    {
        String version = FBUtilities.getReleaseVersionString();
        CassandraVersion semver = new CassandraVersion(version.contains("-") ? version.substring(0, version.indexOf('-'))
                                                                           : version);
        return (String.format("%s.%s.%s", semver.major - 1, semver.minor, semver.patch));
    }

    private Set<String> getSystemSnapshotFiles()
    {
        Set<String> snapshottedTableNames = new HashSet<>();
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
        {
            if (!cfs.getSnapshotDetails().isEmpty())
                snapshottedTableNames.add(cfs.getColumnFamilyName());
        }
        return snapshottedTableNames;
    }

    private void setupReleaseVersion(String version)
    {
        // besides the release_version, we also need to insert the cluster_name or the check
        // in SystemKeyspace.checkHealth were we verify it matches DatabaseDescriptor will fail
        QueryProcessor.executeInternal(String.format("INSERT INTO system.local(key, release_version, cluster_name) " +
                                                     "VALUES ('local', '%s', '%s')",
                                                     version,
                                                     DatabaseDescriptor.getClusterName()));
        String r = readLocalVersion();
        assertEquals(String.format("Expected %s, got %s", version, r), version, r);
    }

    private String readLocalVersion()
    {
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT release_version FROM system.local WHERE key='local'");
        return rs.isEmpty() || !rs.one().has("release_version") ? null : rs.one().getString("release_version");
    }
}
