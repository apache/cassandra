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
package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.filesystem.ForwardingFileSystem;
import org.apache.cassandra.io.filesystem.ForwardingFileSystemProvider;
import org.apache.cassandra.io.filesystem.ForwardingPath;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.DataResurrectionCheck.Heartbeat;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INVALID_LEGACY_SSTABLE_ROOT;
import static org.apache.cassandra.io.util.FileUtils.createTempFile;
import static org.apache.cassandra.service.DataResurrectionCheck.HEARTBEAT_FILE_CONFIG_PROPERTY;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.check_data_resurrection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartupChecksTest
{
    StartupChecks startupChecks;
    Path sstableDir;
    static File heartbeatFile;

    StartupChecksOptions options = new StartupChecksOptions();

    @BeforeClass
    public static void setupServer()
    {
        heartbeatFile = createTempFile("cassandra-heartbeat-", "");
        SchemaLoader.prepareServer();
    }

    @Before
    public void setup() throws IOException
    {
        for (ColumnFamilyStore cfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            cfs.clearUnsafe();
        for (File dataDir : Directories.getKSChildDirectories(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            dataDir.deleteRecursive();

        File dataDir = new File(DatabaseDescriptor.getAllDataFileLocations()[0]);
        sstableDir = Paths.get(dataDir.absolutePath(), "Keyspace1", "Standard1");
        Files.createDirectories(sstableDir);

        options.enable(check_data_resurrection);
        options.getConfig(check_data_resurrection)
               .put(HEARTBEAT_FILE_CONFIG_PROPERTY, heartbeatFile.absolutePath());

        startupChecks = new StartupChecks();
    }

    @After
    public void tearDown() throws IOException
    {
        new File(sstableDir).deleteRecursive();
    }

    @AfterClass
    public static void tearDownClass()
    {
        heartbeatFile.delete();
    }

    @Test
    public void failStartupIfInvalidSSTablesFound() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyInvalidLegacySSTables(sstableDir);

        verifyFailure(startupChecks, "Detected unreadable sstables");

        // we should ignore invalid sstables in a snapshots directory
        new File(sstableDir).deleteRecursive();
        Path snapshotDir = sstableDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);
        copyInvalidLegacySSTables(snapshotDir); startupChecks.verify(options);

        // and in a backups directory
        new File(sstableDir).deleteRecursive();
        Path backupDir = sstableDir.resolve("backups");
        Files.createDirectories(backupDir);
        copyInvalidLegacySSTables(backupDir);
        startupChecks.verify(options);

        // and in the system directory as of CASSANDRA-17777
        new File(backupDir).deleteRecursive();
        File dataDir = new File(DatabaseDescriptor.getAllDataFileLocations()[0]);
        Path systemDir = Paths.get(dataDir.absolutePath(), "system", "InvalidSystemDirectory");
        Files.createDirectories(systemDir);
        copyInvalidLegacySSTables(systemDir);
        startupChecks.verify(options);
    }

    @Test
    public void compatibilityCheckIgnoresNonDbFiles() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyLegacyNonSSTableFiles(sstableDir);
        assertNotEquals(0, new File(sstableDir).tryList().length);

        startupChecks.verify(options);
    }

    @Test
    public void checkReadAheadKbSettingCheck() throws Exception
    {
        // This test just validates if the verify function
        // doesn't throw any exceptions
        startupChecks = startupChecks.withTest(StartupChecks.checkReadAheadKbSetting);
        startupChecks.verify(options);
    }

    @Test
    public void testGetReadAheadKBPath()
    {
        Path sdaDirectory = StartupChecks.getReadAheadKBPath("/dev/sda12");
        Assert.assertEquals(Paths.get("/sys/block/sda/queue/read_ahead_kb"), sdaDirectory);

        Path scsiDirectory = StartupChecks.getReadAheadKBPath("/dev/scsi1");
        Assert.assertEquals(Paths.get("/sys/block/scsi/queue/read_ahead_kb"), scsiDirectory);

        Path dirWithoutNumbers = StartupChecks.getReadAheadKBPath("/dev/sca");
        Assert.assertEquals(Paths.get("/sys/block/sca/queue/read_ahead_kb"), dirWithoutNumbers);

        Path invalidDir = StartupChecks.getReadAheadKBPath("/invaliddir/xpto");
        Assert.assertNull(invalidDir);
    }

    @Test
    public void maxMapCountCheck() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkMaxMapCount);
        startupChecks.verify(options);
    }

    private void copyLegacyNonSSTableFiles(Path targetDir) throws IOException
    {

        Path legacySSTableRoot = Paths.get(TEST_INVALID_LEGACY_SSTABLE_ROOT.getString(),
                                           "Keyspace1",
                                           "Standard1");
        for (String filename : new String[]{"Keyspace1-Standard1-ic-0-TOC.txt",
                                            "Keyspace1-Standard1-ic-0-Digest.sha1",
                                            "legacyleveled.json"})
            Files.copy(Paths.get(legacySSTableRoot.toString(), filename), targetDir.resolve(filename));
    }

    @Test
    public void testDataResurrectionCheck() throws Exception
    {
        DataResurrectionCheck check = new DataResurrectionCheck() {
            @Override
            List<String> getKeyspaces()
            {
                return singletonList("abc");
            }

            @Override
            List<TableGCPeriod> getTablesGcPeriods(String userKeyspace)
            {
                return singletonList(new TableGCPeriod("def", 10));
            }
        };

        Heartbeat heartbeat = new Heartbeat(Instant.ofEpochMilli(Clock.Global.currentTimeMillis()));
        heartbeat.serializeToJsonFile(heartbeatFile);

        Thread.sleep(15 * 1000);

        startupChecks.withTest(check);

        verifyFailure(startupChecks, "Invalid tables: abc.def");
    }

    @Test
    public void testKernelBug1057843Check() throws Exception
    {
        Assume.assumeTrue(DatabaseDescriptor.getCommitLogCompression() == null); // we would not be able to enable direct io otherwise
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.63.1-generic"), false);
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.64.1-generic"), true);
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.65.1-generic"), true);
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.66.1-generic"), false);
        testKernelBug1057843Check("tmpfs", DiskAccessMode.direct, new Semver("6.1.64.1-generic"), false);
        testKernelBug1057843Check("ext4", DiskAccessMode.mmap, new Semver("6.1.64.1-generic"), false);
    }

    private <R> void withPathOverriddingFileSystem(Map<String, String> pathOverrides, Callable<? extends R> callable) throws Exception
    {
        Map<String, FileStore> fileStores = Set.copyOf(pathOverrides.values()).stream().collect(Collectors.toMap(s -> s, s -> {
            FileStore fs = mock(FileStore.class);
            when(fs.type()).thenReturn(s);
            return fs;
        }));
        FileSystem savedFileSystem = File.unsafeGetFilesystem();
        try
        {
            ForwardingFileSystemProvider fsp = new ForwardingFileSystemProvider(savedFileSystem.provider())
            {
                @Override
                public FileStore getFileStore(Path path) throws IOException
                {
                    String override = pathOverrides.get(path.toString());
                    if (override != null)
                        return fileStores.get(override);

                    return super.getFileStore(path);
                }
            };

            ForwardingFileSystem fs = new ForwardingFileSystem(File.unsafeGetFilesystem())
            {
                private final FileSystem thisFileSystem = this;

                @Override
                public FileSystemProvider provider()
                {
                    return fsp;
                }

                @Override
                protected Path wrap(Path p)
                {
                    return new ForwardingPath(p)
                    {
                        @Override
                        public FileSystem getFileSystem()
                        {
                            return thisFileSystem;
                        }
                    };
                }
            };
            File.unsafeSetFilesystem(fs);
            callable.call();
        }
        finally
        {
            File.unsafeSetFilesystem(savedFileSystem);
        }
    }

    private void testKernelBug1057843Check(String fsType, DiskAccessMode diskAccessMode, Semver kernelVersion, boolean expectToFail) throws Exception
    {
        String commitLogLocation = Files.createTempDirectory("testKernelBugCheck").toString();

        String savedCommitLogLocation = DatabaseDescriptor.getCommitLogLocation();
        DiskAccessMode savedCommitLogWriteDiskAccessMode = DatabaseDescriptor.getCommitLogWriteDiskAccessMode();
        Semver savedKernelVersion = FBUtilities.getKernelVersion();
        try
        {
            DatabaseDescriptor.setCommitLogLocation(commitLogLocation);
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(diskAccessMode);
            DatabaseDescriptor.initializeCommitLogDiskAccessMode();
            assertThat(DatabaseDescriptor.getCommitLogWriteDiskAccessMode()).isEqualTo(diskAccessMode);
            FBUtilities.setKernelVersionSupplier(() -> kernelVersion);
            withPathOverriddingFileSystem(Map.of(commitLogLocation, fsType), () -> {
                if (expectToFail)
                    assertThatExceptionOfType(StartupException.class).isThrownBy(() -> StartupChecks.checkKernelBug1057843.execute(options));
                else
                    StartupChecks.checkKernelBug1057843.execute(options);
                return null;
            });
        }
        finally
        {
            DatabaseDescriptor.setCommitLogLocation(savedCommitLogLocation);
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(savedCommitLogWriteDiskAccessMode);
            DatabaseDescriptor.initializeCommitLogDiskAccessMode();
            FBUtilities.setKernelVersionSupplier(() -> savedKernelVersion);
        }
    }

    private void copyInvalidLegacySSTables(Path targetDir) throws IOException
    {
        File legacySSTableRoot = new File(Paths.get(TEST_INVALID_LEGACY_SSTABLE_ROOT.getString(),
                                                    "Keyspace1",
                                                    "Standard1"));
        for (File f : legacySSTableRoot.tryList())
            Files.copy(f.toPath(), targetDir.resolve(f.name()));

    }

    private void verifyFailure(StartupChecks tests, String message)
    {
        try
        {
            tests.verify(options);
            fail("Expected a startup exception but none was thrown");
        }
        catch (StartupException e)
        {
            assertTrue(e.getMessage().contains(message));
        }
    }
}
