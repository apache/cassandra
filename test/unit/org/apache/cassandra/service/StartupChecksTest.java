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
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.vdurmont.semver4j.Semver;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.StartupChecksConfiguration;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.compress.AbstractCompressionProvider;
import org.apache.cassandra.io.compress.CompressorRegistry;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.filesystem.ForwardingFileSystem;
import org.apache.cassandra.io.filesystem.ForwardingFileSystemProvider;
import org.apache.cassandra.io.filesystem.ForwardingPath;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.DataResurrectionCheck.Heartbeat;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.CompressionProviderHelper.CompatibleSnappyProvider;
import org.apache.cassandra.utils.CompressionProviderHelper.IncompatibleSnappyProvider;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SystemInfo;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INVALID_LEGACY_SSTABLE_ROOT;
import static org.apache.cassandra.io.util.FileUtils.createTempFile;
import static org.apache.cassandra.service.DataResurrectionCheck.HEARTBEAT_FILE_CONFIG_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StartupChecksTest
{
    static
    {
        // This test was failing because in the middle of file deletions in @Before hook, it happened that some
        // thread modified system.local table. Each change to system.local is immediately flushed to disk. Creation
        // of those new files when the directory was being deleted caused the test to fail occasionally.
        // The property below disables flushing system.local after each change.
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
    }

    StartupChecks startupChecks;
    Path sstableDir;
    File heartbeatFile;

    StartupChecksConfiguration options = new StartupChecksConfiguration(new StartupChecks().withDefaultTests(), new HashMap<>());

    @BeforeClass
    public static void setupServer()
    {
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

        heartbeatFile = createTempFile("cassandra-heartbeat-" + UUID.randomUUID(), "");

        options.enable("check_data_resurrection");
        options.getConfig("check_data_resurrection")
               .put(HEARTBEAT_FILE_CONFIG_PROPERTY, heartbeatFile.absolutePath());

        startupChecks = new StartupChecks();
    }

    @After
    public void tearDown() throws IOException
    {
        new File(sstableDir).deleteRecursive();
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
        copyInvalidLegacySSTables(snapshotDir);
        startupChecks.verify(options);

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
        assertEquals(Paths.get("/sys/block/sda/queue/read_ahead_kb"), sdaDirectory);

        Path scsiDirectory = StartupChecks.getReadAheadKBPath("/dev/scsi1");
        assertEquals(Paths.get("/sys/block/scsi/queue/read_ahead_kb"), scsiDirectory);

        Path dirWithoutNumbers = StartupChecks.getReadAheadKBPath("/dev/sca");
        assertEquals(Paths.get("/sys/block/sca/queue/read_ahead_kb"), dirWithoutNumbers);

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
        for (String filename : new String[]{ "Keyspace1-Standard1-ic-0-TOC.txt",
                                             "Keyspace1-Standard1-ic-0-Digest.sha1",
                                             "legacyleveled.json" })
            Files.copy(Paths.get(legacySSTableRoot.toString(), filename), targetDir.resolve(filename));
    }

    @Test
    public void testDataResurrectionCheck() throws Exception
    {
        DataResurrectionCheck check = new DataResurrectionCheck()
        {
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
    public void testDataResurrectionCheckLastModifiedFallback() throws Exception
    {
        DataResurrectionCheck check = new DataResurrectionCheck()
        {
            @Override
            List<String> getKeyspaces()
            {
                return singletonList("test_ks");
            }

            @Override
            List<TableGCPeriod> getTablesGcPeriods(String userKeyspace)
            {
                return singletonList(new TableGCPeriod("test_table", 10));
            }
        };

        int originalHintWindow = DatabaseDescriptor.getMaxHintWindow();
        try
        {
            DatabaseDescriptor.setMaxHintWindow(5 * 1000);

            // Empty file
            Files.write(heartbeatFile.toPath(), "".getBytes(StandardCharsets.UTF_8));
            Instant recentTimestamp = Instant.ofEpochMilli(Clock.Global.currentTimeMillis());
            Files.setLastModifiedTime(heartbeatFile.toPath(), FileTime.from(recentTimestamp));

            startupChecks.withTest(check);
            verifySuccess(startupChecks);
        }
        finally
        {
            DatabaseDescriptor.setMaxHintWindow(originalHintWindow);
        }
    }

    private void verifySuccess(StartupChecks tests)
    {
        try
        {
            tests.verify(options);
        }
        catch (StartupException e)
        {
            fail("Failed startup check with error: " + e.getMessage());
        }
    }

    @Test
    public void testKernelBug1057843Check() throws Exception
    {
        Assume.assumeTrue(DatabaseDescriptor.getCommitLogCompression() == null); // we would not be able to enable direct io otherwise
        Assume.assumeTrue("Skipping this test on non-Linux OS", FBUtilities.isLinux);
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.63.1-generic"), false);
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.64.1-generic"), true);
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.65.1-generic"), true);
        testKernelBug1057843Check("ext4", DiskAccessMode.direct, new Semver("6.1.66.1-generic"), false);
        testKernelBug1057843Check("tmpfs", DiskAccessMode.direct, new Semver("6.1.64.1-generic"), false);
        testKernelBug1057843Check("ext4", DiskAccessMode.mmap, new Semver("6.1.64.1-generic"), false);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testErrorneousCustomCheckFailsStartup()
    {
        // ServiceLoader instantiates providers lazily: a custom StartupCheck whose no-arg
        // constructor throws does NOT fail ServiceLoader.load(), it fails later, during
        // iteration, surfacing as a ServiceConfigurationError from the iterator. We model that
        // here by having the iterator throw on next(). withServiceLoaderTests() must catch the
        // error around the iteration loop and rethrow it as a ConfigurationException, rather than
        // letting it escape uncaught (which would be wrapped by applyStartupChecks() into a
        // misleading "Invalid configuration of startup_checks" failure).
        ServiceConfigurationError error = new ServiceConfigurationError("org.example.BadCheck could not be instantiated",
                                                                        new RuntimeException("Failure to instantiate"));

        Iterator<StartupCheck> failingIterator = mock(Iterator.class);
        when(failingIterator.hasNext()).thenReturn(true);
        when(failingIterator.next()).thenThrow(error);

        ServiceLoader<StartupCheck> loader = mock(ServiceLoader.class);
        doReturn(failingIterator).when(loader).iterator();

        try (MockedStatic<ServiceLoader> serviceLoader = Mockito.mockStatic(ServiceLoader.class))
        {
            serviceLoader.when(() -> ServiceLoader.load(StartupCheck.class)).thenReturn(loader);

            assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> new StartupChecks().withDefaultTests().withServiceLoaderTests())
                .withMessageContaining("Unable to get startup checks via ServiceLoader")
                .withCause(error);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExternalCheckIsLoaded() throws StartupException
    {
        StartupCheck externalCheck = spy(new StartupCheck()
        {
            @Override
            public String name()
            {
                return "my_custom_check";
            }

            @Override
            public void execute(StartupChecksConfiguration configuration)
            {

            }

            @Override
            public boolean isConfigurable()
            {
                return true;
            }

            @Override
            public boolean isDisabledByDefault()
            {
                return false;
            }
        });

        ServiceLoader<StartupCheck> loader = mock(ServiceLoader.class);
        doReturn(List.of(externalCheck).iterator()).when(loader).iterator();
        try (MockedStatic<ServiceLoader> serviceLoader = Mockito.mockStatic(ServiceLoader.class))
        {
            serviceLoader.when(() -> ServiceLoader.load(StartupCheck.class)).thenReturn(loader);

            StartupChecks checks = new StartupChecks().withDefaultTests().withServiceLoaderTests();

            StartupCheck myCustomCheck = checks.getCheck("my_custom_check");
            assertNotNull(myCustomCheck);

            StartupChecksConfiguration configuration = new StartupChecksConfiguration(checks, new HashMap<>());

            checks.verify(configuration);
            verify(externalCheck, times(1)).execute(configuration);
        }
    }

    @Test
    public void testLoadingCustomChecksWithNotUniqueNameIsForbidden()
    {
        StartupCheck externalCheck = spy(new StartupCheck()
        {
            @Override
            public String name()
            {
                return "my_custom_check";
            }

            @Override
            public void execute(StartupChecksConfiguration configuration)
            {

            }

            @Override
            public boolean isConfigurable()
            {
                return true;
            }

            @Override
            public boolean isDisabledByDefault()
            {
                return false;
            }
        });

        ServiceLoader<StartupCheck> loader = mock(ServiceLoader.class);

        // two times! We model loading of two checks with same name
        doReturn(List.of(externalCheck, externalCheck).iterator()).when(loader).iterator();

        try (MockedStatic<ServiceLoader> serviceLoader = Mockito.mockStatic(ServiceLoader.class))
        {
            serviceLoader.when(() -> ServiceLoader.load(StartupCheck.class)).thenReturn(loader);

            try
            {
                new StartupChecks().withDefaultTests().withServiceLoaderTests();
                fail("it should not be possible to specify two custom checks with same name");
            }
            catch (Throwable t)
            {
                assertEquals("There was an attempt to load custom startup checks with same name which is ambiguous: [my_custom_check]",
                             t.getMessage());
            }
        }
    }

    @Test
    public void testCustomCheckHasSameNameAsInBuiltCheck()
    {
        StartupCheck externalCheck = spy(new StartupCheck()
        {
            @Override
            public String name()
            {
                // for the sake of it being same as one of in-builts
                return StartupChecks.checkLz4Native.name();
            }

            @Override
            public void execute(StartupChecksConfiguration configuration)
            {

            }

            @Override
            public boolean isConfigurable()
            {
                return true;
            }

            @Override
            public boolean isDisabledByDefault()
            {
                return false;
            }
        });

        ServiceLoader<StartupCheck> loader = mock(ServiceLoader.class);

        // two times! We model loading of two checks with same name
        doReturn(List.of(externalCheck, externalCheck).iterator()).when(loader).iterator();

        try (MockedStatic<ServiceLoader> serviceLoader = Mockito.mockStatic(ServiceLoader.class))
        {
            serviceLoader.when(() -> ServiceLoader.load(StartupCheck.class)).thenReturn(loader);

            try
            {
                new StartupChecks().withDefaultTests().withServiceLoaderTests();
                fail("it should not be possible to specify a check with same name as in-built check");
            }
            catch (Throwable t)
            {
                assertEquals("There was an attempt to load custom startup checks with same name which is ambiguous: [" + StartupChecks.checkLz4Native.name() + ']',
                             t.getMessage());
            }
        }
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
        SystemInfo savedSystemInfo = FBUtilities.getSystemInfo();
        try
        {
            DatabaseDescriptor.setCommitLogLocation(commitLogLocation);
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(diskAccessMode);
            DatabaseDescriptor.initializeCommitLogDiskAccessMode();
            assertThat(DatabaseDescriptor.getCommitLogWriteDiskAccessMode()).isEqualTo(diskAccessMode);
            FBUtilities.setSystemInfoSupplier(() -> new SystemInfo()
            {
                @Override
                public Semver getKernelVersion()
                {
                    return kernelVersion;
                }
            });
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
            FBUtilities.setSystemInfoSupplier(() -> savedSystemInfo);
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

    @Test
    public void testFindDirectIOUnsupportedLocationsSkipsNonExistentDirs()
    {
        // Non-existent directories should be skipped, not added to unsupported list
        List<String> unsupported = StartupChecks.findDirectIOUnsupportedLocations(
        new String[]{ "/this/path/does/not/exist/for/testing" });
        assertThat(unsupported).isEmpty();
    }

    @Test
    public void testCompatibleCompressionProvider()
    {
        // This test will go through the smoke test verifing with a valid custom provider,
        // providing a compressor compatible with Snappy
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString());
        Map<String, ParameterizedClass> providerOptions = Map.of(
        SnappyCompressor.class.getSimpleName(),
        new ParameterizedClass(CompatibleSnappyProvider.class.getName(), params));

        CompressorRegistry.instance.reset();
        CompressorRegistry.instance.registerProviders(providerOptions);
        try
        {
            StartupChecks.checkCustomCompressionProviders.execute(options);
        }
        catch (Throwable t)
        {
            fail("This exception should not be thrown since the provider is compatible " +
                 "with the compressor it is supposed to create: " + t.getMessage());
        }
        finally
        {
            CompressorRegistry.instance.reset(); // only registry state touched — safe to reset
        }
    }

    @Test
    public void testIncompatibleCompressionProvider()
    {
        // This test is trying to simulate a failure scenario by providing a custom provider which is not compatible
        // with the compressor it is supposed to create. In this case, we are providing a compressor which is compatible
        // with Snappy, but we are registering it as provider for NoopCompressor.
        // The smoke test should fail and throw an exception alerting that using this provider may lead to data corruption.
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString());
        Map<String, ParameterizedClass> providerOptions = Map.of(
        SnappyCompressor.class.getSimpleName(),
        new ParameterizedClass(IncompatibleSnappyProvider.class.getName(), params));

        CompressorRegistry.instance.reset();
        CompressorRegistry.instance.registerProviders(providerOptions);
        try
        {
            assertThatThrownBy(() -> StartupChecks.checkCustomCompressionProviders.execute(options))
            .isInstanceOf(StartupException.class)
            .hasMessageContaining("The following custom compression providers failed smoke test");
        }
        finally
        {
            CompressorRegistry.instance.reset();
        }
    }
}
