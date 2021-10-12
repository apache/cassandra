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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.cassandra.io.util.File;
import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StartupChecksTest
{
    public static final String INVALID_LEGACY_SSTABLE_ROOT_PROP = "invalid-legacy-sstable-root";
    StartupChecks startupChecks;
    Path sstableDir;

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
            FileUtils.deleteRecursive(dataDir);

        File dataDir = new File(DatabaseDescriptor.getAllDataFileLocations()[0]);
        sstableDir = Paths.get(dataDir.absolutePath(), "Keyspace1", "Standard1");
        Files.createDirectories(sstableDir);

        startupChecks = new StartupChecks();
    }

    @After
    public void tearDown() throws IOException
    {
        FileUtils.deleteRecursive(new File(sstableDir));
    }

    @Test
    public void failStartupIfInvalidSSTablesFound() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyInvalidLegacySSTables(sstableDir);

        verifyFailure(startupChecks, "Detected unreadable sstables");

        // we should ignore invalid sstables in a snapshots directory
        FileUtils.deleteRecursive(new File(sstableDir));
        Path snapshotDir = sstableDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);
        copyInvalidLegacySSTables(snapshotDir); startupChecks.verify();

        // and in a backups directory
        FileUtils.deleteRecursive(new File(sstableDir));
        Path backupDir = sstableDir.resolve("backups");
        Files.createDirectories(backupDir);
        copyInvalidLegacySSTables(backupDir);
        startupChecks.verify();
    }

    @Test
    public void compatibilityCheckIgnoresNonDbFiles() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkSSTablesFormat);

        copyLegacyNonSSTableFiles(sstableDir);
        assertFalse(new File(sstableDir).tryList().length == 0);

        startupChecks.verify();
    }

    @Test
    public void maxMapCountCheck() throws Exception
    {
        startupChecks = startupChecks.withTest(StartupChecks.checkMaxMapCount);
        startupChecks.verify();
    }

    private void copyLegacyNonSSTableFiles(Path targetDir) throws IOException
    {

        Path legacySSTableRoot = Paths.get(System.getProperty(INVALID_LEGACY_SSTABLE_ROOT_PROP),
                                          "Keyspace1",
                                          "Standard1");
        for (String filename : new String[]{"Keyspace1-Standard1-ic-0-TOC.txt",
                                            "Keyspace1-Standard1-ic-0-Digest.sha1",
                                            "legacyleveled.json"})
            Files.copy(Paths.get(legacySSTableRoot.toString(), filename), targetDir.resolve(filename));
    }

    private void copyInvalidLegacySSTables(Path targetDir) throws IOException
    {
        File legacySSTableRoot = new File(Paths.get(System.getProperty(INVALID_LEGACY_SSTABLE_ROOT_PROP),
                                           "Keyspace1",
                                           "Standard1"));
        for (File f : legacySSTableRoot.tryList())
            Files.copy(f.toPath(), targetDir.resolve(f.name()));

    }

    private void verifyFailure(StartupChecks tests, String message)
    {
        try
        {
            tests.verify();
            fail("Expected a startup exception but none was thrown");
        }
        catch (StartupException e)
        {
            assertTrue(e.getMessage().contains(message));
        }
    }
}
