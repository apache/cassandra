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

import java.nio.file.Files;
import java.nio.file.Path;

import javax.annotation.Nullable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.assertj.core.api.Assertions;

public class CustomStorageProviderTest
{
    static
    {
        System.setProperty("cassandra.custom_storage_provider",
                           CustomStorageProviderTest.TestCustomStorageProvider.class.getName());
    }

    private static final String KS = "ks";
    private static final String TABLE = "cf";
    private static final String ASSERT_MESSAGE = "Should throw if the directory exists";
    private static File tempDataDir;

    private TableMetadata tableMetadata;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
    }

    @Before
    public void beforeEach()
    {
        tempDataDir = FileUtils.createTempFile("cassandra", "unittest");
        tempDataDir.tryDelete(); // hack to create a temp dir
        tempDataDir.tryCreateDirectory();

        tableMetadata = TableMetadata.builder(KS, TABLE)
                                     .addPartitionKeyColumn("thekey", UTF8Type.instance)
                                     .addClusteringColumn("thecolumn", UTF8Type.instance)
                                     .build();
    }

    @AfterClass
    public static void afterClass()
    {
        FileUtils.deleteRecursive(tempDataDir);
        System.clearProperty("cassandra.custom_storage_provider");
    }

    private static Directories.DataDirectory[] toDataDirectories(File location)
    {
        return new Directories.DataDirectory[]{ new Directories.DataDirectory(location) };
    }

    @Test
    public void testCustomStorageProvider()
    {
        Assertions.assertThat(StorageProvider.instance).isInstanceOf(TestCustomStorageProvider.class);
        ((TestCustomStorageProvider) StorageProvider.instance).useCustomBehavior = true;

        File newDir = new File(tempDataDir, "testCustomStorageProvider");
        new Directories(tableMetadata, toDataDirectories(newDir));
        Assert.assertTrue(Files.exists(newDir.toPath()));
    }

    @Test
    public void testDirectoriesMock()
    {
        Assertions.assertThat(StorageProvider.instance).isInstanceOf(TestCustomStorageProvider.class);
        ((TestCustomStorageProvider) StorageProvider.instance).useCustomBehavior = true;

        File newDir = new File(tempDataDir, "testDirectoriesMock");
        new Directories(tableMetadata, toDataDirectories(newDir));

        // Call to normal constuctor on an existing directory.
        Assertions.assertThatThrownBy(() -> new Directories(tableMetadata, toDataDirectories(newDir)))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessage(ASSERT_MESSAGE);

        // Call to mock constructor, which doesn't call StorageProvider.instance, on an existing directory.
        new Directories(tableMetadata, newDir.toPath());
    }

    public static class TestCustomStorageProvider extends StorageProvider.DefaultProvider
    {
        // Should be false during initialization, so the defaul behaviour is used
        boolean useCustomBehavior = false;

        /**
         * This method is called from Directories constuctor by accessing StorageProvider.instance,
         * which is expected to be set to this custom storage provider.
         * The method is overriden with custom behavior that the same directory cannot be created twice.
         */
        @Override
        public Directories.DataDirectory[] createDataDirectories(@Nullable KeyspaceMetadata ksMetadata,
                                                                 String keyspaceName,
                                                                 Directories.DataDirectory[] dirs)
        {
            if (!useCustomBehavior)
                return super.createDataDirectories(ksMetadata, keyspaceName, dirs);

            for (Directories.DataDirectory d : dirs)
            {
                Path dir = d.location.toPath();
                if (Files.exists(dir))
                    throw new IllegalArgumentException(ASSERT_MESSAGE);
                PathUtils.createDirectoriesIfNotExists(dir);
            }
            return dirs;
        }
    }
}
