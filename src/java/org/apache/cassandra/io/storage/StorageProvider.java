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

package org.apache.cassandra.io.storage;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_STORAGE_PROVIDER;

/**
 * The storage provider is used to support directory creation and remote/local conversion for remote storage.
 * The default implementation {@link DefaultProvider} is based on local file system.
 */
public interface StorageProvider
{
    Logger logger = LoggerFactory.getLogger(StorageProvider.class);

    StorageProvider instance = !CUSTOM_STORAGE_PROVIDER.isPresent()
        ? new DefaultProvider()
        : FBUtilities.construct(CUSTOM_STORAGE_PROVIDER.getString(), "storage provider");

    enum DirectoryType
    {
        DATA("data_file_directories"),
        LOCAL_SYSTEM_DATA("local_system_data_file_directories"),
        COMMITLOG("commit_log_directory"),
        HINTS("hints_directory"),
        SAVED_CACHES("saved_caches_directory"),
        CDC("cdc_raw_directory"),
        SNAPSHOT("snapshot_directory"),
        NODES("nodes_local_directory"),
        LOG_TRANSACTION("log_transaction_directory"),
        LOGS("logs_directory"),
        TEMP("temp_directory"),
        OTHERS("other_directory");

        final String name;

        final boolean readable;
        final boolean writable;

        DirectoryType(String name)
        {
            this.name = name;
            this.readable = true;
            this.writable = true;
        }
    }

    /**
     * @return local path if given path is remote path, otherwise returns itself
     */
    File getLocalPath(File path);

    /**
     * update the given path with open options for the sstable components
     */
    File withOpenOptions(File ret, Component component);

    /**
     * Create data directories for given table
     *
     * @param ksMetadata The keyspace metadata, can be null. This is used when schema metadata is
     *                   not available in {@link SchemaManager}, eg. CNDB backup & restore
     * @param keyspaceName the name of the keyspace
     * @param dirs current local data directories
     * @return data directories that are created
     */
    Directories.DataDirectory[] createDataDirectories(@Nullable KeyspaceMetadata ksMetadata, String keyspaceName, Directories.DataDirectory[] dirs);

    /**
     * Create directory for the given path and type, either locally or remotely if any remote storage parameters are passed in.
     *
     * @param dir the directory absolute path to create
     * @param type the type of directory to create
     * @return the actual directory path, which can be either local or remote; or null if directory can't be created
     */
    File createDirectory(String dir, DirectoryType type);

    class DefaultProvider implements StorageProvider
    {
        @Override
        public File getLocalPath(File path)
        {
            return path;
        }

        @Override
        public File withOpenOptions(File ret, Component component)
        {
            return ret;
        }

        @Override
        public Directories.DataDirectory[] createDataDirectories(@Nullable KeyspaceMetadata ksMetadata, String keyspaceName, Directories.DataDirectory[] dirs)
        {
            // data directories are already created in DatabadeDescriptor#createAllDirectories
            return dirs;
        }

        @Override
        public File createDirectory(String dir, DirectoryType type)
        {
            File ret = new File(dir);
            PathUtils.createDirectoriesIfNotExists(ret.toPath());
            return ret;
        }
    }
}
