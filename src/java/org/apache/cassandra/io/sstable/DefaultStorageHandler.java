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

package org.apache.cassandra.io.sstable;

import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;

/**
 * The default storage handler, used when sstables are stored on the local file system.
 */
public class DefaultStorageHandler extends StorageHandler
{
    public DefaultStorageHandler(TableMetadataRef metadata, Directories directories, Tracker dataTracker)
    {
        super(metadata, directories, dataTracker);
    }

    @Override
    public boolean isReady()
    {
        return !StorageService.instance.isBootstrapMode();
    }

    @Override
    public Collection<SSTableReader> loadInitialSSTables()
    {
        Directories.SSTableLister sstableFiles = directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
        Collection<SSTableReader> sstables = SSTableReader.openAll(sstableFiles.list().entrySet(), metadata);
        dataTracker.addInitialSSTablesWithoutUpdatingSize(sstables);
        return sstables;
    }

    @Override
    public Collection<SSTableReader> reloadSSTables(ReloadReason reason)
    {
        // no op for local storage
        return Collections.emptySet();
    }

    @Override
    public void unload()
    {
        // no op for local storage
    }

    @Override
    public boolean isRemote()
    {
        return false;
    }

    @Override
    public void runWithReloadingDisabled(Runnable runnable)
    {
        // by default no sstables are loaded, so we just need to execute the runnable
        runnable.run();
    }
}
