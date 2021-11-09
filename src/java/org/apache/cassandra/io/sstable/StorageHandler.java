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

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.REMOTE_STORAGE_HANDLER;

/**
 * The handler of the storage of sstables, and possibly other files such as txn logs.
 * <p/>
 * If sstables are stored on the local disk, then this handler is a thin wrapper of {@link Directories.SSTableLister},
 * but for sstables stored remotely, for example on S3, then the handler may need to perform more
 * work, such as selecting only part of the remote sstables available, or adding new ones when offline compaction
 * has run. This behaviour can be implemented in a sub-class that can be set with {@link #remoteStorageHandler}.
 * <p/>
 * Local sytem tables will always use the default storage handler, {@link DefaultStorageHandler}
 * whereas user tables will use {@link #remoteStorageHandler} if one is set, or the default handler if none
 * has been set.
 * <p/>
 */
public abstract class StorageHandler
{
    private final static String remoteStorageHandler = REMOTE_STORAGE_HANDLER.getString();

    public enum ReloadReason
    {
        /** New nodes joined or left */
        TOPOLOGY_CHANGED,
        /** Data was truncated */
        TRUNCATION,
        /** Ordinary periodic reload */
        PERIODIC,
        /** Data was replayed either from the commit log or a batch log */
        DATA_REPLAYED,
        /** When repair task started */
        REPAIR,
        /** A request over forced by users to reload. */
        USER_REQUESTED,
    }

    protected final TableMetadataRef metadata;
    protected final Directories directories;
    protected final Tracker dataTracker;

    public StorageHandler(TableMetadataRef metadata, Directories directories, Tracker dataTracker)
    {
        Preconditions.checkNotNull(directories, "Directories should not be null");

        this.metadata = metadata;
        this.directories = directories;
        this.dataTracker = dataTracker;
    }

    /**
     * @return true if the node is ready to serve data for this table. This means that the
     *         node is not bootstrapping and that no data may be missing, e.g. if sstables are
     *         being downloaded from remote storage or streamed from other nodes then isReady()
     *         would return false. Generally, user read queries should not succeed if this method
     *         returns false.
     */
    public abstract boolean isReady();

    /**
     * Load the initial sstables into the tracker that was passed in to the constructor.
     *
     * @return the sstables that were loaded
     */
    public abstract Collection<SSTableReader> loadInitialSSTables();

    /**
     * Reload any sstables that may have been created and not yet loaded. This is normally
     * a no-op for the default local storage, but for remote storage implementations it
     * signals that sstables need to be refreshed.
     *
     * @return the sstables that were loaded
     */
    public abstract Collection<SSTableReader> reloadSSTables(ReloadReason reason);

    /**
     * This method determines if the backing storage handles is remote storage
     * <p/>
     * @return true if storage handler is remote
     */
    public abstract boolean isRemote();

    /**
     * This method will run the operation specified by the {@link Runnable} passed it
     * whilst guaranteeing the guarantees that no sstable will be loaded or unloaded
     * whilst this operation is running, by waiting for in-progress operation to complete.
     * In other words, the storage handler must not change the status of the tracker,
     * or try to load any sstable as long as this operation is executing.
     *
     * @param runnable the operation to execute.
     */
    public abstract void runWithReloadingDisabled(Runnable runnable);

    /**
     * Called when the CFS is unloaded, this needs to perform any cleanup.
     */
    public abstract void unload();

    public static StorageHandler create(TableMetadataRef metadata, Directories directories, Tracker dataTracker)
    {
        // local keyspaces are always local so don't use the remote handler even if it has been configured
        if (remoteStorageHandler == null || Schema.isKeyspaceWithLocalStrategy(metadata.keyspace))
            return new DefaultStorageHandler(metadata, directories, dataTracker);

        Class<StorageHandler> factoryClass =  FBUtilities.classForName(remoteStorageHandler, "Remote storage handler");

        try
        {
            return factoryClass.getConstructor(TableMetadataRef.class, Directories.class, Tracker.class)
                    .newInstance(metadata, directories, dataTracker);
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e)
        {
            throw new ConfigurationException("Unable to find correct constructor for " + remoteStorageHandler, e);
        }
    }
}
