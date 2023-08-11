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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.SyncUtil;

import static java.util.stream.Collectors.groupingBy;

/**
 * A simple catalog for easy host id -> {@link HintsStore} lookup and manipulation.
 */
final class HintsCatalog
{
    private static final Logger logger = LoggerFactory.getLogger(HintsCatalog.class);

    private final File hintsDirectory;
    private final Map<UUID, HintsStore> stores;
    private final ImmutableMap<String, Object> writerParams;

    private HintsCatalog(File hintsDirectory, ImmutableMap<String, Object> writerParams, Map<UUID, List<HintsDescriptor>> descriptors)
    {
        this.hintsDirectory = hintsDirectory;
        this.writerParams = writerParams;
        this.stores = new ConcurrentHashMap<>();

        for (Map.Entry<UUID, List<HintsDescriptor>> entry : descriptors.entrySet())
            stores.put(entry.getKey(), HintsStore.create(entry.getKey(), hintsDirectory, writerParams, entry.getValue()));
    }

    /**
     * Loads hints stores from a given directory.
     */
    static HintsCatalog load(File hintsDirectory, ImmutableMap<String, Object> writerParams)
    {
        try(Stream<Path> list = Files.list(hintsDirectory.toPath()))
        {
            Map<UUID, List<HintsDescriptor>> stores =
                     list
                     .filter(HintsDescriptor::isHintFileName)
                     .map(HintsDescriptor::readFromFileQuietly)
                     .filter(Optional::isPresent)
                     .map(Optional::get)
                     .collect(groupingBy(h -> h.hostId));
            return new HintsCatalog(hintsDirectory, writerParams, stores);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, hintsDirectory);
        }
    }

    Stream<HintsStore> stores()
    {
        return stores.values().stream();
    }

    void maybeLoadStores(Iterable<UUID> hostIds)
    {
        for (UUID hostId : hostIds)
            get(hostId);
    }

    HintsStore get(UUID hostId)
    {
        // we intentionally don't just return stores.computeIfAbsent() because it's expensive compared to simple get(),
        // and in this case would also allocate for the capturing lambda; the method is on a really hot path
        HintsStore store = stores.get(hostId);
        return store == null
             ? stores.computeIfAbsent(hostId, (id) -> HintsStore.create(id, hintsDirectory, writerParams, Collections.emptyList()))
             : store;
    }

    @Nullable
    HintsStore getNullable(UUID hostId)
    {
        return stores.get(hostId);
    }

    /**
     * Delete all hints for all host ids.
     *
     * Will not delete the files that are currently being dispatched, or written to.
     */
    void deleteAllHints()
    {
        stores.keySet().forEach(this::deleteAllHints);
    }

    /**
     * Delete all hints for the specified host id.
     *
     * Will not delete the files that are currently being dispatched, or written to.
     */
    void deleteAllHints(UUID hostId)
    {
        HintsStore store = stores.get(hostId);
        if (store != null)
            store.deleteAllHints();
    }

    /**
     * @return true if at least one of the stores has a file pending dispatch
     */
    boolean hasFiles()
    {
        return stores().anyMatch(HintsStore::hasFiles);
    }

    void exciseStore(UUID hostId)
    {
        deleteAllHints(hostId);
        stores.remove(hostId);
    }

    void fsyncDirectory()
    {
        int fd = NativeLibrary.tryOpenDirectory(hintsDirectory.absolutePath());
        if (fd != -1)
        {
            try
            {
                SyncUtil.trySync(fd);
                NativeLibrary.tryCloseFD(fd);
            }
            catch (FSError e) // trySync failed
            {
                logger.error("Unable to sync directory {}", hintsDirectory.absolutePath(), e);
                FileUtils.handleFSErrorAndPropagate(e);
            }
        }
        else if (DatabaseDescriptor.isClientInitialized())
        {
            logger.warn("Unable to open hint directory using Native library. Skipping sync.");
        }
        else
        {
            if (SyncUtil.SKIP_SYNC)
                return;
            logger.error("Unable to open directory {}", hintsDirectory.absolutePath());
            FileUtils.handleFSErrorAndPropagate(new FSWriteError(new IOException(String.format("Unable to open hint directory %s", hintsDirectory.absolutePath())), hintsDirectory.absolutePath()));
        }
    }

    ImmutableMap<String, Object> getWriterParams()
    {
        return writerParams;
    }
}
