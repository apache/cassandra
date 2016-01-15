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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.SyncUtil;

/**
 * Encapsulates the state of a peer's hints: the queue of hints files for dispatch, and the current writer (if any).
 *
 * The queue for dispatch is multi-threading safe.
 *
 * The writer MUST only be accessed by {@link HintsWriteExecutor}.
 */
final class HintsStore
{
    private static final Logger logger = LoggerFactory.getLogger(HintsStore.class);

    public final UUID hostId;
    private final File hintsDirectory;
    private final ImmutableMap<String, Object> writerParams;

    private final Map<HintsDescriptor, Long> dispatchOffsets;
    private final Deque<HintsDescriptor> dispatchDequeue;
    private final Queue<HintsDescriptor> blacklistedFiles;

    // last timestamp used in a descriptor; make sure to not reuse the same timestamp for new descriptors.
    private volatile long lastUsedTimestamp;
    private volatile HintsWriter hintsWriter;

    private HintsStore(UUID hostId, File hintsDirectory, ImmutableMap<String, Object> writerParams, List<HintsDescriptor> descriptors)
    {
        this.hostId = hostId;
        this.hintsDirectory = hintsDirectory;
        this.writerParams = writerParams;

        dispatchOffsets = new ConcurrentHashMap<>();
        dispatchDequeue = new ConcurrentLinkedDeque<>(descriptors);
        blacklistedFiles = new ConcurrentLinkedQueue<>();

        //noinspection resource
        lastUsedTimestamp = descriptors.stream().mapToLong(d -> d.timestamp).max().orElse(0L);
    }

    static HintsStore create(UUID hostId, File hintsDirectory, ImmutableMap<String, Object> writerParams, List<HintsDescriptor> descriptors)
    {
        descriptors.sort((d1, d2) -> Long.compare(d1.timestamp, d2.timestamp));
        return new HintsStore(hostId, hintsDirectory, writerParams, descriptors);
    }

    InetAddress address()
    {
        return StorageService.instance.getEndpointForHostId(hostId);
    }

    boolean isLive()
    {
        InetAddress address = address();
        return address != null && FailureDetector.instance.isAlive(address);
    }

    HintsDescriptor poll()
    {
        return dispatchDequeue.poll();
    }

    void offerFirst(HintsDescriptor descriptor)
    {
        dispatchDequeue.offerFirst(descriptor);
    }

    void offerLast(HintsDescriptor descriptor)
    {
        dispatchDequeue.offerLast(descriptor);
    }

    void deleteAllHints()
    {
        HintsDescriptor descriptor;
        while ((descriptor = poll()) != null)
        {
            cleanUp(descriptor);
            delete(descriptor);
        }

        while ((descriptor = blacklistedFiles.poll()) != null)
        {
            cleanUp(descriptor);
            delete(descriptor);
        }
    }

    void delete(HintsDescriptor descriptor)
    {
        File hintsFile = new File(hintsDirectory, descriptor.fileName());
        if (hintsFile.delete())
            logger.info("Deleted hint file {}", descriptor.fileName());
        else
            logger.error("Failed to delete hint file {}", descriptor.fileName());

        //noinspection ResultOfMethodCallIgnored
        new File(hintsDirectory, descriptor.checksumFileName()).delete();
    }

    boolean hasFiles()
    {
        return !dispatchDequeue.isEmpty();
    }

    Optional<Long> getDispatchOffset(HintsDescriptor descriptor)
    {
        return Optional.ofNullable(dispatchOffsets.get(descriptor));
    }

    void markDispatchOffset(HintsDescriptor descriptor, long mark)
    {
        dispatchOffsets.put(descriptor, mark);
    }

    void cleanUp(HintsDescriptor descriptor)
    {
        dispatchOffsets.remove(descriptor);
    }

    void blacklist(HintsDescriptor descriptor)
    {
        blacklistedFiles.add(descriptor);
    }

    /*
     * Methods dealing with HintsWriter.
     *
     * All of these, with the exception of isWriting(), are for exclusively single-threaded use by HintsWriteExecutor.
     */

    boolean isWriting()
    {
        return hintsWriter != null;
    }

    HintsWriter getOrOpenWriter()
    {
        if (hintsWriter == null)
            hintsWriter = openWriter();
        return hintsWriter;
    }

    HintsWriter getWriter()
    {
        return hintsWriter;
    }

    private HintsWriter openWriter()
    {
        lastUsedTimestamp = Math.max(System.currentTimeMillis(), lastUsedTimestamp + 1);
        HintsDescriptor descriptor = new HintsDescriptor(hostId, lastUsedTimestamp, writerParams);

        try
        {
            return HintsWriter.create(hintsDirectory, descriptor);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, descriptor.fileName());
        }
    }

    void closeWriter()
    {
        if (hintsWriter != null)
        {
            hintsWriter.close();
            offerLast(hintsWriter.descriptor());
            hintsWriter = null;
            SyncUtil.trySyncDir(hintsDirectory);
        }
    }

    void fsyncWriter()
    {
        if (hintsWriter != null)
            hintsWriter.fsync();
    }
}
