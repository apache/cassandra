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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.SyncUtil;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

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

    private final Map<HintsDescriptor, InputPosition> dispatchPositions;
    private final Deque<HintsDescriptor> dispatchDequeue;
    private final Queue<HintsDescriptor> corruptedFiles;
    private final Map<HintsDescriptor, Long> hintsExpirations;

    // last timestamp used in a descriptor; make sure to not reuse the same timestamp for new descriptors.
    private volatile long lastUsedTimestamp;
    private volatile HintsWriter hintsWriter;

    private HintsStore(UUID hostId, File hintsDirectory, ImmutableMap<String, Object> writerParams, List<HintsDescriptor> descriptors)
    {
        this.hostId = hostId;
        this.hintsDirectory = hintsDirectory;
        this.writerParams = writerParams;

        dispatchPositions = new ConcurrentHashMap<>();
        dispatchDequeue = new ConcurrentLinkedDeque<>(descriptors);
        corruptedFiles = new ConcurrentLinkedQueue<>();
        hintsExpirations = new ConcurrentHashMap<>();

        //noinspection resource
        lastUsedTimestamp = descriptors.stream().mapToLong(d -> d.timestamp).max().orElse(0L);
    }

    static HintsStore create(UUID hostId, File hintsDirectory, ImmutableMap<String, Object> writerParams, List<HintsDescriptor> descriptors)
    {
        descriptors.sort((d1, d2) -> Long.compare(d1.timestamp, d2.timestamp));
        return new HintsStore(hostId, hintsDirectory, writerParams, descriptors);
    }

    @VisibleForTesting
    int getDispatchQueueSize()
    {
        return dispatchDequeue.size();
    }

    @VisibleForTesting
    int getHintsExpirationsMapSize()
    {
        return hintsExpirations.size();
    }

    InetAddressAndPort address()
    {
        return StorageService.instance.getEndpointForHostId(hostId);
    }

    @Nullable
    PendingHintsInfo getPendingHintsInfo()
    {
        Iterator<HintsDescriptor> descriptors = dispatchDequeue.iterator();
        int queueSize = 0;
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        while (descriptors.hasNext())
        {
            HintsDescriptor descriptor = descriptors.next();
            minTimestamp = Math.min(minTimestamp, descriptor.timestamp);
            maxTimestamp = Math.max(maxTimestamp, descriptor.timestamp);
            queueSize++;
        }

        if (queueSize == 0)
            return null;
        return new PendingHintsInfo(hostId, queueSize, minTimestamp, maxTimestamp);
    }

    boolean isLive()
    {
        InetAddressAndPort address = address();
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

        while ((descriptor = corruptedFiles.poll()) != null)
        {
            cleanUp(descriptor);
            delete(descriptor);
        }
    }

    void deleteExpiredHints(long now)
    {
        deleteHints(it -> hasExpired(it, now));
    }

    private boolean hasExpired(HintsDescriptor descriptor, long now)
    {
        Long cachedExpiresAt = hintsExpirations.get(descriptor);
        if (null != cachedExpiresAt)
            return cachedExpiresAt <= now;

        File hintFile = new File(hintsDirectory, descriptor.fileName());
        // the file does not exist or if an I/O error occurs
        if (!hintFile.exists() || hintFile.lastModified() == 0)
            return false;

        // 'lastModified' can be considered as the upper bound of the hint creation time.
        // So the TTL upper bound of all hints in the file can be estimated by lastModified + maxGcgs of all tables
        long ttl = hintFile.lastModified() + Schema.instance.largestGcgs();
        hintsExpirations.put(descriptor, ttl);
        return ttl <= now;
    }

    private void deleteHints(Predicate<HintsDescriptor> predicate)
    {
        Set<HintsDescriptor> removeSet = new HashSet<>();
        try
        {
            for (HintsDescriptor descriptor : Iterables.concat(dispatchDequeue, corruptedFiles))
            {
                if (predicate.test(descriptor))
                {
                    cleanUp(descriptor);
                    delete(descriptor);
                    removeSet.add(descriptor);
                }
            }
        }
        finally // remove the already deleted hints from internal queues in case of exception
        {
            dispatchDequeue.removeAll(removeSet);
            corruptedFiles.removeAll(removeSet);
        }
    }

    void delete(HintsDescriptor descriptor)
    {
        File hintsFile = descriptor.file(hintsDirectory);
        if (hintsFile.tryDelete())
            logger.info("Deleted hint file {}", descriptor.fileName());
        else if (hintsFile.exists())
            logger.error("Failed to delete hint file {}", descriptor.fileName());
        else
            logger.info("Already deleted hint file {}", descriptor.fileName());

        //noinspection ResultOfMethodCallIgnored
        descriptor.checksumFile(hintsDirectory).tryDelete();
    }

    boolean hasFiles()
    {
        return !dispatchDequeue.isEmpty();
    }

    InputPosition getDispatchOffset(HintsDescriptor descriptor)
    {
        return dispatchPositions.get(descriptor);
    }

    void markDispatchOffset(HintsDescriptor descriptor, InputPosition inputPosition)
    {
        dispatchPositions.put(descriptor, inputPosition);
    }


    /**
     * @return the total size of all files belonging to the hints store, in bytes.
     */
    long getTotalFileSize()
    {
        long total = 0;
        for (HintsDescriptor descriptor : Iterables.concat(dispatchDequeue, corruptedFiles))
        {
            total += descriptor.file(hintsDirectory).length();
        }
        return total;
    }

    void cleanUp(HintsDescriptor descriptor)
    {
        dispatchPositions.remove(descriptor);
        hintsExpirations.remove(descriptor);
    }

    void markCorrupted(HintsDescriptor descriptor)
    {
        corruptedFiles.add(descriptor);
    }

    /**
     * @return a copy of the first {@link HintsDescriptor} in the queue for dispatch or {@code null} if queue is empty.
     */
    HintsDescriptor getFirstDescriptor()
    {
        return dispatchDequeue.peekFirst();
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
        lastUsedTimestamp = Math.max(currentTimeMillis(), lastUsedTimestamp + 1);
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
