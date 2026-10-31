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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Expired-hints cleanup and hint dispatch run on different threads and both end up deleting hints files.
 * The dispatch side claims a file by polling its descriptor off the store; the cleanup side has to respect
 * that claim, otherwise it unlinks a file that a dispatcher is about to read (CASSANDRA-21217).
 */
@RunWith(BMUnitRunner.class)
public class HintsStoreCleanupRaceTest
{
    private static final String KEYSPACE = "hints_store_cleanup_race_test";
    private static final String TABLE = "table";

    private static final long AWAIT_MINUTES = 1;

    // Rendezvous between the cleanup thread, parked inside HintsStore, and the thread racing it.
    static volatile CountDownLatch cleanupReachedUnlink;
    static volatile CountDownLatch dispatchPolled;
    static volatile CountDownLatch cleanupFinishedUnlink;
    static volatile CountDownLatch losingCleanupCachedExpiry;
    static volatile CountDownLatch winningCleanupDone;

    static volatile AtomicBoolean firstPollRecorded;
    static volatile AtomicBoolean losingCleanupParked;
    static volatile AtomicReference<HintsDescriptor> claimedByDispatch;

    static volatile Thread cleanupThread;
    static volatile Thread losingCleanupThread;
    static volatile boolean armed;

    private File directory;
    private UUID hostId;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Before
    public void setUp() throws IOException
    {
        directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();
        hostId = UUID.randomUUID();

        cleanupReachedUnlink = new CountDownLatch(1);
        dispatchPolled = new CountDownLatch(1);
        cleanupFinishedUnlink = new CountDownLatch(1);
        losingCleanupCachedExpiry = new CountDownLatch(1);
        winningCleanupDone = new CountDownLatch(1);
        firstPollRecorded = new AtomicBoolean();
        losingCleanupParked = new AtomicBoolean();
        claimedByDispatch = new AtomicReference<>();
        cleanupThread = null;
        losingCleanupThread = null;
        armed = false;
    }

    public static void pauseCleanupBeforeUnlink()
    {
        if (!armed || Thread.currentThread() != cleanupThread)
            return;

        cleanupReachedUnlink.countDown();
        await(dispatchPolled);
    }

    public static void releaseDispatchAfterUnlink()
    {
        if (!armed || Thread.currentThread() != cleanupThread)
            return;

        cleanupFinishedUnlink.countDown();
    }

    public static void recordDispatchPoll(Object descriptor)
    {
        if (!armed || Thread.currentThread() == cleanupThread)
            return;

        if (!firstPollRecorded.compareAndSet(false, true))
            return;

        claimedByDispatch.set((HintsDescriptor) descriptor);
        dispatchPolled.countDown();

        // Hold the dispatcher until the cleanup has finished unlinking, so that the outcome does not depend on
        // which of the two threads happens to reach the file first.
        if (descriptor != null)
            await(cleanupFinishedUnlink);
    }

    public static void pauseLosingCleanupBeforeCachingExpiry()
    {
        if (!armed || Thread.currentThread() != losingCleanupThread)
            return;

        if (!losingCleanupParked.compareAndSet(false, true))
            return;

        losingCleanupCachedExpiry.countDown();
        await(winningCleanupDone);
    }

    private static void await(CountDownLatch latch)
    {
        try
        {
            if (!latch.await(AWAIT_MINUTES, TimeUnit.MINUTES))
                throw new AssertionError("Timed out waiting for the other thread to reach the rendezvous");
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /**
     * Parks the cleanup thread at the point where it has decided a hints file is expired and is about to unlink it,
     * then lets a real transfer run against the same store. If the descriptor is still claimable at that point the
     * transfer picks it up and reads a file that the cleanup thread then deletes underneath it.
     */
    @Test(timeout = 300000)
    @BMRules(rules = {
        @BMRule(name = "park the cleanup thread just before it unlinks a hints file",
                targetClass = "org.apache.cassandra.hints.HintsStore",
                targetMethod = "delete",
                targetLocation = "AT ENTRY",
                action = "org.apache.cassandra.hints.HintsStoreCleanupRaceTest.pauseCleanupBeforeUnlink()"),
        @BMRule(name = "release the dispatch thread once the hints file is unlinked",
                targetClass = "org.apache.cassandra.hints.HintsStore",
                targetMethod = "delete",
                targetLocation = "AT EXIT",
                action = "org.apache.cassandra.hints.HintsStoreCleanupRaceTest.releaseDispatchAfterUnlink()"),
        @BMRule(name = "record the descriptor the dispatch thread claims",
                targetClass = "org.apache.cassandra.hints.HintsStore",
                targetMethod = "poll",
                targetLocation = "AT EXIT",
                action = "org.apache.cassandra.hints.HintsStoreCleanupRaceTest.recordDispatchPoll($!)")
    })
    public void expiredHintsCleanupMustNotUnlinkAClaimedFile() throws Exception
    {
        long now = currentTimeMillis();
        HintsDescriptor descriptor = new HintsDescriptor(hostId, now);
        writeHints(descriptor, 100, now);

        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        HintsStore store = catalog.get(hostId);
        assertEquals(1, store.getDispatchQueueSize());

        HintsDispatchExecutor dispatchExecutor =
            new HintsDispatchExecutor(directory, 1, new AtomicBoolean(false), address -> true);

        long afterExpiry = now + TimeUnit.SECONDS.toMillis(Hint.maxHintTTL) + 10;
        Thread cleanup = new Thread(() -> store.deleteExpiredHints(afterExpiry), "hints-expired-cleanup");
        cleanupThread = cleanup;
        armed = true;
        cleanup.start();

        // The cleanup thread has now committed to unlinking this file.
        await(cleanupReachedUnlink);

        // Start dispatching only now, so the cleanup thread always reaches the unlink first.
        Future<?> transfer = dispatchExecutor.transfer(catalog, () -> UUID.randomUUID());

        await(dispatchPolled);
        cleanup.join(TimeUnit.MINUTES.toMillis(AWAIT_MINUTES));

        Throwable dispatchFailure = null;
        try
        {
            transfer.get(AWAIT_MINUTES, TimeUnit.MINUTES);
        }
        catch (ExecutionException e)
        {
            dispatchFailure = e.getCause();
        }
        finally
        {
            armed = false;
            dispatchExecutor.shutdownBlocking();
        }

        assertNull("Expired-hints cleanup unlinked " + descriptor.fileName() + " while a concurrent dispatch had " +
                   "already claimed it. The dispatch then failed with: " + dispatchFailure,
                   claimedByDispatch.get());
        assertFalse("The expired hints file should have been dropped from the store", store.hasFiles());
        assertFalse("The expired hints file should have been deleted", descriptor.file(directory).exists());
    }

    /**
     * Two cleanup passes over the same descriptor. The losing one has already read the file and is about to cache
     * the expiration it computed, so it caches an entry for a descriptor the winner has since taken and deleted.
     * Losing the claim must not leave that entry behind.
     */
    @Test(timeout = 300000)
    @BMRule(name = "park a cleanup thread just before it caches a computed expiration",
            targetClass = "org.apache.cassandra.hints.HintsStore",
            targetMethod = "hasExpired",
            targetLocation = "AT INVOKE largestGcgs",
            action = "org.apache.cassandra.hints.HintsStoreCleanupRaceTest.pauseLosingCleanupBeforeCachingExpiry()")
    public void aCleanupThatLosesTheClaimMustNotStrandItsCachedExpiry() throws Exception
    {
        long now = currentTimeMillis();
        HintsDescriptor descriptor = new HintsDescriptor(hostId, now);
        writeHints(descriptor, 100, now);

        HintsStore store = HintsCatalog.load(directory, ImmutableMap.of()).get(hostId);
        assertEquals(1, store.getDispatchQueueSize());
        assertEquals(0, store.getHintsExpirationsMapSize());

        long afterExpiry = now + TimeUnit.SECONDS.toMillis(Hint.maxHintTTL) + 10;

        Thread losing = new Thread(() -> store.deleteExpiredHints(afterExpiry), "hints-expired-cleanup-loser");
        losingCleanupThread = losing;
        armed = true;
        losing.start();

        // The losing thread has read the file and is one statement away from caching its expiration.
        await(losingCleanupCachedExpiry);

        // A second pass claims the descriptor and runs to completion, clearing the cache on its way out.
        store.deleteExpiredHints(afterExpiry);
        assertFalse("The winning cleanup should have deleted the hints file", store.hasFiles());

        winningCleanupDone.countDown();
        losing.join(TimeUnit.MINUTES.toMillis(AWAIT_MINUTES));
        armed = false;

        assertEquals("A cleanup that lost the claim left its cached expiration behind",
                     0, store.getHintsExpirationsMapSize());
    }

    private void writeHints(HintsDescriptor descriptor, int hintsCount, long hintCreationTime) throws IOException
    {
        try (HintsWriter writer = HintsWriter.create(directory, descriptor))
        {
            ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);
            try (HintsWriter.Session session = writer.newSession(buffer))
            {
                for (int i = 0; i < hintsCount; i++)
                    session.append(createHint(i, hintCreationTime));
            }
            MemoryUtil.clean(buffer);
        }
    }

    private Hint createHint(int idx, long creationTime)
    {
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        Mutation mutation = new RowUpdateBuilder(table, creationTime, bytes(idx))
                            .clustering(bytes(idx))
                            .add("val", bytes(idx))
                            .build();

        return Hint.create(mutation, creationTime, 1);
    }
}
