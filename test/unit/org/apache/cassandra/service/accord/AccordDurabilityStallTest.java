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

package org.apache.cassandra.service.accord;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.impl.TestAgent;
import accord.primitives.Range;
import accord.primitives.Ranges;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry;
import org.apache.cassandra.service.accord.execution.AccordExecutionTestUtils;
import org.apache.cassandra.service.accord.execution.AccordExecutor;
import org.apache.cassandra.service.accord.execution.AccordExecutorSignalLoop;

import static org.apache.cassandra.service.accord.execution.AccordExecutionTestUtils.anyInconsistentIntersecting;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A durability bound must not pass a key whose update is outstanding.
 *
 * <p>{@code AbstractReplayer.minReplay} is computed from {@code LOCALLY_DURABLE_TO_COMMAND_STORE}, and replay is what
 * re-derives a fan-out that could not be applied. So reporting durability over a key with an outstanding update does
 * not merely defer the update - it loses it permanently, because the commands it derives from are never replayed again.
 * Conversely, <em>not</em> advancing the bound is the whole durable record of the failure, which is why the FAILED mark
 * itself does not need to be durable.
 *
 * <p>The report is stalled rather than made per-key: {@code RedundantBefore} is a persisted range map, so excluding
 * individual keys would fragment durable state, and with retry the stall is short lived.
 *
 * <p>Scope: this covers the decision {@code AccordCommandStore.ensureDurable} consults. Driving {@code ensureDurable}
 * end to end requires a schema (its success path goes through {@code AccordDurableOnFlush.notifyOnDurable} and
 * {@code AccordKeyspace.AccordColumnFamilyStores.commandsForKey}), so the end-to-end assertion - that the reported
 * bound does not advance, and does advance once the update is applied - belongs in a dtest and is not covered here.
 */
public class AccordDurabilityStallTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    /**
     * A durability report must be refused while any key in its ranges has an outstanding update.
     *
     * <p>Production does not ask this question directly - {@code ensureDurable}'s own scan throws when it meets an
     * inconsistent entry, and so makes no report - so this drives {@code anyInconsistentIntersecting}, which is the
     * same predicate expressed as a query. What it is protecting is worth restating: replay is what re-applies a
     * fan-out that failed, {@code AbstractReplayer.minReplay} is computed from LOCALLY_DURABLE_TO_COMMAND_STORE, so a
     * report that passes such a key does not defer the update, it loses it.
     */
    @Test
    public void durabilityReportIsRefusedWhileAnUpdateIsOutstanding() throws Exception
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 11));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey failing = key(tableId, partitioner, 5);
        RoutingKey other = key(tableId, partitioner, 9);

        TestAgent agent = new TestAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 1, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        AccordCommandStore store = AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });
        try
        {
            AccordFailedKeyTestHarness.loadIntoCache(executor, store, failing);

            Range fullRange = TokenRange.fullRange(tableId, partitioner);
            executor.executeDirectlyWithLock(() -> {
                // nothing outstanding: every report is permitted
                assertNull("no key has an outstanding update", anyInconsistentIntersecting(store, Ranges.of(fullRange)));
                assertNull("no key has an outstanding update", anyInconsistentIntersecting(store, null));

                AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(failing);
                assertNotNull("the key was never loaded, so this test proves nothing", entry);
                AccordExecutionTestUtils.setInconsistent(entry);
                assertTrue("the entry must report itself inconsistent", entry.isInconsistent());

                // a report over any range containing the key must be refused, as must a report over everything
                assertEquals("a report covering a key with an outstanding update must be refused",
                             failing, anyInconsistentIntersecting(store, Ranges.of(fullRange)));
                assertEquals("null ranges means every range this store owns", failing,
                             anyInconsistentIntersecting(store, null));

                // ... but a report over a disjoint range is unaffected: the refusal is as narrow as the range map allows
                assertNull("a report over a range that does not contain the key must not be refused",
                           anyInconsistentIntersecting(store, Ranges.of(rangeAround(tableId, partitioner, other))));

                // and once the update has been applied, reporting resumes
                AccordExecutionTestUtils.unsetInconsistent(entry);
                assertTrue("the mark must be cleared from the entry", !entry.isInconsistent());
                assertNull("the refusal must lift once the outstanding update has been applied",
                           anyInconsistentIntersecting(store, Ranges.of(fullRange)));
            });
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static RoutingKey key(TableId tableId, IPartitioner partitioner, int k)
    {
        return new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(k)));
    }

    /** a range containing only {@code key}, so that it is disjoint from the other keys used here */
    private static Range rangeAround(TableId tableId, IPartitioner partitioner, RoutingKey key)
    {
        Token token = ((TokenKey) key).token();
        return new TokenRange(new TokenKey(tableId, token.decreaseSlightly()), new TokenKey(tableId, token));
    }
}
