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
package org.apache.cassandra.service.paxos;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AugmentedCommitTest
{
    private static final PaxosCommit.Status SUCCESS = new PaxosCommit.Status(null);
    private static final PaxosCommit.Status FAILURE = new PaxosCommit.Status(
        new Paxos.MaybeFailure(true, 3, 2, 0, emptyMap()));

    private static PaxosCommit.AugmentedCommit<Consumer<PaxosCommit.Status>> create(AtomicReference<PaxosCommit.Status> capture)
    {
        return new PaxosCommit.AugmentedCommit<>(capture::set);
    }

    // ========================================
    // Both succeed
    // ========================================

    @Test
    public void testBothSucceed_paxosFirst()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onPaxosComplete(SUCCESS);
        assertNull("Should not complete with only paxos", result.get());

        ac.onMutationComplete(SUCCESS);
        assertNotNull("Should complete when both done", result.get());
        assertTrue("Should be success", result.get().isSuccess());
    }

    @Test
    public void testBothSucceed_mutationFirst()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onMutationComplete(SUCCESS);
        assertNull("Should not complete with only mutation", result.get());

        ac.onPaxosComplete(SUCCESS);
        assertNotNull("Should complete when both done", result.get());
        assertTrue("Should be success", result.get().isSuccess());
    }

    // ========================================
    // Paxos fails
    // ========================================

    @Test
    public void testPaxosFails_immediateCompletion()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onPaxosComplete(FAILURE);
        assertNotNull("Should complete immediately on paxos failure", result.get());
        assertFalse("Should report failure", result.get().isSuccess());
    }

    @Test
    public void testPaxosFails_afterMutationSucceeds()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onMutationComplete(SUCCESS);
        assertNull(result.get());

        ac.onPaxosComplete(FAILURE);
        assertNotNull("Should complete on paxos failure", result.get());
        assertFalse("Should report failure", result.get().isSuccess());
    }

    // ========================================
    // Mutation fails
    // ========================================

    @Test
    public void testMutationFails_immediateCompletion()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onMutationComplete(FAILURE);
        assertNotNull("Should complete immediately on mutation failure", result.get());
        assertFalse("Should report failure", result.get().isSuccess());
    }

    @Test
    public void testMutationFails_afterPaxosSucceeds()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onPaxosComplete(SUCCESS);
        assertNull(result.get());

        ac.onMutationComplete(FAILURE);
        assertNotNull("Should complete on mutation failure", result.get());
        assertFalse("Should report failure", result.get().isSuccess());
    }

    // ========================================
    // Both fail
    // ========================================

    @Test
    public void testBothFail_paxosFirst()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onPaxosComplete(FAILURE);
        assertNotNull("Should complete immediately", result.get());
        assertFalse(result.get().isSuccess());

        // Second failure is a no-op
        ac.onMutationComplete(FAILURE);
    }

    @Test
    public void testBothFail_mutationFirst()
    {
        AtomicReference<PaxosCommit.Status> result = new AtomicReference<>();
        var ac = create(result);

        ac.onMutationComplete(FAILURE);
        assertNotNull("Should complete immediately", result.get());
        assertFalse(result.get().isSuccess());

        // Second failure is a no-op
        ac.onPaxosComplete(FAILURE);
    }

    // ========================================
    // Terminal state is idempotent
    // ========================================

    @Test
    public void testCompleteState_ignoresFurtherUpdates()
    {
        AtomicInteger callCount = new AtomicInteger();
        var ac = new PaxosCommit.AugmentedCommit<Consumer<PaxosCommit.Status>>(s -> callCount.incrementAndGet());

        ac.onPaxosComplete(SUCCESS);
        ac.onMutationComplete(SUCCESS);
        assertEquals("onDone should be called exactly once", 1, callCount.get());

        // Further calls should be no-ops
        ac.onPaxosComplete(SUCCESS);
        ac.onMutationComplete(FAILURE);
        ac.onPaxosComplete(FAILURE);
        assertEquals("onDone should still be called exactly once", 1, callCount.get());
    }

    @Test
    public void testCompleteViaFailure_ignoresFurtherUpdates()
    {
        AtomicInteger callCount = new AtomicInteger();
        var ac = new PaxosCommit.AugmentedCommit<Consumer<PaxosCommit.Status>>(s -> callCount.incrementAndGet());

        ac.onPaxosComplete(FAILURE);
        assertEquals(1, callCount.get());

        ac.onMutationComplete(SUCCESS);
        ac.onMutationComplete(FAILURE);
        ac.onPaxosComplete(SUCCESS);
        assertEquals("onDone should still be called exactly once", 1, callCount.get());
    }

    // ========================================
    // Duplicate calls to same side
    // ========================================

    @Test(expected = IllegalStateException.class)
    public void testDuplicatePaxosComplete_throws()
    {
        var ac = create(new AtomicReference<>());
        ac.onPaxosComplete(SUCCESS);
        ac.onPaxosComplete(SUCCESS);
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicateMutationComplete_throws()
    {
        var ac = create(new AtomicReference<>());
        ac.onMutationComplete(SUCCESS);
        ac.onMutationComplete(SUCCESS);
    }
}
