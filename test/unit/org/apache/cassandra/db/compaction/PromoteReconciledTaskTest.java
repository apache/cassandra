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

package org.apache.cassandra.db.compaction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PromoteReconciledTaskTest extends AbstractPendingRepairTest
{
    private Set<SSTableReader> sstables(int count)
    {
        Set<SSTableReader> sstables = new HashSet<>();
        for (int i = 0; i < count; i++)
            sstables.add(makeSSTable(true));
        return sstables;
    }

    private AbstractCompactionTask tryPromote(Set<SSTableReader> candidates)
    {
        return PromoteReconciledTask.tryPromote(cfs, candidates, "test", null);
    }

    @Test
    public void noCandidatesYieldsNoTask()
    {
        assertNull(tryPromote(Collections.emptySet()));
    }

    @Test
    public void allCandidatesBusyYieldsNoTask()
    {
        Set<SSTableReader> candidates = sstables(2);
        AtomicInteger hookRuns = new AtomicInteger();

        try (LifecycleTransaction held = cfs.getTracker().tryModify(candidates, OperationType.COMPACTION))
        {
            assertNotNull(held);
            assertNull("nothing is claimable, so there is nothing to promote",
                       PromoteReconciledTask.tryPromote(cfs, candidates, "test", hookRuns::incrementAndGet));
        }
        assertEquals("no task was produced, so nothing should have been signalled", 0, hookRuns.get());
    }

    @Test
    public void allCandidatesFreeClaimsAllOfThem()
    {
        Set<SSTableReader> candidates = sstables(3);

        AbstractCompactionTask task = tryPromote(candidates);
        assertNotNull(task);
        try
        {
            assertEquals(candidates, task.transaction.originals());
        }
        finally
        {
            task.transaction.abort();
        }
    }

    /**
     * One busy sstable shouldn't block compaction on the others. The task should try to aquire the remaining tables
     * that aren't referenced
     */
    @Test
    public void busyCandidateDoesntBlockRemainder()
    {
        Set<SSTableReader> candidates = sstables(3);
        SSTableReader busy = candidates.iterator().next();

        try (LifecycleTransaction held = cfs.getTracker().tryModify(Collections.singleton(busy),
                                                                   OperationType.COMPACTION))
        {
            assertNotNull(held);

            AbstractCompactionTask task = tryPromote(candidates);
            assertNotNull("the free candidates should still have been promoted", task);
            try
            {
                Set<SSTableReader> claimed = task.transaction.originals();
                // The busy one is left out and every other candidate is claimed.
                assertEquals(candidates.size() - 1, claimed.size());
                assertFalse(claimed.contains(busy));
                Set<SSTableReader> rest = new HashSet<>(candidates);
                rest.remove(busy);
                assertTrue(claimed.containsAll(rest));
            }
            finally
            {
                task.transaction.abort();
            }
        }
    }
}
