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
package org.apache.cassandra.db.partitions;

import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CQL-driven regression test for CASSANDRA-21390 ("TrieMemtable MemtableReclaimMemory AssertionError:
 * Negative released in MemtablePool$SubPool").
 * <p>
 * It drives the same overwrite/delete churn through the public CQL write path
 * (with explicit {@code USING TIMESTAMP} so reconciliation is deterministic), then inspects the live
 * memtable's on-heap ownership after every statement and forces a flush to exercise the real discard/release path.
 * <p>
 * Root cause: the memtable on-heap accounting in {@link BTreePartitionUpdater}
 * over-subtracts when an update's cells are SHADOWED by an existing,
 * newer row/partition deletion. The shadowed cells' heap overhead is subtracted from {@code owns}
 * even though it was never added, so under repair-style churn (re-writing cells into a partition that
 * already carries a newer tombstone) {@code owns} drifts below zero and the next flush trips
 * {@code AssertionError: Negative released} in {@code MemtablePool$SubPool.released} during discard.
 * <p>
 * The test asserts the same invariant the production code asserts at flush
 * ({@code memtable on-heap owns >= 0}) and then flushes, which runs the real discard path
 * ({@code MemtablePool$SubPool.released <- SubAllocator.releaseAll}) and reproduces the exact
 * production stack when {@code owns} has gone negative.
 * <p>
 * Note on minimisation: the {@code PartitionUpdate}-level sibling reproduces with a single shadowed
 * update, but the CQL path first clones the incoming row into the memtable allocator and only then
 * merges, so an isolated shadowed statement nets close to zero. The drift only becomes observable
 * across mixed churn with non-monotonic timestamps — which is exactly the repair re-streaming
 * workload that hit this in the field — hence the fixed-seed churn below rather than a one-liner.
 */
@RunWith(Parameterized.class)
public class MemtableNegativeReleasedCQLReproTest extends CQLTester
{
    @Parameter
    public String memtableClass;

    @Parameters(name = "memtable={0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("skiplist", "skiplist_sharded", "trie");
    }

    /**
     * Deterministic fixed-seed churn of overwrites / row tombstones / partition deletions against a
     * single partition (what repair re-streaming does). On buggy code this drives the memtable's
     * on-heap ownership negative; the per-statement assertion catches the drift and the trailing flush
     * reproduces the exact production discard crash.
     */
    @Test
    public void repeatedOverwriteAndDeleteChurnKeepsOwnsNonNegative()
    {
        for (int seed = 0; seed < 50; seed++)
        {
            createTable("CREATE TABLE %s (pk int, ck int, r1 int, c2 set<int>, PRIMARY KEY (pk, ck)) " +
                        "WITH memtable = '" + memtableClass + "'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            Random rnd = new Random(seed);
            for (int i = 0; i < 40; i++)
            {
                mutateSamePartition(rnd);
                assertThat(currentOnHeapOwns(cfs))
                .as("memtable on-heap owns went NEGATIVE during overwrite/delete churn (memtable=%s, " +
                    "seed=%d, mutation=%d) — the memtable reported releasing more on-heap memory than it " +
                    "allocated (CASSANDRA-21390)", memtableClass, seed, i)
                .isGreaterThanOrEqualTo(0L);
            }
            // The real crash path: flush discards the memtable, running releaseAll() -> SubPool.released(owns),
            // which throws AssertionError("Negative released: ...") under -ea if owns ever went negative.
            flush();
        }
    }

    private long currentOnHeapOwns(ColumnFamilyStore cfs)
    {
        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        return Memtable.getMemoryUsage(memtable).ownsOnHeap;
    }

    /**
     * A varying mutation of the same partition: live overwrite, row tombstone, or partition deletion.
     */
    private void mutateSamePartition(Random rnd)
    {
        // RANDOM (non-monotonic) timestamp is essential: it makes reconciliation sometimes keep the
        // existing cell and sometimes shadow/replace it, producing the merges that over-subtract.
        long ts = 1000 + rnd.nextInt(3000);
        switch (rnd.nextInt(6))
        {
            case 0: // whole-partition deletion
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 0");
                break;
            case 1: // whole-row tombstone
                execute("DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = 0 AND ck = 0");
                break;
            default: // live overwrite of varying shape
            {
                StringBuilder set = new StringBuilder("r1 = ").append(rnd.nextInt());
                int cells = rnd.nextInt(5);
                StringBuilder elements = new StringBuilder();
                for (int k = 0; k < cells; k++)
                {
                    if (elements.length() > 0)
                        elements.append(", ");
                    elements.append(1000 + rnd.nextInt(8));
                }
                if (cells > 0)
                {
                    if (rnd.nextInt(3) == 0)
                        set.append(", c2 = {").append(elements).append('}'); // full overwrite -> complex deletion
                    else
                        set.append(", c2 = c2 + {").append(elements).append('}'); // append cells, no complex deletion
                }
                execute("UPDATE %s USING TIMESTAMP " + ts + " SET " + set + " WHERE pk = 0 AND ck = 0");
                break;
            }
        }
    }
}
