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

package org.apache.cassandra.utils.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.BulkIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Asserts that {@link BTree#update}'s heap accounting -- the total reported through
 * {@link UpdateFunction#onAllocatedOnHeap(long)} -- matches the actual on-heap structure of the resulting tree, as
 * measured by {@link BTree#sizeOnHeapOf(Object[])}. {@code onAllocatedOnHeap} reports the extra heap allocated over
 * the previous tree, so summed over a sequence of updates from empty it must equal {@code sizeOnHeapOf(result)}.
 * <p>
 * The scenarios cover the {@code Updater} paths that matter for accounting (coverage verified with JaCoCo): many tiny
 * disjoint inserts; a large contiguous run inserted in one update (forcing a node to overflow more than once before
 * draining); re-inserting existing keys (exercising the merge path); and a height-4 tree.
 */
public class BTreeUpdateHeapAccountingTest
{
    private static final Logger logger = LoggerFactory.getLogger(BTreeUpdateHeapAccountingTest.class);

    private static final Comparator<Integer> CMP = Integer::compare;

    private enum Scenario { SMALL_INSERTS, BLOCK_INSERTS, OVERLAPPING, DEEP }

    /**
     * A non-simple (i.e. not {@link UpdateFunction.Simple}) update function, so that {@code BTree.update} actually
     * performs heap accounting. It is an identity merge over the keys, so the only heap it reports is that of the
     * BTree node arrays themselves -- exactly what {@code sizeOnHeapOf} measures. It also counts merge invocations
     * so the test can assert the overlapping scenario really exercised the merge path.
     */
    private static final class AccountingUpdateFunction implements UpdateFunction<Integer, Integer>
    {
        long reported = 0;
        long merges = 0;

        @Override
        public Integer insert(Integer insert)
        {
            return insert;
        }

        @Override
        public Integer merge(Integer replacing, Integer update)
        {
            merges++;
            return update;
        }

        @Override
        public void onAllocatedOnHeap(long heapSize)
        {
            reported += heapSize;
        }
    }

    @Test
    public void reportedAllocationShouldMatchHeapSizeOfResult()
    {
        Trial[] trials = {
            runTrial(Scenario.SMALL_INSERTS, 1L, 3000, 3),
            runTrial(Scenario.SMALL_INSERTS, 42L, 2500, 100),
            runTrial(Scenario.BLOCK_INSERTS, 7L, 12000, 250),
            runTrial(Scenario.OVERLAPPING, 99L, 4000, 40),
            runTrial(Scenario.DEEP, 12345L, 40000, 200),
        };

        List<String> failures = new ArrayList<>();
        for (Trial r : trials)
        {
            logger.info(String.format("%-13s seed=%-6d keys=%-6d reported=%-10d sizeOnHeapOf=%-9d gap=%-10d  [over=+%d/%du, under=-%d/%du]  merges=%d height=%d",
                                      r.scenario, r.seed, r.distinctKeys, r.reported, r.actualHeap, r.reported - r.actualHeap,
                                      r.sumOver, r.updatesOver, r.sumUnder, r.updatesUnder, r.merges, r.height));
            if (r.reported != r.actualHeap)
                failures.add(String.format("%s (seed=%d): reported=%d but sizeOnHeapOf(result)=%d (net gap=%d; over=+%d, under=-%d; merges=%d)",
                                            r.scenario, r.seed, r.reported, r.actualHeap, r.reported - r.actualHeap,
                                            r.sumOver, r.sumUnder, r.merges));
        }

        assertTrue("BTree.update's onAllocatedOnHeap total does not match BTree.sizeOnHeapOf(result):\n  "
                   + String.join("\n  ", failures),
                   failures.isEmpty());
    }

    private static Trial runTrial(Scenario scenario, long seed, int numKeys, int maxBatch)
    {
        Random random = new Random(seed);

        // distinct keys inserted in random order, in random-sized batches; depending on the scenario some batches
        // are large contiguous runs (to force multi-overflow) and some re-insert existing keys (to force merge).
        List<Integer> order = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++)
            order.add(i);
        Collections.shuffle(order, random);

        AccountingUpdateFunction fn = new AccountingUpdateFunction();
        Object[] tree = BTree.empty();
        // reference set of the keys the tree should contain, used for the end-of-trial sanity check
        TreeSet<Integer> model = new TreeSet<>();

        Trial trial = new Trial();
        int pos = 0; // number of distinct keys consumed from order[] so far, i.e. order[0..pos) are in the tree
        boolean blockDone = false;
        // run until every distinct key has been inserted; OVERLAPPING additionally keeps going until the merge path
        // has been exercised at least 50 times (re-inserts only start producing merges once keys are in the tree)
        while (pos < numKeys || (scenario == Scenario.OVERLAPPING && fn.merges < 50))
        {
            TreeSet<Integer> batchSet = new TreeSet<>(CMP);

            int block = 4000; // large enough to overflow not just a leaf but a whole branch >1x in a single update
            if (scenario == Scenario.BLOCK_INSERTS && !blockDone && pos > maxBatch && pos < numKeys - block)
            {
                // a single update inserting a long contiguous run between two existing keys -- this overflows the
                // same leaf, and its parent branch, more than once within one update.
                for (int i = 0; i < block && pos < numKeys; i++)
                    batchSet.add(order.get(pos++));
                blockDone = true;
            }
            else
            {
                int batch = 1 + random.nextInt(maxBatch);
                for (int i = 0; i < batch; i++)
                {
                    // OVERLAPPING re-inserts an already-inserted key (one from order[0..pos)) to drive the merge
                    // path: roughly one entry in three while fresh keys remain, and every entry once they run out
                    boolean reInsert = scenario == Scenario.OVERLAPPING && pos > 0
                                       && (pos >= numKeys || random.nextInt(3) == 0);
                    if (reInsert)
                        batchSet.add(order.get(random.nextInt(pos)));
                    else if (pos < numKeys)
                        batchSet.add(order.get(pos++));
                }
                if (batchSet.isEmpty() && pos < numKeys)
                    batchSet.add(order.get(pos++));
            }
            if (batchSet.isEmpty())
                break;

            Integer[] keys = batchSet.toArray(new Integer[0]); // already sorted (TreeSet w/ CMP)
            Object[] insert = BTree.build(BulkIterator.of(keys), keys.length, UpdateFunction.noOp());

            // per-update contract: reported delta == sizeOnHeapOf(after) - sizeOnHeapOf(before)
            long heapBefore = BTree.sizeOnHeapOf(tree);
            long reportedBefore = fn.reported;
            tree = BTree.update(tree, insert, CMP, fn);
            long err = (fn.reported - reportedBefore) - (BTree.sizeOnHeapOf(tree) - heapBefore);
            if (err > 0) { trial.updatesOver++; trial.sumOver += err; }
            else if (err < 0) { trial.updatesUnder++; trial.sumUnder += -err; }

            model.addAll(batchSet);
        }

        // sanity: the tree must contain exactly the model, in order
        assertEquals(model.size(), BTree.size(tree));
        Integer prev = null;
        int n = 0;
        for (Integer key : BTree.<Integer>iterable(tree))
        {
            if (prev != null)
                assertTrue("tree not sorted", key > prev);
            assertTrue("unexpected key " + key, model.contains(key));
            prev = key;
            n++;
        }
        assertEquals(model.size(), n);

        trial.scenario = scenario;
        trial.seed = seed;
        trial.distinctKeys = model.size();
        trial.reported = fn.reported;
        trial.merges = fn.merges;
        trial.actualHeap = BTree.sizeOnHeapOf(tree);
        trial.height = BTree.depth(tree);
        return trial;
    }

    private static final class Trial
    {
        Scenario scenario;
        long seed;
        int distinctKeys;
        long reported;
        long merges;
        long actualHeap;
        int height;
        int updatesOver;
        long sumOver;
        int updatesUnder;
        long sumUnder;
    }

    /**
     * Verifies that re-inserting every key that already exists reports a net heap delta of exactly zero,
     * and that {@code onAllocatedOnHeap} is actually invoked (not silently skipped by an {@code allocated > 0}
     * guard). The call must happen so that callers accumulating signed deltas see the confirmation, and so
     * that a negative allocated value cannot permanently disable accounting by crossing the -1 sentinel.
     */
    @Test
    public void netZeroDeltaOnAllocatedOnHeapIsInvoked()
    {
        int numKeys = BTree.MAX_KEYS * BTree.MAX_KEYS;
        Integer[] keys = new Integer[numKeys];
        for (int i = 0; i < numKeys; i++)
            keys[i] = i;

        Object[] tree = BTree.build(BulkIterator.of(keys), numKeys, UpdateFunction.noOp());
        Object[] insert = BTree.build(BulkIterator.of(keys), numKeys, UpdateFunction.noOp());

        // Use a counting variant that records invocation count separately from the accumulated total
        final long[] invocations = { 0 };
        final long[] total = { 0 };

        UpdateFunction<Integer, Integer> fn = new UpdateFunction<Integer, Integer>()
        {
            @Override
            public Integer insert(Integer i)
            {
                return i;
            }

            @Override
            public Integer merge(Integer existing, Integer update)
            {
                return update;
            }

            @Override
            public void onAllocatedOnHeap(long delta)
            {
                invocations[0]++;
                total[0] += delta;
            }
        };

        Object[] result = BTree.update(tree, insert, CMP, fn);

        assertSame("full re-insert of identical keys must reuse the existing tree root", tree, result);

        assertEquals("reported net delta must be zero for a full re-insert of identical keys",
                     0L, total[0]);

        assertTrue("onAllocatedOnHeap must be invoked even for a net-zero update; " +
                   "skipping it conflates 'tracking disabled' (-1 sentinel) with 'net zero result' (0)",
                   invocations[0] >= 1);
    }
}
