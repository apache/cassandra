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

package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.schema.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MutationSummaryTest
{
    private static final TableId TABLE_ID = TableId.generate();
    private static final CoordinatorLogId LOG_1 = new CoordinatorLogId(1, 1);
    private static final CoordinatorLogId LOG_2 = new CoordinatorLogId(2, 1);

    private static MutationSummary.CoordinatorSummary coordinatorSummary(CoordinatorLogId logId,
                                                                         int[] reconciled,
                                                                         int[] unreconciled)
    {
        return new MutationSummary.CoordinatorSummary(new Offsets.Immutable(logId, reconciled),
                                                      new Offsets.Immutable(logId, unreconciled));
    }

    private static MutationSummary summary(MutationSummary.CoordinatorSummary... summaries)
    {
        MutationSummary.Builder builder = new MutationSummary.Builder(TABLE_ID);
        for (MutationSummary.CoordinatorSummary s : summaries)
        {
            MutationSummary.CoordinatorSummary.Builder logBuilder = builder.builderForLog(s.logId());
            for (ShortMutationId id : s.reconciled)
                logBuilder.reconciled.add(id.offset);
            for (ShortMutationId id : s.unreconciled)
                logBuilder.unreconciled.add(id.offset);
        }
        return builder.build();
    }

    @Test
    public void testBothEmpty()
    {
        MutationSummary s1 = summary();
        MutationSummary s2 = summary();

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(s1, s2, diff);
        assertTrue(diff.isEmpty());
        assertFalse(MutationSummary.hasMissingUnreconciled(s1, s2));

        Log2OffsetsMap.Mutable offsetsDiff = new Log2OffsetsMap.Mutable();
        MutationSummary.unreconciledDifference(s1, s2, offsetsDiff);
        assertTrue(offsetsDiff.isEmpty());
    }

    @Test
    public void testMatchingUnreconciled()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 4 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 4 }));

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(source, target, diff);
        assertTrue(diff.isEmpty());
        assertFalse(MutationSummary.hasMissingUnreconciled(source, target));

        Log2OffsetsMap.Mutable offsetsDiff = new Log2OffsetsMap.Mutable();
        MutationSummary.unreconciledDifference(source, target, offsetsDiff);
        assertTrue(offsetsDiff.isEmpty());
    }

    @Test
    public void testSourceUnreconciledPresentInTargetReconciled()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 3 }, new int[]{ 4 }));

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(source, target, diff);
        assertTrue(diff.isEmpty());
        assertFalse(MutationSummary.hasMissingUnreconciled(source, target));

        Log2OffsetsMap.Mutable offsetsDiff = new Log2OffsetsMap.Mutable();
        MutationSummary.unreconciledDifference(source, target, offsetsDiff);
        assertTrue(offsetsDiff.isEmpty());
    }

    @Test
    public void testSourceUnreconciledMissingFromTarget()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 3, 5, 5 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 3 }));

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(source, target, diff);
        assertEquals(List.of(new ShortMutationId(LOG_1, 5)), diff);
        assertTrue(MutationSummary.hasMissingUnreconciled(source, target));

        Log2OffsetsMap.Mutable offsetsDiff = new Log2OffsetsMap.Mutable();
        MutationSummary.unreconciledDifference(source, target, offsetsDiff);
        assertTrue(offsetsDiff.contains(new ShortMutationId(LOG_1, 5)));
        assertEquals(1, offsetsDiff.idCount());
    }

    @Test
    public void testExtraReconciledOnSourceIsIgnored()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{ 1, 1, 99, 99 }, new int[]{}));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 3 }));

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(source, target, diff);
        assertTrue(diff.isEmpty());
        assertFalse(MutationSummary.hasMissingUnreconciled(source, target));

        Log2OffsetsMap.Mutable offsetsDiff = new Log2OffsetsMap.Mutable();
        MutationSummary.unreconciledDifference(source, target, offsetsDiff);
        assertTrue(offsetsDiff.isEmpty());
    }

    @Test
    public void testSourceHasLogAbsentFromTarget()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 3 }),
                                         coordinatorSummary(LOG_2, new int[]{ 10, 20 }, new int[]{ 30, 30 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 3 }));

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(source, target, diff);
        assertEquals(List.of(new ShortMutationId(LOG_2, 30)), diff);
        assertTrue(MutationSummary.hasMissingUnreconciled(source, target));

        Log2OffsetsMap.Mutable offsetsDiff = new Log2OffsetsMap.Mutable();
        MutationSummary.unreconciledDifference(source, target, offsetsDiff);
        assertTrue(offsetsDiff.contains(new ShortMutationId(LOG_2, 30)));
        assertEquals(1, offsetsDiff.idCount());
    }

    @Test
    public void testTargetHasLogAbsentFromSource()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 3 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 3 }),
                                         coordinatorSummary(LOG_2, new int[]{ 10, 20 }, new int[]{ 30, 30 }));

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(source, target, diff);
        assertTrue(diff.isEmpty());
        assertFalse(MutationSummary.hasMissingUnreconciled(source, target));

        Log2OffsetsMap.Mutable offsetsDiff = new Log2OffsetsMap.Mutable();
        MutationSummary.unreconciledDifference(source, target, offsetsDiff);
        assertTrue(offsetsDiff.isEmpty());
    }

    @Test
    public void testSameInstance()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 4 }));
        assertFalse(MutationSummary.hasMissingUnreconciled(source, source));
    }

    @Test
    public void testSourceMinLessThanTargetMin()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{}, new int[]{ 1, 5 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 2, 5 }, new int[]{}));
        assertTrue(MutationSummary.hasMissingUnreconciled(source, target));
    }

    @Test
    public void testSourceMaxGreaterThanTargetMax()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{}, new int[]{ 2, 6 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 2, 5 }, new int[]{}));
        assertTrue(MutationSummary.hasMissingUnreconciled(source, target));
    }

    @Test
    public void testSourceStraddlesReconciledAndUnreconciled()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{}, new int[]{ 1, 4 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 3, 4 }));
        assertFalse(MutationSummary.hasMissingUnreconciled(source, target));

        MutationSummary missingTarget = summary(coordinatorSummary(LOG_1, new int[]{ 1, 2 }, new int[]{ 4, 5 }));
        assertTrue(MutationSummary.hasMissingUnreconciled(source, missingTarget));
    }

    @Test
    public void testSourceCoveredByOverlappingReconciledAndUnreconciled()
    {
        MutationSummary source = summary(coordinatorSummary(LOG_1, new int[]{}, new int[]{ 5, 25 }));
        MutationSummary target = summary(coordinatorSummary(LOG_1, new int[]{ 1, 8, 10, 12, 21, 25 }, new int[]{ 3, 5, 7, 20 }));
        assertFalse(MutationSummary.hasMissingUnreconciled(source, target));

        List<ShortMutationId> diff = new ArrayList<>();
        MutationSummary.unreconciledDifference(source, target, diff);
        assertTrue(diff.isEmpty());
    }
}

