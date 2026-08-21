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

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Snapshot of {@link AbstractCompactionPipeline}'s pipeline-selection counters, plus the assertion
 * that a compaction really went through the pipeline the scenario asked for.
 * <p>
 * {@code CursorCompactor.isSupported} answers whether the cursor path COULD run;
 * {@link AbstractCompactionPipeline#create} additionally gates on
 * {@code DatabaseDescriptor.cursorCompactionEnabled()}, which isSupported never reads. So a
 * scenario can satisfy a supportability precheck and still be served by the iterator pipeline,
 * comparing that path against itself or asserting nothing about the cursor writer at all.
 * <p>
 * Lives in {@code org.apache.cassandra.db.compaction} so that it can read the package-private
 * counters without widening any production declaration.
 * <p>
 * Always a DELTA across one compaction, never an absolute: {@code forkmode=perTest} gives each test
 * class its own JVM, but the methods within a class share it, so the counters carry every earlier
 * compaction in the same fork.
 */
public final class CompactionPipelineCounts
{
    private final long cursor;
    private final long iterator;
    private final boolean cursorCompactionEnabled;

    private CompactionPipelineCounts(long cursor, long iterator, boolean cursorCompactionEnabled)
    {
        this.cursor = cursor;
        this.iterator = iterator;
        this.cursorCompactionEnabled = cursorCompactionEnabled;
    }

    /**
     * Snapshots both counters and the current {@code cursorCompactionEnabled} setting. Take it
     * immediately before the compaction under test, and after any flip of the flag that the exact
     * check in {@link #assertPipelineRan} should apply to.
     */
    public static CompactionPipelineCounts mark()
    {
        return new CompactionPipelineCounts(AbstractCompactionPipeline.cursorPipelinesCreated(),
                                            AbstractCompactionPipeline.iteratorPipelinesCreated(),
                                            DatabaseDescriptor.cursorCompactionEnabled());
    }

    /**
     * Asserts that at least one compaction selecting the expected pipeline happened since
     * {@code before}, and that no cursor pipeline was created at all if cursor compaction was
     * switched off for the whole bracket.
     * <p>
     * The first is a lower bound on one counter only. A compaction unrelated to the scenario — a
     * system table's, or a background compaction the test did not disable — moves the same static
     * counters, so requiring the other counter to be unmoved, or requiring an exact delta, would
     * fail for reasons that are not defects. A lower bound cannot fail that way: an incidental
     * compaction only ever adds to a delta.
     * <p>
     * That lower bound alone can be satisfied by an incidental compaction while the scenario's own
     * compaction selected the other pipeline. The second assertion closes that: while
     * {@code cursorCompactionEnabled} is off, a compaction reaching
     * {@link AbstractCompactionPipeline#create} cannot select the cursor pipeline, so any cursor
     * pipeline across the bracket is a real defect. It applies only when the flag reads off both
     * before and after, so a caller that flips the flag inside its own bracket cannot trip it. It
     * reads the same accessor {@code create} does, so it cannot detect a setter that never changed
     * that accessor's value; what it does pin is that {@code create} still honours it.
     * <p>
     * The exact check assumes no compaction was already in flight when the flag was flipped, which
     * is why callers disable autocompaction on tables they do not intend to compact.
     */
    public static void assertPipelineRan(boolean expectCursor, CompactionPipelineCounts before)
    {
        CompactionPipelineCounts after = mark();
        String detail = " (cursor pipelines +" + (after.cursor - before.cursor) +
                        ", iterator pipelines +" + (after.iterator - before.iterator) + ')';
        if (expectCursor)
            assertTrue("cursor compaction was requested, but no cursor pipeline was created for " +
                       "this compaction: it ran the iterator path instead, so this scenario asserts " +
                       "nothing about the cursor reader or writer" + detail,
                       after.cursor - before.cursor >= 1);
        else
            assertTrue("the iterator path was requested, but no iterator pipeline was created for " +
                       "this compaction" + detail,
                       after.iterator - before.iterator >= 1);

        if (!before.cursorCompactionEnabled && !after.cursorCompactionEnabled)
            assertEquals("cursor compaction was switched off across this compaction, so nothing " +
                         "could have selected the cursor pipeline, yet one was created" + detail,
                         0, after.cursor - before.cursor);
    }
}
