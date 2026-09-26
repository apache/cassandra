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

/** Snapshot of {@link AbstractCompactionPipeline}'s pipeline-selection counters, with an assertion that a compaction ran the expected pipeline. */
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

    /** Snapshots both counters and the current {@code cursorCompactionEnabled} setting. */
    public static CompactionPipelineCounts mark()
    {
        return new CompactionPipelineCounts(AbstractCompactionPipeline.cursorPipelinesCreated(),
                                            AbstractCompactionPipeline.iteratorPipelinesCreated(),
                                            DatabaseDescriptor.cursorCompactionEnabled());
    }

    /** The raw cursor-pipeline counter. */
    public static long cursorPipelines()
    {
        return AbstractCompactionPipeline.cursorPipelinesCreated();
    }

    /** The raw iterator-pipeline counter. */
    public static long iteratorPipelines()
    {
        return AbstractCompactionPipeline.iteratorPipelinesCreated();
    }

    /** Asserts a compaction selecting the expected pipeline ran since {@code before}, and that no cursor pipeline was created while cursor compaction was off. */
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
