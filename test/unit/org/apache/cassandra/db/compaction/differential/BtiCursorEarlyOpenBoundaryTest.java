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

package org.apache.cassandra.db.compaction.differential;

/**
 * The BTI half of {@link CursorEarlyOpenBoundaryTest}.
 *
 * BTI reaches no index summary, so it cannot carry the boundary defect the parent class is named
 * for. It has its own retention instead: {@code PartitionIndexBuilder} holds {@code firstKey} for
 * the whole build and the previous key across {@code addEntry} to compute each separator, then
 * writes both bounds into the Partitions.db footer. Handed a key the next partition overwrites,
 * both bounds collapse onto the final partition and every separator derives from a key that has
 * since moved, so the trie routes seeks to the wrong place.
 *
 * The parent's committed-bounds assertion is what catches that here. Its early-open assertions still
 * run on whatever BTI publishes, but nothing requires a publication: {@code BtiTableWriter.openEarly}
 * defers through {@code PartitionIndexBuilder.buildPartial} until the data, row index and partition
 * index writers have all flushed past the recorded ends, refuses a second request while one is
 * pending, and {@code openFinalEarly} cancels what is still outstanding. Zero reopens is correct
 * behaviour, not a failure, so a count-based oracle is unsound on this format.
 */
public class BtiCursorEarlyOpenBoundaryTest extends CursorEarlyOpenBoundaryTest
{
    @Override
    protected String formatName()
    {
        return "bti";
    }

    @Override
    protected boolean requiresMidStreamReopen()
    {
        return false;
    }
}
