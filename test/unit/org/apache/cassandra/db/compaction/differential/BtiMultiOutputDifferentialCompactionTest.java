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

import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

/**
 * Runs the size-capped multi-output scenarios with the BTI format selected. A writer switch is the
 * only route to any of the following, and nothing else in this suite takes it under BTI:
 * <ul>
 * <li>{@code PartitionIndexBuilder.firstKey}, {@code lastKey} and {@code complete()} per output
 *     ({@code PartitionIndexBuilder.java:130-183}). With one output those run once over the whole
 *     key range; with several they run over a partial range whose bounds the switch decided. All
 *     three inherited scenarios reach this.</li>
 * <li>{@code BtiTableWriter.openFinalEarly} on a SWITCHED writer. {@code SSTableRewriter} early-opens
 *     the finished writer as it switches ({@code SSTableRewriter.java:271-282}), and early open stays
 *     enabled in this harness deliberately. All three inherited scenarios reach this too.</li>
 * <li>A fresh {@code SSTableCursorWriter} per output, and so a fresh {@code BtiCursorIndexWriter}
 *     and a fresh {@code RowIndexWriter} — {@code CursorCompactor} builds them when it switches
 *     ({@code CursorCompactor.java:1788-1793}). Every other BTI scenario constructs exactly one of
 *     each per compaction, so per-output construction and per-output {@code close()} are otherwise
 *     unexercised.</li>
 * </ul>
 * Only {@code widePartitionsForceFrequentSwitches} builds a ROW TRIE here: its partitions hold about
 * 6 KiB, above the 4 KiB column_index_size, so {@code BtiCursorIndexWriter.endPartition} cuts a block
 * plus a tail and calls {@code RowIndexWriter.complete}. {@code manyPartitionsSplitAcrossOutputs} and
 * {@code tombstonesAndStaticsAcrossOutputs} hold about 1 KiB per partition, so their block count never
 * exceeds one and every partition takes the {@code trieRoot = -1} arm. Those two cover the switch and
 * the per-output partition index; they say nothing about the row trie, and per-output
 * {@code RowIndexWriter.reset()} does nothing observable on them.
 * <p>
 * Beyond the switch itself, the format selection also puts BTI's own components — {@code Partitions.db}
 * and {@code Rows.db} — into the per-output byte comparison; under BIG the same scenarios compare
 * {@code Index.db} and {@code Summary.db} instead.
 * <p>
 * The inherited scenarios each assert {@code out.sstables.size() >= 2}, so a data volume that stopped
 * rolling over under BTI fails here rather than passing vacuously. The switch decision reads
 * {@code SortedTableWriter.getEstimatedOnDiskBytesWritten()}, which is the DATA file's position only,
 * so it is the same number under both formats for the same rows; the index components differ in size
 * but do not enter the decision.
 */
public class BtiMultiOutputDifferentialCompactionTest extends MultiOutputDifferentialCompactionTest
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void selectBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }
}
