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

package org.apache.cassandra.io.sstable.metadata;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

/**
 * Each partition key is hashed to 64 bits, which are split up as follows (p = 13, sp = 25):
 * <pre>
 *   63         51 50         39 38                            0   (hash bits)
 *  +-------------+-------------+-------------------------------+
 *  |  dense idx  |  middle 12  |          low 39 bits          |
 *  +-------------+-------------+-------------------------------+
 *  |    sparse idx (25 bits)   |
 *  +---------------------------+
 * </pre>
 *
 * <p>
 * The sparse idx is always hash bits 63..39. The two sparse entry layouts below only differ in where
 * they place it within the 32-bit entry.
 * </p>
 *
 * <pre>
 * The run length (rl), which is only used for the dense format, is found by scanning the bits after
 * the dense idx, from bit 50 down to bit 0, and counting how many bits it takes to
 * reach the first 1 (the number of leading zeros plus one):
 *
 * hash bit:   50  49  48  47  ...   1   0
 * position:    1   2   3   4  ...  50  51      (52 if none of them is 1)
 * </pre>
 *
 * For example, rl = 1 if bit 50 is 1, and rl = 4 for 0001. It ranges from 1 to 52.
 * Sparse format: a list of ints sorted by sparse idx, one entry per distinct sparse idx:
 *
 * <pre>
 *  +-----------+-----------+-----------+-----+-----------+
 *  |  entry 0  |  entry 1  |  entry 2  | ... | entry n-1 |
 *  +-----------+-----------+-----------+-----+-----------+
 *
 *   31                                        0   (entry bits)
 *  +--------+----------------+--------------+---+
 *  | 000000 | dense idx (13) |  middle 12   | 0 |  index-only entry: middle 12 bits not all zero
 *  +--------+-------+--------+-----+--------+---+
 *  | dense idx (13) | 000000000000 | ~rl(6) | 1 |  run-length entry: middle 12 bits all zero
 *  +----------------+--------------+--------+---+
 * </pre>
 *
 * <p>
 * The two layouts are also the source of the bug, which is fixed upstream in stream-lib 2.6.0
 * (https://github.com/addthis/stream-lib/pull/54). When stream-lib 2.5.2 merges two sparse sketches, it
 * decides which list to advance by comparing entries rather than sparse indexes. This does not cause
 * correctness issues when every entry is index-only. A run-length entry breaks the invariant the merge relies on,
 * that entries are sorted by sparse idx. As a result the merge does not combine entries that share a sparse idx,
 * so they appear twice in the merged list. In sparse format the cardinality is computed as a function of the number of entries
 * (see - linear counting), causing the estimate to be greater than expected. Adding every sketch to a
 * dense union instead does not depend on entry order or duplicates, which avoids the bug.
 * </p>
 */
public final class PartitionKeyUnion
{
    private ICardinality first;
    private HyperLogLogPlus union;

    public void add(ICardinality sketch) throws CardinalityMergeException
    {
        if (first == null)
        {
            first = sketch;
            return;
        }

        if (union == null)
        {
            union = new HyperLogLogPlus(MetadataCollector.CARDINALITY_PRECISION);
            union.addAll((HyperLogLogPlus) first);
        }

        union.addAll((HyperLogLogPlus) sketch);
    }

    public boolean isEmpty()
    {
        return first == null;
    }

    public long cardinality()
    {
        if (union != null)
            return union.cardinality();
        return first == null ? 0 : first.cardinality();
    }
}
