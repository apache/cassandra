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

package org.apache.cassandra.index.sai.utils;

import com.carrotsearch.hppc.LongFloatHashMap;

/**
 * A class that records a row's score in the QueryContext's score cache.
 */
public class RowIdScoreRecorder
{
    // A shared reference to the scores map for a single SSTable. The QueryContext stores all the relevant
    // maps. We copy the reference here to avoid a lookup on every row.
    private final LongFloatHashMap sstableRowIdScoreMap;
    private final long segmentRowIdOffset;

    /**
     * @param sstableRowIdScoreMap - a map from sstable row id to score
     * @param segmentRowIdOffset - the offset to add to the rowId to get the SS Table row id
     */
    public RowIdScoreRecorder(long segmentRowIdOffset, LongFloatHashMap sstableRowIdScoreMap)
    {
        this.sstableRowIdScoreMap = sstableRowIdScoreMap;
        this.segmentRowIdOffset = segmentRowIdOffset;
    }

    public void record(long rowId, float score)
    {
        long sstableRowId = rowId + segmentRowIdOffset;
        float previousScore = sstableRowIdScoreMap.put(sstableRowId, score);
        // Because SSTables are immutable, we should never have a rowId with a different score.
        // However, because of shadow primary keys, there is a chance that we'll store the score for the same row
        // twice.
        assert Math.abs(previousScore - 0.0f) < 0.000001f || Math.abs(previousScore - score) < 0.000001f;
    }
}
