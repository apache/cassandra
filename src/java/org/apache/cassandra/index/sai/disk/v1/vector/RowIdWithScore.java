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

package org.apache.cassandra.index.sai.disk.v1.vector;

import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Represents a row id with its computed score.
 */
public class RowIdWithScore
{
    private final int segmentRowId;
    private final float score;

    public RowIdWithScore(int segmentRowId, float score)
    {
        this.segmentRowId = segmentRowId;
        this.score = score;
    }

    public PrimaryKeyWithScore toPrimaryKeyWithScore(ColumnMetadata columnMetadata,
                                                     SSTableId sstableId,
                                                     PrimaryKeyMap primaryKeyMap,
                                                     long segmentRowIdOffset)
    {
        PrimaryKey pk = primaryKeyMap.primaryKeyFromRowId(segmentRowIdOffset + segmentRowId);
        return new PrimaryKeyWithScore(columnMetadata, sstableId, pk, score);
    }
}
