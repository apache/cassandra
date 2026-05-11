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

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * An iterator over scored primary keys ordered by the score descending
 * Not skippable.
 */
public class RowIdToPrimaryKeyWithScoreIterator extends AbstractIterator<PrimaryKeyWithScore>
{
    private final ColumnMetadata column;
    private final SSTableId sstableId;
    private final PrimaryKeyMap primaryKeyMap;
    private final CloseableIterator<RowIdWithScore> scoredRowIdIterator;
    private final long segmentRowIdOffset;

    public RowIdToPrimaryKeyWithScoreIterator(ColumnMetadata column,
                                              PrimaryKeyMap.Factory primaryKeyMapFactory,
                                              CloseableIterator<RowIdWithScore> scoredRowIdIterator,
                                              long segmentRowIdOffset) throws IOException
    {
        this.column = column;
        this.scoredRowIdIterator = scoredRowIdIterator;
        this.primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
        this.sstableId = primaryKeyMap.getSSTableId();
        this.segmentRowIdOffset = segmentRowIdOffset;
    }

    @Override
    protected PrimaryKeyWithScore computeNext()
    {
        if (!scoredRowIdIterator.hasNext())
            return endOfData();
        RowIdWithScore rowIdWithScore = scoredRowIdIterator.next();
        return rowIdWithScore.toPrimaryKeyWithScore(column, sstableId, primaryKeyMap, segmentRowIdOffset);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(primaryKeyMap);
        FileUtils.closeQuietly(scoredRowIdIterator);
    }
}
