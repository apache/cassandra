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
import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

import io.github.jbellis.jvector.graph.SearchResult;

/**
 * An iterator over {@link RowIdWithScore} sorted by score descending. The iterator converts ordinals (node ids) to
 * segment row ids and pairs them with the score given by the index.
 */
public class NodeScoreToRowIdWithScoreIterator extends AbstractIterator<RowIdWithScore>
{
    private final CloseableIterator<SearchResult.NodeScore> nodeScores;
    private final OnDiskOrdinalsMap.RowIdsView rowIdsView;

    private PrimitiveIterator.OfInt segmentRowIdIterator = IntStream.empty().iterator();
    private float currentScore;

    public NodeScoreToRowIdWithScoreIterator(CloseableIterator<SearchResult.NodeScore> nodeScores,
                                             OnDiskOrdinalsMap.RowIdsView rowIdsView)
    {
        this.nodeScores = nodeScores;
        this.rowIdsView = rowIdsView;
    }

    @Override
    protected RowIdWithScore computeNext()
    {
        try
        {
            if (segmentRowIdIterator.hasNext())
                return new RowIdWithScore(segmentRowIdIterator.nextInt(), currentScore);

            while (nodeScores.hasNext())
            {
                SearchResult.NodeScore result = nodeScores.next();
                currentScore = result.score;
                int ordinal = result.node;
                segmentRowIdIterator = Arrays.stream(rowIdsView.getSegmentRowIdsMatching(ordinal)).iterator();
                if (segmentRowIdIterator.hasNext())
                    return new RowIdWithScore(segmentRowIdIterator.nextInt(), currentScore);
            }
            return endOfData();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(rowIdsView);
        FileUtils.closeQuietly(nodeScores);
    }
}
