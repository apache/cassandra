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

import org.apache.cassandra.utils.AbstractIterator;

import io.github.jbellis.jvector.graph.NeighborQueue;

/**
 * An iterator over {@link RowIdWithScore} that lazily consumes a {@link NeighborQueue}.
 */
public class NeighborQueueRowIdIterator extends AbstractIterator<RowIdWithScore>
{
    private final NeighborQueue scoreQueue;

    public NeighborQueueRowIdIterator(NeighborQueue scoreQueue)
    {
        this.scoreQueue = scoreQueue;
    }

    @Override
    protected RowIdWithScore computeNext()
    {
        if (scoreQueue.size() == 0)
            return endOfData();
        float score = scoreQueue.topScore();
        int rowId = scoreQueue.pop();
        return new RowIdWithScore(rowId, score);
    }
}
