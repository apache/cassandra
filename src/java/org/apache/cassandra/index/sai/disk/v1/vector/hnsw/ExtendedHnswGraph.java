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

package org.apache.cassandra.index.sai.disk.v1.vector.hnsw;

import org.apache.cassandra.utils.ObjectSizes;

import java.io.IOException;
import java.util.Arrays;

public abstract class ExtendedHnswGraph extends HnswGraph
{
    public abstract int getNeighborCount(int level, int node) throws IOException;

    /** non-test classes should override this */
    public long ramBytesUsed()
    {
        return ObjectSizes.measureDeep(this);
    }

    static int[] getSortedNodes(HnswGraph hnsw, int level) {
        NodesIterator nodesOnLevel = null;
        try
        {
            nodesOnLevel = hnsw.getNodesOnLevel(level);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        var sortedNodes = new int[nodesOnLevel.size()];

        for(var n = 0; nodesOnLevel.hasNext(); n++) {
            sortedNodes[n] = nodesOnLevel.nextInt();
        }

        Arrays.sort(sortedNodes);
        return sortedNodes;
    }
}
