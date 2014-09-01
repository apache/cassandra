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
package org.apache.cassandra.utils.btree;

import java.util.Comparator;

import org.apache.cassandra.utils.SearchIterator;

import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;

public class BTreeSearchIterator<CK, K extends CK, V> extends Path implements SearchIterator<K, V>
{
    final Comparator<CK> comparator;
    final boolean forwards;

    public BTreeSearchIterator(Object[] btree, Comparator<CK> comparator, boolean forwards)
    {
        init(btree);
        if (!forwards)
            this.indexes[0] = (byte)(getKeyEnd(path[0]) - 1);
        this.comparator = comparator;
        this.forwards = forwards;
    }

    public V next(K target)
    {
        // We could probably avoid some of the repetition but leaving that for later.
        if (forwards)
        {
            while (depth > 0)
            {
                byte successorParentDepth = findSuccessorParentDepth();
                if (successorParentDepth < 0)
                    break; // we're in last section of tree, so can only search down
                int successorParentIndex = indexes[successorParentDepth] + 1;
                Object[] successParentNode = path[successorParentDepth];
                Object successorParentKey = successParentNode[successorParentIndex];
                int c = BTree.compare(comparator, target, successorParentKey);
                if (c < 0)
                    break;
                if (c == 0)
                {
                    depth = successorParentDepth;
                    indexes[successorParentDepth]++;
                    return (V) successorParentKey;
                }
                depth = successorParentDepth;
                indexes[successorParentDepth]++;
            }
            if (find(comparator, target, Op.CEIL, true))
                return (V) currentKey();
        }
        else
        {
            while (depth > 0)
            {
                byte predecessorParentDepth = findPredecessorParentDepth();
                if (predecessorParentDepth < 0)
                    break; // we're in last section of tree, so can only search down
                int predecessorParentIndex = indexes[predecessorParentDepth] - 1;
                Object[] predecessParentNode = path[predecessorParentDepth];
                Object predecessorParentKey = predecessParentNode[predecessorParentIndex];
                int c = BTree.compare(comparator, target, predecessorParentKey);
                if (c > 0)
                    break;
                if (c == 0)
                {
                    depth = predecessorParentDepth;
                    indexes[predecessorParentDepth]--;
                    return (V) predecessorParentKey;
                }
                depth = predecessorParentDepth;
                indexes[predecessorParentDepth]--;
            }
            if (find(comparator, target, Op.FLOOR, false))
                return (V) currentKey();
        }
        return null;
    }

    public boolean hasNext()
    {
        return depth != 0 || indexes[0] != (forwards ? getKeyEnd(path[0]) : -1);
    }
}
