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

package org.apache.cassandra.db.tries;

import org.junit.BeforeClass;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

public class InMemoryRangeTrieThreadedTest extends ThreadedTestBase<TestRangeState, InMemoryRangeTrie<TestRangeState>>
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }


    @Override
    TestRangeState value(ByteComparable b)
    {
        return new TestRangeState(b, 1, 1, 1, false);
    }

    @Override
    InMemoryRangeTrie<TestRangeState> makeTrie(OpOrder readOrder)
    {
        return InMemoryRangeTrie.longLived(VERSION, readOrder);
    }

    @Override
    void add(InMemoryRangeTrie<TestRangeState> trie, ByteComparable b, TestRangeState v, int iteration) throws TrieSpaceExhaustedException
    {
        ByteComparable left = ver -> ByteSource.withTerminator(ByteSource.LT_NEXT_COMPONENT, b.asComparableBytes(ver));
        ByteComparable right = ver -> ByteSource.withTerminator(ByteSource.GT_NEXT_COMPONENT, b.asComparableBytes(ver));
        if (iteration % 2 == 0)
        {
            trie.putRecursive(left, v, (x, y) -> y.asBoundary(Direction.FORWARD));
            trie.putRecursive(right, v, (x, y) -> y.asBoundary(Direction.REVERSE));
        }
        else
            trie.apply(RangeTrie.range(left, right, VERSION, v), (x, y) -> y, x -> true);
    }
}
