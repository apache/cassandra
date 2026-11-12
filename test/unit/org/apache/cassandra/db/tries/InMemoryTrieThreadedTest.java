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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

public class InMemoryTrieThreadedTest extends ThreadedTestBase<String, InMemoryTrie<String>>
{
    @Override
    String value(ByteComparable b)
    {
        return b.byteComparableAsString(VERSION);
    }


    @Override
    InMemoryTrie<String> makeTrie(OpOrder readOrder)
    {
        return InMemoryTrie.longLived(VERSION, readOrder);
    }

    @Override
    void add(InMemoryTrie<String> trie, ByteComparable b, String v, int iteration) throws TrieSpaceExhaustedException
    {
        trie.putSingleton(b, v, (x, y) -> y, iteration % 2 != 0);
    }
}
