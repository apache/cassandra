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
package org.apache.cassandra.index.sai.memory;

import java.util.Arrays;
import java.util.PriorityQueue;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;

public class PriorityInMemoryKeyRangeIteratorTest extends AbstractInMemoryKeyRangeIteratorTester
{
    @Override
    protected KeyRangeIterator makeIterator(long minimumTokenValue, long maximumTokenValue, long... tokens)
    {
        PriorityQueue<PrimaryKey> queue = new PriorityQueue<>(tokens.length);

        Arrays.stream(tokens).forEach(t -> queue.add(keyForToken(t)));

        return new InMemoryKeyRangeIterator(primaryKeyFactory.create(new Murmur3Partitioner.LongToken(minimumTokenValue)),
                                            primaryKeyFactory.create(new Murmur3Partitioner.LongToken(maximumTokenValue)),
                                            queue);
    }
}
