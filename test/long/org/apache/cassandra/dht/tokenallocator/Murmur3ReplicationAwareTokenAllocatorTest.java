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

package org.apache.cassandra.dht.tokenallocator;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.dht.Murmur3Partitioner;

public class Murmur3ReplicationAwareTokenAllocatorTest extends AbstractReplicationAwareTokenAllocatorTest
{
    private static final int MAX_VNODE_COUNT = 64;

    @Test
    public void testExistingCluster()
    {
        super.testExistingCluster(new Murmur3Partitioner(), MAX_VNODE_COUNT);
    }

    @Test
    public void testNewCluster()
    {
        Util.flakyTest(this::flakyTestNewCluster,
                       2,
                       "It tends to fail sometimes due to the random selection of the tokens in the first few nodes.");
    }

    private void flakyTestNewCluster()
    {
        testNewCluster(new Murmur3Partitioner(), MAX_VNODE_COUNT);
    }
}
