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

package org.apache.cassandra.db.memtable;

import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.SlabPool;

public class MemtableSizeHeapBuffersTest extends MemtableSizeTestBase
{
    // Overrides CQLTester.setUpClass to run before it
    @BeforeClass
    public static void setUpClass()
    {
        setup(Config.MemtableAllocationType.heap_buffers);
    }

    @Override
    void checkMemtablePool()
    {
        MemtablePool memoryPool = AbstractAllocatorMemtable.MEMORY_POOL;
        logger.info("Memtable pool {} off-heap limit {}", memoryPool, memoryPool.offHeap.limit);
        Assert.assertTrue(memoryPool instanceof SlabPool);
        Assert.assertEquals(0, memoryPool.offHeap.limit);
    }
}
