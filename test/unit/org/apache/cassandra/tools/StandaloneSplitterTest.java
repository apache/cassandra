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

package org.apache.cassandra.tools;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StandaloneSplitterTest extends ToolsTester
{
    @BeforeClass
    public static void before()
    {
        // the legacy tables use a different partitioner :(
        // (Don't use ByteOrderedPartitioner.class.getName() as that would initialize the class and work
        // against the goal of this test to check classes and threads initialized by the tool.)
        System.setProperty("cassandra.partitioner", "org.apache.cassandra.dht.ByteOrderedPartitioner");
    }

    @Test
    public void testStandaloneSplitter_NoArgs()
    {
        runTool(1, "org.apache.cassandra.tools.StandaloneSplitter");
        assertNoUnexpectedThreadsStarted(null, null);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    // Note: StandaloneSplitter modifies sstables
}
