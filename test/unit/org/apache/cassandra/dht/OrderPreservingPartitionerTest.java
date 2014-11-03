/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.dht;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;

public class OrderPreservingPartitionerTest extends PartitionerTestCase
{
    @BeforeClass
    public static void cleanStatesFromPreviousTest()
    {
        // Since OrderPreservingPartitioner#describeOwnership tries to read SSTables,
        // we need to clear data dir to clear garbage from previous test before running tests.
        SchemaLoader.cleanupAndLeaveDirs();
    }

    public void initPartitioner()
    {
        partitioner = new OrderPreservingPartitioner();
    }

    @Test
    public void testCompare()
    {
        assert tok("").compareTo(tok("asdf")) < 0;
        assert tok("asdf").compareTo(tok("")) > 0;
        assert tok("").compareTo(tok("")) == 0;
        assert tok("z").compareTo(tok("a")) > 0;
        assert tok("a").compareTo(tok("z")) < 0;
        assert tok("asdf").compareTo(tok("asdf")) == 0;
        assert tok("asdz").compareTo(tok("asdf")) > 0;
    }
}
