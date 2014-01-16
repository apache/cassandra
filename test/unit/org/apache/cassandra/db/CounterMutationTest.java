/**
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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.utils.*;

public class CounterMutationTest extends SchemaLoader
{
    @Test
    public void testGetOldShardFromSystemKeyspace() throws IOException
    {
        // Renewing a bunch of times and checking we get the same thing from
        // the system keyspace that what is in memory
        CounterId.renewLocalId();
        CounterId.renewLocalId();
        CounterId.renewLocalId();

        List<CounterId.CounterIdRecord> inMem = CounterId.getOldLocalCounterIds();
        List<CounterId.CounterIdRecord> onDisk = SystemKeyspace.getOldLocalCounterIds();

        assert inMem.equals(onDisk);
    }
}

