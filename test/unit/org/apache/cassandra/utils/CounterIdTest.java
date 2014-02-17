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
package org.apache.cassandra.utils;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.SystemKeyspace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterIdTest extends SchemaLoader
{
    @Test
    public void testGetCurrentIdFromSystemKeyspace()
    {
        // Renewing a bunch of times and checking we get the same thing from
        // the system keyspace that what is in memory
        CounterId id0 = CounterId.getLocalId();
        assertEquals(id0, SystemKeyspace.getCurrentLocalCounterId());

        CounterId.renewLocalId();
        CounterId id1 = CounterId.getLocalId();
        assertEquals(id1, SystemKeyspace.getCurrentLocalCounterId());
        assertTrue(id1.compareTo(id0) == 1);

        CounterId.renewLocalId();
        CounterId id2 = CounterId.getLocalId();
        assertEquals(id2, SystemKeyspace.getCurrentLocalCounterId());
        assertTrue(id2.compareTo(id1) == 1);
    }
}

