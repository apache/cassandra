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

package org.apache.cassandra.cql3;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.SystemKeyspace.BATCHES;
import static org.apache.cassandra.db.SystemKeyspace.LOCAL;
import static org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;
import static org.apache.cassandra.db.SystemKeyspace.SNAPSHOT_TABLE_NAME;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;

public class SystemKeyspaceQueryTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    @Test
    public void testSelectsWithDifferentPartitioners()
    {
        // Verify that querying tables which use ReversedLongLocalPartitioner doesn't cause an error
        assertRowCountNet(executeNet(String.format("SELECT * FROM %s.%s WHERE epoch = 1",
                                                   SYSTEM_KEYSPACE_NAME, METADATA_LOG)), 0);
        assertRowCountNet(executeNet(String.format("SELECT * FROM %s.%s WHERE epoch = 1",
                                                   SYSTEM_KEYSPACE_NAME, SNAPSHOT_TABLE_NAME)), 0);
        // system.batches table uses LocalPartitioner
        assertRowCountNet(executeNet(String.format("SELECT * FROM %s.%s WHERE id = %s",
                                                   SYSTEM_KEYSPACE_NAME, BATCHES,
                                                   TimeUUID.Generator.nextTimeUUID())), 0);
        // Query a table using the global system partitioner
        assertRowCountNet(executeNet(String.format("SELECT * FROM %s.%s WHERE key = 'invalidkey'",
                                                   SYSTEM_KEYSPACE_NAME, LOCAL)), 0);
    }
}
