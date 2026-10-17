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
package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests that a replica which does not know the index a read command was built with answers the read without the
 * index, rather than failing to deserialize the command.
 */
public class ReadCommandUnknownIndexTest extends CQLTester
{
    @Test
    public void testDeserializeWithUnknownIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String index = createIndex("CREATE INDEX ON %s (v)");

        ReadCommand command = readCommand("SELECT * FROM %s WHERE v = 1");
        assertNotNull(command.indexQueryPlan());

        DataOutputBuffer out = new DataOutputBuffer();
        ReadCommand.serializer.serialize(command, out, MessagingService.current_version);

        // the replica the command reaches has not seen the index yet, which the drop stands in for
        dropIndex("DROP INDEX %s." + index);

        try (DataInputBuffer in = new DataInputBuffer(out.getData()))
        {
            ReadCommand deserialized = ReadCommand.serializer.deserialize(in, MessagingService.current_version);
            assertNull(deserialized.indexQueryPlan());
        }
    }

    private ReadCommand readCommand(String query)
    {
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(formatQuery(query))
                                                                 .prepare(ClientState.forInternalCalls());
        return (ReadCommand) select.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
    }
}
