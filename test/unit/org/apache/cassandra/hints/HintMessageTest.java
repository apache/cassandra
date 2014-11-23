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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertEquals;

import static org.apache.cassandra.hints.HintsTestUtil.assertHintsEqual;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class HintMessageTest
{
    private static final String KEYSPACE = "hint_message_test";
    private static final String TABLE = "table";

    @Test
    public void testSerializer() throws IOException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));

        UUID hostId = UUID.randomUUID();
        long now = FBUtilities.timestampMicros();

        CFMetaData table = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        Mutation mutation =
            new RowUpdateBuilder(table, now, bytes("key"))
                .clustering("column")
                .add("val", "val" + 1234)
                .build();
        Hint hint = Hint.create(mutation, now / 1000);
        HintMessage message = new HintMessage(hostId, hint);

        // serialize
        int serializedSize = (int) HintMessage.serializer.serializedSize(message, MessagingService.current_version);
        DataOutputBuffer dob = new DataOutputBuffer();
        HintMessage.serializer.serialize(message, dob, MessagingService.current_version);
        assertEquals(serializedSize, dob.getLength());

        // deserialize
        DataInputPlus di = new DataInputBuffer(dob.buffer(), true);
        HintMessage deserializedMessage = HintMessage.serializer.deserialize(di, MessagingService.current_version);

        // compare before/after
        assertEquals(hostId, deserializedMessage.hostId);
        assertHintsEqual(message.hint, deserializedMessage.hint);
    }
}
