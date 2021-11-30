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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

public class BatchlogTest
{
    private static final String KEYSPACE = "BatchRequestTest";
    private static final String CF_STANDARD = "Standard";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD, 1, BytesType.instance));
    }

    @Test
    public void testSerialization() throws IOException
    {
        TableMetadata cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_STANDARD).metadata();

        long now = FBUtilities.timestampMicros();
        int version = MessagingService.current_version;
        TimeUUID uuid = nextTimeUUID();

        List<Mutation> mutations = new ArrayList<>(10);
        for (int i = 0; i < 10; i++)
        {
            mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), bytes(i))
                          .clustering("name" + i)
                          .add("val", "val" + i)
                          .build());
        }

        Batch batch1 = Batch.createLocal(uuid, now, mutations);
        assertEquals(uuid, batch1.id);
        assertEquals(now, batch1.creationTime);
        assertEquals(mutations, batch1.decodedMutations);

        DataOutputBuffer out = new DataOutputBuffer();
        Batch.serializer.serialize(batch1, out, version);

        assertEquals(out.getLength(), Batch.serializer.serializedSize(batch1, version));

        DataInputPlus dis = new DataInputBuffer(out.getData());
        Batch batch2 = Batch.serializer.deserialize(dis, version);

        assertEquals(batch1.id, batch2.id);
        assertEquals(batch1.creationTime, batch2.creationTime);
        assertEquals(batch1.decodedMutations.size(), batch2.encodedMutations.size());

        Iterator<Mutation> it1 = batch1.decodedMutations.iterator();
        Iterator<ByteBuffer> it2 = batch2.encodedMutations.iterator();
        while (it1.hasNext())
        {
            try (DataInputBuffer in = new DataInputBuffer(it2.next().array()))
            {
                assertEquals(it1.next().toString(), Mutation.serializer.deserialize(in, version).toString());
            }
        }
    }
}
