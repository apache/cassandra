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
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
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
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchTest
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
        CFMetaData cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_STANDARD).metadata;

        long now = FBUtilities.timestampMicros();
        int version = MessagingService.current_version;
        UUID uuid = UUIDGen.getTimeUUID();

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

    /**
     * This is just to test decodeMutations() when deserializing,
     * since Batch will never be serialized at a version 2.2.
     * @throws IOException
     */
    @Test
    public void testSerializationNonCurrentVersion() throws IOException
    {
        CFMetaData cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_STANDARD).metadata;

        long now = FBUtilities.timestampMicros();
        int version = MessagingService.VERSION_22;
        UUID uuid = UUIDGen.getTimeUUID();

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
        assertEquals(batch1.decodedMutations.size(), batch2.decodedMutations.size());

        Iterator<Mutation> it1 = batch1.decodedMutations.iterator();
        Iterator<Mutation> it2 = batch2.decodedMutations.iterator();
        while (it1.hasNext())
        {
            // We can't simply test the equality of both mutation string representation, that is do:
            //   assertEquals(it1.next().toString(), it2.next().toString());
            // because when deserializing from the old format, the returned iterator will always have it's 'columns()'
            // method return all the table columns (no matter what's the actual content), and the table contains a
            // 'val0' column we're not setting in that test.
            //
            // And it's actually not easy to fix legacy deserialization as we'd need to know which columns are actually
            // set upfront, which would require use to iterate over the whole content first, which would be costly. And
            // as the result of 'columns()' is only meant as a superset of the columns in the iterator, we don't bother.
            Mutation mut1 = it1.next();
            Mutation mut2 = it2.next();
            assertTrue(mut1 + " != " + mut2, Util.sameContent(mut1, mut2));
        }
    }
}
