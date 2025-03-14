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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;

/**
 * Tests to sanity-check the integration points with Journal
 * (mutation id and mutation ser/de, comparison, etc.)
 */
public class MutationJournalTest
{
    private static final String KEYSPACE = "mjtks";
    private static final String TABLE = "mjtt";

    private static MutationJournal journal;

    @BeforeClass
    public static void setUp() throws IOException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(3),
                                    TableMetadata.builder(KEYSPACE, TABLE)
                                                 .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                 .addClusteringColumn("ck", UTF8Type.instance)
                                                 .addRegularColumn("value", UTF8Type.instance)
                                                 .build());

        File directory = new File(Files.createTempDirectory("mutation-journal-test-simple"));
        directory.deleteRecursiveOnExit();

        journal = new MutationJournal(directory, TestParams.INSTANCE);
        journal.start();
    }

    @AfterClass
    public static void tearDown()
    {
        journal.shutdownBlocking();
    }

    @Test
    public void testWriteOneReadOne()
    {
        Mutation expected =
            new RowUpdateBuilder(Schema.instance.getTableMetadata(KEYSPACE, TABLE), 0, "key")
               .clustering("ck")
               .add("value", "value")
               .build();

        ShortMutationId id = new ShortMutationId(100L, 0);
        journal.write(id, expected);

        // regular read
        Mutation actual = journal.read(id);
        assertMutationEquals(expected, actual);

        // read via RecordConsumer
        journal.read(id, ((segment, position, key, buffer, userVersion) ->
                          {
                              assertEquals(id, key);
                              assertEquals(serialize(expected), buffer);
                          }));
    }

    @Test
    public void testWriteManyReadMany()
    {
        Mutation expected1 =
            new RowUpdateBuilder(Schema.instance.getTableMetadata(KEYSPACE, TABLE), 0, "key1")
               .clustering("ck1")
               .add("value", "value1")
               .build();
        Mutation expected2 =
            new RowUpdateBuilder(Schema.instance.getTableMetadata(KEYSPACE, TABLE), 0, "key2")
               .clustering("ck2")
               .add("value", "value2")
               .build();
        List<Mutation> expected = List.of(expected1, expected2);

        ShortMutationId id1 = new ShortMutationId(100L, 1);
        ShortMutationId id2 = new ShortMutationId(100L, 2);
        List<ShortMutationId> ids = List.of(id1, id2);

        journal.write(id1, expected1);
        journal.write(id2, expected2);

        List<Mutation> actual = new ArrayList<>();
        journal.readAll(ids, actual);
        assertMutationsEqual(expected, actual);
    }

    private static void assertMutationEquals(Mutation expected, Mutation actual)
    {
        assertEquals(serialize(expected), serialize(actual));
    }

    private static void assertMutationsEqual(List<Mutation> expected, List<Mutation> actual)
    {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
            assertMutationEquals(expected.get(i), actual.get(i));
    }

    private static ByteBuffer serialize(Mutation mutation)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            Mutation.serializer.serialize(mutation, out, MessagingService.maximum_version);
            return out.asNewBuffer();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }
}
