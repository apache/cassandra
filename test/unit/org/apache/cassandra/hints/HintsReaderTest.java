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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.Iterables;
import org.apache.cassandra.io.util.File;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.ValueAccessors;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class HintsReaderTest
{
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

    private static HintsDescriptor descriptor;

    private static File directory;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();

        descriptor = new HintsDescriptor(UUID.randomUUID(), System.currentTimeMillis());
    }

    private static Mutation createMutation(int index, long timestamp, String ks, String tb)
    {
        TableMetadata table = Schema.instance.getTableMetadata(ks, tb);
        return new RowUpdateBuilder(table, timestamp, bytes(index))
               .clustering(bytes(index))
               .add("val", bytes(index))
               .build();
    }

    private void generateHints(int num, String ks) throws IOException
    {
        try (HintsWriter writer = HintsWriter.create(directory, descriptor))
        {
            ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);
            try (HintsWriter.Session session = writer.newSession(buffer))
            {
                for (int i = 0; i < num; i++)
                {
                    long timestamp = descriptor.timestamp + i;
                    Mutation m = createMutation(i, TimeUnit.MILLISECONDS.toMicros(timestamp), ks, CF_STANDARD1);
                    session.append(Hint.create(m, timestamp));
                    m = createMutation(i, TimeUnit.MILLISECONDS.toMicros(timestamp), ks, CF_STANDARD2);
                    session.append(Hint.create(m, timestamp));
                }
            }
            FileUtils.clean(buffer);
        }
    }

    private void readHints(int num, int numTable)
    {
        readAndVerify(num, numTable, HintsReader.Page::hintsIterator);
        readAndVerify(num, numTable, this::deserializePageBuffers);
    }

    private void readAndVerify(int num, int numTable, Function<HintsReader.Page, Iterator<Hint>> getHints)
    {
        long baseTimestamp = descriptor.timestamp;
        int index = 0;
        try (HintsReader reader = HintsReader.open(descriptor.file(directory)))
        {
            for (HintsReader.Page page : reader)
            {
                Iterator<Hint> hints = getHints.apply(page);
                while (hints.hasNext())
                {
                    int i = index / numTable;
                    Hint hint = hints.next();
                    if (hint != null)
                    {
                        verifyHint(hint, baseTimestamp, i);
                        index++;
                    }
                }
            }
        }
        assertEquals(index, num);
    }

    private void verifyHint(Hint hint, long baseTimestamp, int i)
    {
        long timestamp = baseTimestamp + i;
        Mutation mutation = hint.mutation;

        assertEquals(timestamp, hint.creationTime);
        assertEquals(dk(bytes(i)), mutation.key());

        Row row = mutation.getPartitionUpdates().iterator().next().iterator().next();
        assertEquals(1, Iterables.size(row.cells()));
        ValueAccessors.assertDataEquals(bytes(i), row.clustering().get(0));
        Cell<?> cell = row.cells().iterator().next();
        assertNotNull(cell);
        ValueAccessors.assertDataEquals(bytes(i), cell.buffer());
        assertEquals(timestamp * 1000, cell.timestamp());
    }


    private Iterator<Hint> deserializePageBuffers(HintsReader.Page page)
    {
        final Iterator<ByteBuffer> buffers = page.buffersIterator();
        return new Iterator<Hint>()
        {
            public boolean hasNext()
            {
                return buffers.hasNext();
            }

            public Hint next()
            {
                try
                {
                    return Hint.serializer.deserialize(new DataInputBuffer(buffers.next(), false),
                                                       descriptor.messagingVersion());
                }
                catch (UnknownTableException e)
                {
                    return null; // ignore
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Unexpected error deserializing hint", e);
                }
            }
        };
    }

    @Test
    public void corruptFile() throws IOException
    {
        corruptFileHelper(new byte[100], "corruptFile");
    }

    @Test(expected = FSReadError.class)
    public void corruptFileNotAllZeros() throws IOException
    {
        byte [] bs = new byte[100];
        bs[50] = 1;
        corruptFileHelper(bs, "corruptFileNotAllZeros");
    }

    private void corruptFileHelper(byte[] toAppend, String ks) throws IOException
    {
        SchemaLoader.createKeyspace(ks,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(ks, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(ks, CF_STANDARD2));
        int numTable = 2;
        directory = new File(Files.createTempDirectory(null));
        try
        {
            generateHints(3, ks);
            File hintFile = new File(directory, descriptor.fileName());
            Files.write(hintFile.toPath(), toAppend, StandardOpenOption.APPEND);
            readHints(3 * numTable, numTable);
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void testNormalRead() throws IOException
    {
        String ks = "testNormalRead";
        SchemaLoader.createKeyspace(ks,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(ks, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(ks, CF_STANDARD2));
        int numTable = 2;
        directory = new File(Files.createTempDirectory(null));
        try
        {
            generateHints(3, ks);
            readHints(3 * numTable, numTable);
        }
        finally
        {
            directory.tryDelete();
        }
    }

    @Test
    public void testDroppedTableRead() throws IOException
    {
        String ks = "testDroppedTableRead";
        SchemaLoader.createKeyspace(ks,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(ks, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(ks, CF_STANDARD2));

        directory = new File(Files.createTempDirectory(null));
        try
        {
            generateHints(3, ks);
            SchemaTestUtil.announceTableDrop(ks, CF_STANDARD1);
            readHints(3, 1);
        }
        finally
        {
            directory.tryDelete();
        }
    }
}
