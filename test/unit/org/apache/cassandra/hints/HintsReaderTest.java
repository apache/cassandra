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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
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
        CFMetaData table = Schema.instance.getCFMetaData(ks, tb);
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

    private void readHints(int num, int numTable) throws IOException
    {
        long baseTimestamp = descriptor.timestamp;
        int index = 0;

        try (HintsReader reader = HintsReader.open(new File(directory, descriptor.fileName())))
        {
            for (HintsReader.Page page : reader)
            {
                Iterator<Hint> hints = page.hintsIterator();
                while (hints.hasNext())
                {
                    int i = index / numTable;
                    Hint hint = hints.next();

                    long timestamp = baseTimestamp + i;
                    Mutation mutation = hint.mutation;

                    assertEquals(timestamp, hint.creationTime);
                    assertEquals(dk(bytes(i)), mutation.key());

                    Row row = mutation.getPartitionUpdates().iterator().next().iterator().next();
                    assertEquals(1, Iterables.size(row.cells()));
                    assertEquals(bytes(i), row.clustering().get(0));
                    Cell cell = row.cells().iterator().next();
                    assertNotNull(cell);
                    assertEquals(bytes(i), cell.value());
                    assertEquals(timestamp * 1000, cell.timestamp());

                    index++;
                }
            }
        }

        assertEquals(index, num);
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
        directory = Files.createTempDirectory(null).toFile();
        try
        {
            generateHints(3, ks);
            readHints(3 * numTable, numTable);
        }
        finally
        {
            directory.delete();
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
        directory = Files.createTempDirectory(null).toFile();
        try
        {
            generateHints(3, ks);
            Schema.instance.dropTable(ks, CF_STANDARD1);
            readHints(3, 1);
        }
        finally
        {
            directory.delete();
        }
    }
}
