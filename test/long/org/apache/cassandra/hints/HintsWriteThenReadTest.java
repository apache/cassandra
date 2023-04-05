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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import com.google.common.collect.Iterables;

import org.apache.cassandra.io.util.File;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class HintsWriteThenReadTest
{
    private static final String KEYSPACE = "hints_write_then_read_test";
    private static final String TABLE = "table";

    private static final int HINTS_COUNT = 10_000_000;

    @Test
    public void testWriteReadCycle() throws IOException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));

        HintsDescriptor descriptor = new HintsDescriptor(UUID.randomUUID(), currentTimeMillis());

        File directory = new File(Files.createTempDirectory(null));
        try
        {
            testWriteReadCycle(directory, descriptor);
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    private void testWriteReadCycle(File directory, HintsDescriptor descriptor) throws IOException
    {
        // write HINTS_COUNT hints to a file
        writeHints(directory, descriptor);

        // calculate the checksum of the file, then compare to the .crc32 checksum file content
        verifyChecksum(directory, descriptor);

        // iterate over the written hints, make sure they are all present
        verifyHints(directory, descriptor);
    }

    private void writeHints(File directory, HintsDescriptor descriptor) throws IOException
    {
        try (HintsWriter writer = HintsWriter.create(directory, descriptor))
        {
            write(writer, descriptor.timestamp);
        }
    }

    private static void verifyChecksum(File directory, HintsDescriptor descriptor) throws IOException
    {
        File hintsFile = descriptor.file(directory);
        File checksumFile = descriptor.checksumFile(directory);

        assertTrue(checksumFile.exists());

        String actualChecksum = Integer.toHexString(calculateChecksum(hintsFile));
        String expectedChecksum = Files.readAllLines(checksumFile.toPath()).iterator().next();

        assertEquals(expectedChecksum, actualChecksum);
    }

    private void verifyHints(File directory, HintsDescriptor descriptor)
    {
        long baseTimestamp = descriptor.timestamp;
        int index = 0;

        try (HintsReader reader = HintsReader.open(descriptor.file(directory)))
        {
            for (HintsReader.Page page : reader)
            {
                Iterator<Hint> hints = page.hintsIterator();
                while (hints.hasNext())
                {
                    Hint hint = hints.next();

                    long timestamp = baseTimestamp + index;
                    Mutation mutation = hint.mutation;

                    assertEquals(timestamp, hint.creationTime);
                    assertEquals(dk(bytes(index)), mutation.key());

                    Row row = mutation.getPartitionUpdates().iterator().next().iterator().next();
                    assertEquals(1, Iterables.size(row.cells()));
                    assertEquals(bytes(index), toByteBuffer(row.clustering().get(0)));
                    Cell<?> cell = row.cells().iterator().next();
                    assertNotNull(cell);
                    assertEquals(bytes(index), toByteBuffer(cell.value()));
                    assertEquals(timestamp * 1000, cell.timestamp());

                    index++;
                }
            }
        }

        assertEquals(index, HINTS_COUNT);
    }

    private void write(HintsWriter writer, long timestamp) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);
        try (HintsWriter.Session session = writer.newSession(buffer))
        {
            write(session, timestamp);
        }
        FileUtils.clean(buffer);
    }

    private void write(HintsWriter.Session session, long timestamp) throws IOException
    {
        for (int i = 0; i < HINTS_COUNT; i++)
            session.append(createHint(i, timestamp));
    }

    private static Hint createHint(int idx, long baseTimestamp)
    {
        long timestamp = baseTimestamp + idx;
        return Hint.create(createMutation(idx, TimeUnit.MILLISECONDS.toMicros(timestamp)), timestamp);
    }

    private static Mutation createMutation(int index, long timestamp)
    {
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        return new RowUpdateBuilder(table, timestamp, bytes(index))
               .clustering(bytes(index))
               .add("val", bytes(index))
               .build();
    }

    private static int calculateChecksum(File file) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[FBUtilities.MAX_UNSIGNED_SHORT];

        try (InputStream in = Files.newInputStream(file.toPath()))
        {
            int bytesRead;
            while((bytesRead = in.read(buffer)) != -1)
                crc.update(buffer, 0, bytesRead);
        }

        return (int) crc.getValue();
    }

    private ByteBuffer toByteBuffer(Object obj)
    {
        if (obj instanceof ByteBuffer)
           return (ByteBuffer) obj;

        return ByteBuffer.wrap((byte[]) obj);
    }
}
