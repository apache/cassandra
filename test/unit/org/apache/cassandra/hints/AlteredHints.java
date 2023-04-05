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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.cassandra.io.util.File;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Base class for testing compressed and encrypted hints.
 */
public abstract class AlteredHints
{
    protected static final String KEYSPACE = "hints_compression_test";
    private static final String TABLE = "table";

    private static Mutation createMutation(int index, long timestamp)
    {
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        return new RowUpdateBuilder(table, timestamp, bytes(index))
               .clustering(bytes(index))
               .add("val", bytes(index))
               .build();
    }

    private static Hint createHint(int idx, long baseTimestamp)
    {
        long timestamp = baseTimestamp + idx;
        return Hint.create(createMutation(idx, TimeUnit.MILLISECONDS.toMicros(timestamp)), timestamp);
    }

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    abstract ImmutableMap<String, Object> params();
    abstract boolean looksLegit(HintsWriter writer);
    abstract boolean looksLegit(ChecksummedDataInput checksummedDataInput);

    public void multiFlushAndDeserializeTest() throws Exception
    {
        int hintNum = 0;
        int bufferSize = HintsWriteExecutor.WRITE_BUFFER_SIZE;
        List<Hint> hints = new LinkedList<>();

        UUID hostId = UUID.randomUUID();
        long ts = System.currentTimeMillis();

        HintsDescriptor descriptor = new HintsDescriptor(hostId, ts, params());
        File dir = new File(Files.createTempDir());
        try (HintsWriter writer = HintsWriter.create(dir, descriptor))
        {
            Assert.assertTrue(looksLegit(writer));

            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
            try (HintsWriter.Session session = writer.newSession(writeBuffer))
            {
                while (session.getBytesWritten() < bufferSize * 3)
                {
                    Hint hint = createHint(hintNum, ts+hintNum);
                    session.append(hint);
                    hints.add(hint);
                    hintNum++;
                }
            }
        }

        try (HintsReader reader = HintsReader.open(descriptor.file(dir)))
        {
            Assert.assertTrue(looksLegit(reader.getInput()));
            List<Hint> deserialized = new ArrayList<>(hintNum);
            List<InputPosition> pagePositions = new ArrayList<>(hintNum);

            for (HintsReader.Page page: reader)
            {
                pagePositions.add(page.position);
                Iterator<Hint> iterator = page.hintsIterator();
                while (iterator.hasNext())
                {
                    deserialized.add(iterator.next());
                }
            }

            Assert.assertEquals(hints.size(), deserialized.size());
            hintNum = 0;
            for (Hint expected: hints)
            {
                HintsTestUtil.assertHintsEqual(expected, deserialized.get(hintNum));
                hintNum++;
            }

            // explicitely seek to each page by iterating collected page positions and check if hints still match as expected
            int hintOffset = 0;
            for (InputPosition pos : pagePositions)
            {
                reader.seek(pos);
                HintsReader.Page page = reader.iterator().next();
                Iterator<Hint> iterator = page.hintsIterator();
                while (iterator.hasNext())
                {
                    Hint seekedHint = iterator.next();
                    HintsTestUtil.assertHintsEqual(hints.get(hintOffset), seekedHint);
                    hintOffset++;
                }
            }
        }
    }
}
