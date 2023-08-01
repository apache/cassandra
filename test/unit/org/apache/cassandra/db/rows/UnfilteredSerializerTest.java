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

package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.junit.Assert.assertEquals;

public class UnfilteredSerializerTest
{
    private static TableMetadata md;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();

        md = TableMetadata.builder("ks", "cf")
                          .addPartitionKeyColumn("pk", IntegerType.instance)
                          .addRegularColumn("v1", BytesType.instance)
                          .addRegularColumn("v2", BytesType.instance)
                          .build();
    }

    @Test
    public void testRowSerDe() throws IOException
    {
        // test serialization and deserialization of a row body
        testRowBodySerDe(10, Function.identity());
    }

    @Test
    public void testRowSerDeWithCorruption() throws IOException
    {
        // test serialization and deserialization of a row body when the row is corrupted in the way that the actual
        // row content is larger than the row size serialized in the preamble
        ByteBuffer largeRow = getSerializedRow(50);
        assertThatIOException().isThrownBy(() -> testRowBodySerDe(10, buf -> replaceRowContent(buf, largeRow)))
                               .withMessageMatching("EOF after \\d+ bytes out of 50");
    }

    public static void testRowBodySerDe(int cellSize, Function<ByteBuffer, ByteBuffer> transform) throws IOException
    {
        Random random = new Random();
        ByteBuffer data1 = ByteBuffer.allocate(cellSize);
        ByteBuffer data2 = ByteBuffer.allocate(cellSize);
        random.nextBytes(data1.array());
        random.nextBytes(data2.array());

        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addCell(BufferCell.live(md.regularColumns().getSimple(0), TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()), data1.duplicate()));
        builder.addCell(BufferCell.live(md.regularColumns().getSimple(1), TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()), data2.duplicate()));
        Row writtenRow = builder.build();

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            UnfilteredSerializer.serializer.serialize(writtenRow, new SerializationHelper(SerializationHeader.makeWithoutStats(md)), out, 0, MessagingService.current_version);
            out.flush();
            try (DataInputBuffer in = new DataInputBuffer(transform.apply(out.asNewBuffer()), false))
            {
                builder = BTreeRow.sortedBuilder();
                DeserializationHelper helper = new DeserializationHelper(md, MessagingService.current_version, DeserializationHelper.Flag.LOCAL, ColumnFilter.all(md));
                Unfiltered readRow = UnfilteredSerializer.serializer.deserialize(in, SerializationHeader.makeWithoutStats(md), helper, builder);
                assertEquals(writtenRow, readRow);
            }
        }
    }

    private ByteBuffer getSerializedRow(int cellSize) throws IOException
    {
        AtomicReference<ByteBuffer> rowData = new AtomicReference<>();
        testRowBodySerDe(cellSize, buf -> {
            rowData.set(buf.duplicate());
            return buf;
        });
        return rowData.get();
    }

    private static ByteBuffer replaceRowContent(ByteBuffer original, ByteBuffer replacement)
    {
        try (DataOutputBuffer out = new DataOutputBuffer();
             DataInputBuffer origIn = new DataInputBuffer(original, true);
             DataInputBuffer replIn = new DataInputBuffer(replacement, false))
        {
            out.writeByte(origIn.readByte()); // flag
            out.writeUnsignedVInt(origIn.readUnsignedVInt()); // row size
            replIn.readByte(); // skip flag
            replIn.readUnsignedVInt(); // skip row size
            out.write(replacement);
            out.close();
            return out.asNewBuffer();
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
