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

import java.io.IOException;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

public class RowIndexEntryTest extends SchemaLoader
{
    @Test
    public void testSerializedSize() throws IOException
    {
        final RowIndexEntry simple = new RowIndexEntry(123);

        DataOutputBuffer buffer = new DataOutputBuffer();
        RowIndexEntry.Serializer serializer = new RowIndexEntry.Serializer(new SimpleDenseCellNameType(UTF8Type.instance));

        serializer.serialize(simple, buffer);

        Assert.assertEquals(buffer.getLength(), serializer.serializedSize(simple));

        buffer = new DataOutputBuffer();
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        ColumnIndex columnIndex = new ColumnIndex.Builder(cf, ByteBufferUtil.bytes("a"), new DataOutputBuffer())
        {{
            int idx = 0, size = 0;
            Cell column;
            do
            {
                column = new BufferCell(CellNames.simpleDense(ByteBufferUtil.bytes("c" + idx++)), ByteBufferUtil.bytes("v"), FBUtilities.timestampMicros());
                size += column.serializedSize(new SimpleDenseCellNameType(UTF8Type.instance), TypeSizes.NATIVE);

                add(column);
            }
            while (size < DatabaseDescriptor.getColumnIndexSize() * 3);

        }}.build();

        RowIndexEntry withIndex = RowIndexEntry.create(0xdeadbeef, DeletionTime.LIVE, columnIndex);

        serializer.serialize(withIndex, buffer);
        Assert.assertEquals(buffer.getLength(), serializer.serializedSize(withIndex));
    }
}
