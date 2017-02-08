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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;

public class LegacyLayoutTest
{
    @Test
    public void testFromUnfilteredRowIterator() throws Throwable
    {
        CFMetaData table = CFMetaData.Builder.create("ks", "table")
                                             .withPartitioner(Murmur3Partitioner.instance)
                                             .addPartitionKey("k", Int32Type.instance)
                                             .addRegularColumn("a", SetType.getInstance(Int32Type.instance, true))
                                             .addRegularColumn("b", SetType.getInstance(Int32Type.instance, true))
                                             .build();

        ColumnDefinition a = table.getColumnDefinition(new ColumnIdentifier("a", false));
        ColumnDefinition b = table.getColumnDefinition(new ColumnIdentifier("b", false));

        Row.Builder builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(a, new DeletionTime(1L, 1));
        builder.addComplexDeletion(b, new DeletionTime(1L, 1));
        Row row = builder.build();

        ByteBuffer key = ByteBufferUtil.bytes(1);
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(table, key, row);

        LegacyLayout.LegacyUnfilteredPartition p = LegacyLayout.fromUnfilteredRowIterator(null, upd.unfilteredIterator());
        assertEquals(DeletionTime.LIVE, p.partitionDeletion);
        assertEquals(0, p.cells.size());

        LegacyLayout.LegacyRangeTombstoneList l = p.rangeTombstones;
        assertEquals("a", l.starts[0].collectionName.name.toString());
        assertEquals("a", l.ends[0].collectionName.name.toString());

        assertEquals("b", l.starts[1].collectionName.name.toString());
        assertEquals("b", l.ends[1].collectionName.name.toString());
    }
}