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
package org.apache.cassandra.service.pager;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RandomizedPagingStateTest
{
    private static final Random rnd = new Random();
    private static final int ROUNDS = 50_000;
    private static final int MAX_PK_SIZE = 3000;
    private static final int MAX_CK_SIZE = 3000;
    private static final int MAX_REMAINING = 5000;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testFormatChecksPkOnly()
    {
        rnd.setSeed(1);
        TableMetadata metadata = TableMetadata.builder("ks", "tbl")
                                              .addPartitionKeyColumn("pk", BytesType.instance)
                                              .addRegularColumn("v", LongType.instance)
                                              .build();
        Row row =  BTreeRow.emptyRow(Clustering.EMPTY);
        for (int i = 0; i < ROUNDS; i++)
            checkState(metadata, MAX_PK_SIZE, row);
    }

    @Test
    public void testFormatChecksPkAndCk()
    {
        rnd.setSeed(1);
        TableMetadata metadata = TableMetadata.builder("ks", "tbl")
                                              .addPartitionKeyColumn("pk", BytesType.instance)
                                              .addClusteringColumn("ck", BytesType.instance)
                                              .addRegularColumn("v", LongType.instance)
                                              .build();
        ColumnMetadata def = metadata.getColumn(new ColumnIdentifier("v", false));

        for (int i = 0; i < ROUNDS; i++)
        {
            ByteBuffer ckBytes = ByteBuffer.allocate(rnd.nextInt(MAX_CK_SIZE) + 1);
            for (int j = 0; j < ckBytes.limit(); j++)
                ckBytes.put((byte) rnd.nextInt());
            ckBytes.flip().rewind();

            Clustering<?> c = Clustering.make(ckBytes);
            Row row = BTreeRow.singleCellRow(c, BufferCell.live(def, 0, ByteBufferUtil.bytes(0L)));

            checkState(metadata, 1, row);
        }
    }
    private static void checkState(TableMetadata metadata, int maxPkSize, Row row)
    {
        PagingState.RowMark mark = PagingState.RowMark.create(metadata, row, ProtocolVersion.V3);
        ByteBuffer pkBytes = ByteBuffer.allocate(rnd.nextInt(maxPkSize) + 1);
        for (int j = 0; j < pkBytes.limit(); j++)
            pkBytes.put((byte) rnd.nextInt());
        pkBytes.flip().rewind();

        PagingState state = new PagingState(pkBytes, mark, rnd.nextInt(MAX_REMAINING) + 1, rnd.nextInt(MAX_REMAINING) + 1);
        ByteBuffer serialized = state.serialize(ProtocolVersion.V3);
        Assert.assertTrue(PagingState.isLegacySerialized(serialized));
        Assert.assertFalse(PagingState.isModernSerialized(serialized));
    }
}
