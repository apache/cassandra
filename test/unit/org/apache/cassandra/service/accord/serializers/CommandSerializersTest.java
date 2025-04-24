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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.FastByteOperations;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class CommandSerializersTest
{
    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));

    }

    @Test
    public void txnSerializer() throws IOException
    {
        Txn txn = AccordTestUtils.createTxn("BEGIN TRANSACTION\n" +
                                                 "  LET row1 = (SELECT * FROM ks.tbl WHERE k=0 AND c=0);\n" +
                                                 "  SELECT row1.v;\n" +
                                                 "  IF row1 IS NULL THEN\n" +
                                                 "    INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1);\n" +
                                                 "  END IF\n" +
                                                 "COMMIT TRANSACTION");
        PartitionKey key = (PartitionKey) txn.keys().get(0);
        PartialTxn expected = txn.slice(Ranges.of(TokenRange.fullRange(key.table(), getPartitioner())), true);
        Serializers.testSerde(CommandSerializers.partialTxn, expected, Version.LATEST);
    }

    @Test
    public void txnIdSerde()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(AccordGens.txnIds()).check(txnId -> {
            Serializers.testSerde(output, CommandSerializers.txnId, txnId);
            ByteBuffer tmp = output.buffer();
            tmp.clear();
            CommandSerializers.txnId.serialize(txnId, tmp);
            tmp.flip();
            TxnId rt = CommandSerializers.txnId.deserialize(tmp);
            Assertions.assertThat(rt).isEqualTo(txnId);
        });
    }

    @Test
    public void txnIdComparable()
    {
        qt().forAll(AccordGens.txnIds(), AccordGens.txnIds()).check(CommandSerializersTest::testComparable);
        qt().forAll(AccordGens.txnIds()).check((a) -> {
            ByteBuffer abb = ByteBuffer.allocate((int) CommandSerializers.txnId.serializedSize(a));
            CommandSerializers.txnId.serializeComparable(a, abb, ByteBufferAccessor.instance, 0);
            if (a.epoch() < Timestamp.MAX_EPOCH)
                testComparable(a, TxnId.fromValues(a.epoch() + 1, a.hlc(), a.flags(), a.node));
            if (a.epoch() > 0)
                testComparable(a, TxnId.fromValues(a.epoch() - 1, a.hlc(), a.flags(), a.node));
            if (a.hlc() < Timestamp.MAX.hlc())
                testComparable(a, TxnId.fromValues(a.epoch(), a.hlc() + 1, a.flags(), a.node));
            if (a.hlc() > 0)
                testComparable(a, TxnId.fromValues(a.epoch(), a.hlc() - 1, a.flags(), a.node));
            if (a.flags() < Timestamp.MAX.flags())
                testComparable(a, TxnId.fromValues(a.epoch(), a.hlc(), a.flags() + 1, a.node));
            if (a.flags() != 0)
                testComparable(a, TxnId.fromValues(a.epoch(), a.hlc(), a.flags() - 1, a.node));
            if (a.node.id > 0)
                testComparable(a, TxnId.fromValues(a.epoch(), a.hlc(), a.flags(), new Node.Id(a.node.id - 1)));
            if (a.node.id < Integer.MAX_VALUE)
                testComparable(a, TxnId.fromValues(a.epoch(), a.hlc(), a.flags(), new Node.Id(a.node.id + 1)));
        });
    }

    private static void testComparable(TxnId a, TxnId b)
    {
        ByteBuffer abb = ByteBuffer.allocate((int) CommandSerializers.txnId.serializedSize(a));
        CommandSerializers.txnId.serializeComparable(a, abb, ByteBufferAccessor.instance, 0);
        TxnId art = CommandSerializers.txnId.deserializeComparable(abb, ByteBufferAccessor.instance, 0);
        Assertions.assertThat(art).isEqualTo(a);
        testComparable(abb, a, b);
    }

    private static void testComparable(ByteBuffer abb, TxnId a, TxnId b)
    {
        ByteBuffer bbb = ByteBuffer.allocate((int) CommandSerializers.txnId.serializedSize(b));
        CommandSerializers.txnId.serializeComparable(b, bbb, ByteBufferAccessor.instance, 0);
        Assertions.assertThat(FastByteOperations.compareUnsigned(abb, bbb)).isEqualTo(a.compareTo(b));
        TxnId brt = CommandSerializers.txnId.deserializeComparable(bbb, ByteBufferAccessor.instance, 0);
        Assertions.assertThat(brt).isEqualTo(b);
    }
}
