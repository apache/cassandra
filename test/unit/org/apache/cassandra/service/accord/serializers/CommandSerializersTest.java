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

import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordTxnBuilder;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.SerializerTestUtils;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.NOT_EXISTS;

public class CommandSerializersTest
{
    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));

    }

    @Test
    public void txnSerializer()
    {
        AccordTxnBuilder txnBuilder = new AccordTxnBuilder();
        txnBuilder.withRead("SELECT * FROM ks.tbl WHERE k=0 AND c=0");
        txnBuilder.withWrite("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1)");
        txnBuilder.withCondition("ks", "tbl", 0, 0, NOT_EXISTS);
        Txn txn = txnBuilder.build();
        TableId tableId = ((PartitionKey) txn.keys().get(0)).tableId();
        PartialTxn expected = txn.slice(Ranges.of(TokenRange.fullRange(tableId)), true);
        SerializerTestUtils.assertSerializerIOEquality(expected, CommandSerializers.partialTxn);
    }
}
