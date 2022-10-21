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

package org.apache.cassandra.service.accord.txn;

import org.apache.cassandra.service.accord.AccordTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.utils.SerializerTestUtils.assertSerializerIOEquality;

public class TxnUpdateTest
{
    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));

    }

    @Test
    public void predicateSerializer()
    {
        Txn txn = AccordTestUtils.createTxn(0, 0);
        TxnUpdate update = (TxnUpdate) txn.update();
        assertSerializerIOEquality(update, TxnUpdate.serializer);
    }
}
