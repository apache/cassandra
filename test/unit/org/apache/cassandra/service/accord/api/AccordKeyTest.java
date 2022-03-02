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

package org.apache.cassandra.service.accord.api;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordKey.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SerializerTestUtils;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class AccordKeyTest
{
    private static final TableId TABLE1 = TableId.fromString("00000000-0000-0000-0000-000000000001");
    private static final TableId TABLE2 = TableId.fromString("00000000-0000-0000-0000-000000000002");

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl1 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE1),
                                    parse("CREATE TABLE tbl2 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE2));

    }

    public static IPartitioner partitioner(TableId tableId)
    {
        return Schema.instance.getTableMetadata(tableId).partitioner;
    }

    @Test
    public void partitionKeyTest()
    {
        DecoratedKey dk = partitioner(TABLE1).decorateKey(ByteBufferUtil.bytes(5));
        PartitionKey pk = new PartitionKey(TABLE1, dk);
        SerializerTestUtils.assertSerializerIOEquality(pk, PartitionKey.serializer);
    }

    @Test
    public void tokenKeyTest()
    {
        DecoratedKey dk = partitioner(TABLE1).decorateKey(ByteBufferUtil.bytes(5));
        TokenKey pk = new TokenKey(TABLE1, dk.getToken().maxKeyBound());
        SerializerTestUtils.assertSerializerIOEquality(pk, TokenKey.serializer);
    }

    @Test
    public void comparisonTest()
    {
        DecoratedKey dk = partitioner(TABLE1).decorateKey(ByteBufferUtil.bytes(5));
        PartitionKey pk = new PartitionKey(TABLE1, dk);
        TokenKey tkLow = new TokenKey(TABLE1, dk.getToken().minKeyBound());
        TokenKey tkHigh = new TokenKey(TABLE1, dk.getToken().maxKeyBound());

        Assert.assertTrue(tkLow.compareTo(pk) < 0);
        Assert.assertTrue(pk.compareTo(tkHigh) < 0);
    }

    @Test
    public void tableComparisonTest()
    {
        Assert.assertTrue(TABLE1.compareTo(TABLE2) < 0);

        DecoratedKey dk1 = partitioner(TABLE1).decorateKey(ByteBufferUtil.bytes(5));
        PartitionKey pk1 = new PartitionKey(TABLE1, dk1);

        DecoratedKey dk2 = partitioner(TABLE2).decorateKey(ByteBufferUtil.bytes(5));
        PartitionKey pk2 = new PartitionKey(TABLE2, dk2);

        Assert.assertTrue(pk1.compareTo(pk2) < 0);
    }

    @Test
    public void sentinelComparisonTest()
    {
        Assert.assertTrue(TABLE1.compareTo(TABLE2) < 0);
        DecoratedKey dk1 = partitioner(TABLE1).decorateKey(ByteBufferUtil.bytes(5));
        PartitionKey pk1 = new PartitionKey(TABLE1, dk1);

        DecoratedKey dk2 = partitioner(TABLE2).decorateKey(ByteBufferUtil.bytes(5));
        PartitionKey pk2 = new PartitionKey(TABLE2, dk2);

        SentinelKey loSentinel = SentinelKey.min(TABLE1);
        SentinelKey hiSentinel = SentinelKey.max(TABLE1);
        Assert.assertTrue(loSentinel.compareTo(hiSentinel) < 0);
        Assert.assertTrue(pk1.compareTo(loSentinel) > 0);
        Assert.assertTrue(loSentinel.compareTo(pk1) < 0);
        Assert.assertTrue(hiSentinel.compareTo(pk2) < 0);
    }
}
