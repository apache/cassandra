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

package org.apache.cassandra.dht;

import com.google.common.collect.Lists;
import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

public class LocalCompositePrefixPartitionerTest extends CassandraTestBase
{
    private static final String KEYSPACE = "ks";
    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
    }

    private static TableMetadata.Builder parse(String keyspace, String name, String cql)
    {
        return CreateTableStatement.parse(format(cql, name), KEYSPACE)
                .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90));
    }

    private static LocalCompositePrefixPartitioner partitioner(AbstractType... types)
    {
        return new LocalCompositePrefixPartitioner(types);
    }

    private static void assertKeysMatch(Iterable<DecoratedKey> expected, Iterator<DecoratedKey> actual)
    {
        List<DecoratedKey> expectedList = Lists.newArrayList(expected);
        List<DecoratedKey> actualList = Lists.newArrayList(actual);
        Assert.assertEquals(expectedList, actualList);
    }

    @Test
    public void keyIteratorTest() throws Throwable
    {
        String keyspaceName = "ks";
        String tableName = "tbl";
        LocalCompositePrefixPartitioner partitioner = partitioner(Int32Type.instance, BytesType.instance, Int32Type.instance);
        TableMetadata metadata = parse(keyspaceName, tableName,
                                       "CREATE TABLE %s (" +
                                               "p1 int," +
                                               "p2 blob," +
                                               "p3 int," +
                                               "v int," +
                                               "PRIMARY KEY ((p1, p2, p3))" +
                                               ")").partitioner(partitioner).build();
        SchemaLoader.createKeyspace(keyspaceName, KeyspaceParams.local(), metadata);

        executeOnceInternal(String.format("INSERT INTO %s.%s (p1, p2, p3, v) VALUES (1, 0x00, 5, 0)", keyspaceName, tableName));
        executeOnceInternal(String.format("INSERT INTO %s.%s (p1, p2, p3, v) VALUES (1, 0x0000, 5, 0)", keyspaceName, tableName));
        executeOnceInternal(String.format("INSERT INTO %s.%s (p1, p2, p3, v) VALUES (2, 0x00, 5, 0)", keyspaceName, tableName));
        executeOnceInternal(String.format("INSERT INTO %s.%s (p1, p2, p3, v) VALUES (2, 0x0100, 5, 0)", keyspaceName, tableName));
        executeOnceInternal(String.format("INSERT INTO %s.%s (p1, p2, p3, v) VALUES (2, 0x02, 5, 0)", keyspaceName, tableName));
        executeOnceInternal(String.format("INSERT INTO %s.%s (p1, p2, p3, v) VALUES (2, 0x02, 6, 0)", keyspaceName, tableName));

        Token startToken = partitioner.createPrefixToken(1, hexToBytes("0000"));
        Token endToken1 = partitioner.createPrefixToken(2, hexToBytes("0100"));
        Token endToken2 = partitioner.createPrefixToken(2, hexToBytes("02"));


        assertKeysMatch(List.of(partitioner.decoratedKey(2, hexToBytes("00"), 5),
                                partitioner.decoratedKey(2, hexToBytes("0100"), 5)
        ), partitioner.keyIterator(metadata, new Range<>(startToken.maxKeyBound(), endToken1.maxKeyBound())));

        assertKeysMatch(List.of(partitioner.decoratedKey(1, hexToBytes("0000"), 5),
                                partitioner.decoratedKey(2, hexToBytes("00"), 5),
                                partitioner.decoratedKey(2, hexToBytes("0100"), 5),
                                partitioner.decoratedKey(2, hexToBytes("02"), 5),
                                partitioner.decoratedKey(2, hexToBytes("02"), 6)
                        ), partitioner.keyIterator(metadata, new Bounds<>(startToken.minKeyBound(), endToken2.maxKeyBound())));

        assertKeysMatch(List.of(partitioner.decoratedKey(1, hexToBytes("0000"), 5),
                                partitioner.decoratedKey(2, hexToBytes("00"), 5),
                                partitioner.decoratedKey(2, hexToBytes("0100"), 5)
        ), partitioner.keyIterator(metadata, new IncludingExcludingBounds<>(startToken.minKeyBound(), endToken2.minKeyBound())));

    }
}
