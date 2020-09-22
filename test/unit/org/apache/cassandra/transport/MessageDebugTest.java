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
package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.cql3.CqlBuilder.truncateCqlLiteral;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.Generate.byteArrays;
import static org.quicktheories.generators.Generate.bytes;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;
import static org.quicktheories.generators.SourceDSL.strings;

public class MessageDebugTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        setUpClass();
    }

    @Test
    public void testMutationToCqlString()
    {
        String create = "CREATE TABLE %s (" +
                        "lonely text static," +
                        "key1 text," +
                        "key2 text," +
                        "cl1 text," +
                        "cl2 text," +
                        "value text," +
                        "PRIMARY KEY ((key1, key2), cl1, cl2))";
        createTable(KEYSPACE, create);

        System.err.println("CCC " + create);
        TableMetadata cfm = currentTableMetadata();

        String start = String.format("INSERT INTO %s.%s", cfm.keyspace, cfm.name);
        String insert =  start + " (key1, key2, cl1, cl2, value) VALUES ";
        ByteBuffer key = cfm.partitionKeyType.fromString("key1val:key2val");
        DecoratedKey dk = getCurrentColumnFamilyStore().decorateKey(key);

        // base cases
        String query = insert + "('key1val', 'key2val', 'cv1', 'cv2', 'v1');";
        assertQueryEqual(query);
        query = insert + "('''escaped', '''escaped', '''escaped', '''escaped', '''escaped');";
        assertQueryEqual(query);
        query = insert + "('', '', '', '', '');";
        assertQueryEqual(query);

        // static
        assertQueryEqual(start + " (key1, key2, lonely) VALUES ('k1', 'k2', 's1');");

        // partition delete
        Mutation mutation = new Mutation(PartitionUpdate.fullPartitionDelete(cfm, dk, 12000000, 12));
        String cql = mutation.toCQLString(false);
        Assert.assertTrue(cql, cql.endsWith("(key1, key2) = ('key1val', 'key2val');"));

        // empty update
        mutation = new Mutation(PartitionUpdate.emptyUpdate(cfm, dk));
        cql = mutation.toCQLString(false);
        Assert.assertTrue("'" + cql + "'", cql.isEmpty());

        // range tombstone
        query = String.format("DELETE FROM %s.%s WHERE key1 = 'k1' AND key2 = 'k2' AND cl1 = 'c1' AND cl2 > 'c2'", cfm.keyspace, cfm.name);
        assertQuery(r -> r.get(0).equals("TombstoneMarker EXCL_START_BOUND(c1, c2); TombstoneMarker INCL_END_BOUND(c1);"), query);

        query = String.format("DELETE FROM %s.%s WHERE key1 = 'k1' AND key2 = 'k2' AND cl1 > 'c1' AND cl1 < 'c2'", cfm.keyspace, cfm.name);
        assertQuery(r -> r.get(0).equals("TombstoneMarker EXCL_START_BOUND(c1); TombstoneMarker EXCL_END_BOUND(c2);"), query);
    }

    private void assertQueryEqual(String query)
    {
        assertQuery(results -> 1 == results.size() && query.equals(results.get(0)), query);
    }

    private void assertQuery(Predicate<List<String>> expected, String query)
    {
        ClientState cs = ClientState.forInternalCalls();
        ModificationStatement m = (ModificationStatement) QueryProcessor.parseStatement(query, cs);
        System.out.println("MMM : " + m.getMutations(QueryOptions.DEFAULT, false, 1, 1, 1).get(0).toString());
        List<String> results = m.getMutations(QueryOptions.DEFAULT, false, 1, 1, 1)
                                .stream()
                                .map(x -> ((Mutation) x).toCQLString(false))
                                .collect(Collectors.toList());
        Assert.assertTrue(query + " AND " + results, expected.test(results));
    }

    @Test
    public void testTruncateTextType()
    {
        Pattern singleQuote = Pattern.compile("'", Pattern.LITERAL);
        int max = 500;
        qt().forAll(strings()
                    .allPossible()
                    .ofLengthBetween(0, 1000)
                    .map(s -> '\'' + singleQuote.matcher(s).replaceAll("''") + '\'')
                    .map(s -> truncateCqlLiteral(s, max)))
            .checkAssert(s -> {
                String contents = s.substring(1, s.length() - 1);
                for (int i = 0; i < contents.length() - 1; i++)
                {
                    if (contents.charAt(i) == '\'')
                    {
                        Assert.assertEquals('\'', contents.charAt(i + 1));
                        i++;
                    }
                }
                if (s.length() > max)
                    Assert.assertTrue(contents.endsWith("...") && s.endsWith("...'"));
                Assert.assertTrue(s.startsWith("'") && s.endsWith("'"));
            });
    }

    @Test
    public void testTruncateBlobType()
    {
        int max = 100;
        qt().forAll(byteArrays(integers().between(0, 100), bytes(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0))
                    .map(b -> "0x" + ByteBufferUtil.bytesToHex(ByteBuffer.wrap(b)))
                    .map(s -> truncateCqlLiteral(s, max)))
            .checkAssert(s-> {
                Assert.assertEquals((s.length() > max), s.endsWith("..."));
                Assert.assertTrue(s.startsWith("0x"));
            });
    }

    @Test
    public void testTruncate()
    {
        Assert.assertEquals("0x12...", truncateCqlLiteral("0x1234", 4));
        Assert.assertEquals("", truncateCqlLiteral("", 3));

        // string literal
        Assert.assertEquals("'te...'", truncateCqlLiteral("'test'", 3));
        Assert.assertEquals("''", truncateCqlLiteral("''", 3));

        // test escaped ' is not interupted
        Assert.assertEquals("'t''...'", truncateCqlLiteral("'t''est'", 3));
        Assert.assertEquals("'t''''...'", truncateCqlLiteral("'t''''est'", 3));
        Assert.assertEquals("'''...'", truncateCqlLiteral("''''", 1));
        Assert.assertEquals("'''...'", truncateCqlLiteral("'''x'''", 1));

        // collections
        Assert.assertEquals("[1...]", truncateCqlLiteral("[1, 2, 3]", 2));
        Assert.assertEquals("(1...)", truncateCqlLiteral("(1, 2, 3)", 2));
        Assert.assertEquals("{1...}", truncateCqlLiteral("{1, 2, 3}", 2));
    }

    @Test
    public void testQueryMessage()
    {
        QueryMessage message = new QueryMessage("SELECT * FROM test", QueryOptions.DEFAULT);
        Assert.assertEquals("QUERY SELECT * FROM test WITH pageSize = -1 AT CONSISTENCY ONE", message.toString());
    }

    @Test
    public void testPrepareMessage()
    {
        PrepareMessage message = new PrepareMessage("SELECT * FROM test", "keyspace1");
        Assert.assertEquals("PREPARE SELECT * FROM test IN keyspace1", message.toString());
    }

    @Test
    public void testExecuteMessage()
    {
        QueryHandler handler = ClientState.getCQLQueryHandler();
        MD5Digest md5 = handler.prepare("INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, ?)",
                                        ClientState.forInternalCalls(), Maps.newHashMap()).statementId;
        qt().forAll(
                timeuuid(),
                mutations(),
                integers().allPositive().map(Int32Type.instance.getSerializer()::serialize))
            .as(Lists::newArrayList)
            .checkAssert(values -> {
                QueryOptions opts = QueryOptions.create(ConsistencyLevel.ONE, values, true, 1,
                                                       null, ConsistencyLevel.SERIAL, ProtocolVersion.V4,
                                                       "system");
                ExecuteMessage message = new ExecuteMessage(md5, null, opts);

                String sb = "EXECUTE " + toInsertString(values) + " AT CONSISTENCY ONE";
                Assert.assertEquals(sb, message.toString());
            });
    }

    @Test
    public void testBatchMessage()
    {
        BatchMessage message = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                Lists.newArrayList("SELECT1", "SELECT2"),
                                                Lists.newArrayList(),
                                                QueryOptions.DEFAULT);
        Assert.assertEquals("UNLOGGED BATCH of [SELECT1, SELECT2] AT CONSISTENCY ONE", message.toString());

        QueryHandler handler = ClientState.getCQLQueryHandler();
        MD5Digest md5 = handler.prepare("INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, ?)",
                                        ClientState.forInternalCalls(), Maps.newHashMap()).statementId;
        qt().forAll(
            timeuuid(),
            mutations(),
            integers().allPositive().map(Int32Type.instance.getSerializer()::serialize),
            arbitrary().enumValues(BatchStatement.Type.class))
        .checkAssert((id, mutations, version, type) -> {
            List<ByteBuffer> values = Lists.newArrayList(id, mutations, version);
            List<List<ByteBuffer>> combined = Lists.newArrayList();
            combined.add(values);
            BatchMessage msg = new BatchMessage(type, Lists.newArrayList(md5), combined, QueryOptions.DEFAULT);
            Assert.assertEquals(type.name()+ " BATCH of [" +toInsertString(values) + "] AT CONSISTENCY ONE", msg.toString());
        });
    }

    @Test
    public void testBatchOnPreparedEviction()
    {
        // prepared statement id that is missing from cache
        MD5Digest missing = MD5Digest.compute(new byte[] {1});
        BatchMessage message = new BatchMessage(BatchStatement.Type.UNLOGGED,
                                                Lists.newArrayList(missing, "SELECT2"),
                                                Lists.newArrayList(),
                                                QueryOptions.DEFAULT);
        Assert.assertEquals("UNLOGGED BATCH of [55a54008ad1ba589aa210d2629c1df41, SELECT2] AT CONSISTENCY ONE", message.toString());
    }

    @Test
    public void testExecuteOnPreparedEviction()
    {
        // prepared statement id that is missing from cache
        MD5Digest missing = MD5Digest.compute(new byte[] {1});
        QueryOptions opts = QueryOptions.create(ConsistencyLevel.ONE, Lists.newArrayList(), true, 1,
                                                null, ConsistencyLevel.SERIAL, ProtocolVersion.V4,
                                                "system");
        ExecuteMessage message = new ExecuteMessage(missing, null, opts);
        Assert.assertEquals("EXECUTE 55a54008ad1ba589aa210d2629c1df41 WITH 0 VALUES AT CONSISTENCY ONE", message.toString());
    }

    private String toInsertString(List<ByteBuffer> values)
    {
        String verString = Int32Type.instance.getSerializer().toCQLLiteral(values.get(2));
        List<ByteBuffer> ms = ListType.getInstance(BytesType.instance, false).getSerializer()
                                      .deserialize(values.get(1));
        StringBuilder mtmp = new StringBuilder("[");
        boolean first = true;
        for (ByteBuffer m : ms)
        {
            if (!first)
                mtmp.append(", ");
            mtmp.append("0x").append(ByteBufferUtil.bytesToHex(m));
            first = false;
        }
        String mstr = truncateCqlLiteral(mtmp.append(']').toString());

        return "INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, ?) WITH [" +
               TimeUUIDType.instance.getSerializer().deserialize(values.get(0)).toString() +
               ", " + mstr + ", " + verString + ']';
    }

    public Gen<ByteBuffer> timeuuid()
    {
        return arbitrary().constant(TimeUUIDType.instance::now);
    }

    public Gen<ByteBuffer> mutations()
    {
        ListSerializer<ByteBuffer> serializer = ListType.getInstance(BytesType.instance, false)
                                                        .getSerializer();
        return lists().of(byteArrays(integers().between(0, 100),
                                     bytes(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0))
                          .map((Function<? super byte[], ? extends ByteBuffer>) ByteBuffer::wrap))
                      .ofSizeBetween(0, 100)
                      .map(x -> serializer.serialize((List<ByteBuffer>) x));
    }
}
