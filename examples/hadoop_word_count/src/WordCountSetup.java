/**
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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountSetup
{
    private static final Logger logger = LoggerFactory.getLogger(WordCountSetup.class);

    public static final int TEST_COUNT = 4;

    public static void main(String[] args) throws Exception
    {
        Cassandra.Iface client = createConnection();

        setupKeyspace(client);

        client.set_keyspace(WordCount.KEYSPACE);

        Map<ByteBuffer, Map<String,List<Mutation>>> mutationMap;
        Column c;

        // text0: no rows

        // text1: 1 row, 1 word
        c = new Column()
            .setName(ByteBufferUtil.bytes("text1"))
            .setValue(ByteBufferUtil.bytes("word1"))
            .setTimestamp(System.currentTimeMillis());
        mutationMap = getMutationMap(ByteBufferUtil.bytes("key0"), WordCount.COLUMN_FAMILY, c);
        client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
        logger.info("added text1");

        // text1: 1 row, 2 word
        c = new Column()
            .setName(ByteBufferUtil.bytes("text2"))
            .setValue(ByteBufferUtil.bytes("word1 word2"))
            .setTimestamp(System.currentTimeMillis());
        mutationMap = getMutationMap(ByteBufferUtil.bytes("key0"), WordCount.COLUMN_FAMILY, c);
        client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
        logger.info("added text2");

        // text3: 1000 rows, 1 word
        mutationMap = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();
        for (int i=0; i<1000; i++)
        {
            c = new Column()
                .setName(ByteBufferUtil.bytes("text3"))
                .setValue(ByteBufferUtil.bytes("word1"))
                .setTimestamp(System.currentTimeMillis());
            addToMutationMap(mutationMap, ByteBufferUtil.bytes("key" + i), WordCount.COLUMN_FAMILY, c);
        }
        client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
        logger.info("added text3");

        // sentence data for the counters
        final ByteBuffer key = ByteBufferUtil.bytes("key-if-verse1");
        final ColumnParent colParent = new ColumnParent(WordCountCounters.COUNTER_COLUMN_FAMILY);
        for (String sentence : sentenceData())
        {
            client.add(key,
                       colParent,
                       new CounterColumn(ByteBufferUtil.bytes(sentence),
                       (long)sentence.split("\\s").length),
                       ConsistencyLevel.ONE );
        }
        logger.info("added key-if-verse1");

        System.exit(0);
    }

    private static Map<ByteBuffer,Map<String,List<Mutation>>> getMutationMap(ByteBuffer key, String cf, Column c)
    {
        Map<ByteBuffer,Map<String,List<Mutation>>> mutationMap = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();
        addToMutationMap(mutationMap, key, cf, c);
        return mutationMap;
    }

    private static void addToMutationMap(Map<ByteBuffer,Map<String,List<Mutation>>> mutationMap, ByteBuffer key, String cf, Column c)
    {
        Map<String,List<Mutation>> cfMutation = new HashMap<String,List<Mutation>>();
        List<Mutation> mList = new ArrayList<Mutation>();
        ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
        Mutation m = new Mutation();

        cc.setColumn(c);
        m.setColumn_or_supercolumn(cc);
        mList.add(m);
        cfMutation.put(cf, mList);
        mutationMap.put(key, cfMutation);
    }

    private static void setupKeyspace(Cassandra.Iface client) throws TException, InvalidRequestException, SchemaDisagreementException {
        List<CfDef> cfDefList = new ArrayList<CfDef>();
        CfDef input = new CfDef(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
        input.setComparator_type("AsciiType");
        input.setDefault_validation_class("AsciiType");
        cfDefList.add(input);
        CfDef output = new CfDef(WordCount.KEYSPACE, WordCount.OUTPUT_COLUMN_FAMILY);
        output.setComparator_type("AsciiType");
        output.setDefault_validation_class("AsciiType");
        cfDefList.add(output);
        CfDef counterInput = new CfDef(WordCount.KEYSPACE, WordCountCounters.COUNTER_COLUMN_FAMILY);
        counterInput.setComparator_type("UTF8Type");
        counterInput.setDefault_validation_class("CounterColumnType");
        cfDefList.add(counterInput);

        KsDef ksDef = new KsDef(WordCount.KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
        ksDef.putToStrategy_options("replication_factor", "1");
        client.system_add_keyspace(ksDef);
        int magnitude = client.describe_ring(WordCount.KEYSPACE).size();
        try
        {
            Thread.sleep(1000 * magnitude);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Cassandra.Iface createConnection() throws TTransportException
    {
        if (System.getProperty("cassandra.host") == null || System.getProperty("cassandra.port") == null)
        {
           logger.warn("cassandra.host or cassandra.port is not defined, using default");
        }
        return createConnection( System.getProperty("cassandra.host","localhost"),
                                 Integer.valueOf(System.getProperty("cassandra.port","9160")),
                                 Boolean.valueOf(System.getProperty("cassandra.framed", "true")) );
    }

    private static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws TTransportException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        trans.open();
        TProtocol protocol = new TBinaryProtocol(trans);

        return new Cassandra.Client(protocol);
    }

    private static String[] sentenceData()
    {   // Public domain context, source http://en.wikisource.org/wiki/If%E2%80%94
        return new String[]{
            "If you can keep your head when all about you",
            "Are losing theirs and blaming it on you",
            "If you can trust yourself when all men doubt you,",
            "But make allowance for their doubting too:",
            "If you can wait and not be tired by waiting,",
            "Or being lied about, don’t deal in lies,",
            "Or being hated, don’t give way to hating,",
            "And yet don’t look too good, nor talk too wise;"
        };
    }
}
