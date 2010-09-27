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

import java.util.*;

import org.apache.cassandra.thrift.*;
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

        Map<byte[], Map<String,List<Mutation>>> mutationMap;
        Column c;

        // text0: no rows

        // text1: 1 row, 1 word
        c = new Column("text1".getBytes(), "word1".getBytes(), System.currentTimeMillis());
        mutationMap = getMutationMap("key0".getBytes(), WordCount.COLUMN_FAMILY, c);
        client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
        logger.info("added text1");

        // text1: 1 row, 2 word
        c = new Column("text2".getBytes(), "word1 word2".getBytes(), System.currentTimeMillis());
        mutationMap = getMutationMap("key0".getBytes(), WordCount.COLUMN_FAMILY, c);
        client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
        logger.info("added text2");

        // text3: 1000 rows, 1 word
        mutationMap = new HashMap<byte[],Map<String,List<Mutation>>>();
        for (int i=0; i<1000; i++)
        {
            c = new Column("text3".getBytes(), "word1".getBytes(), System.currentTimeMillis());
            addToMutationMap(mutationMap, ("key" + i).getBytes(), WordCount.COLUMN_FAMILY, c);
        }
        client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
        logger.info("added text3");

        System.exit(0);
    }

    private static Map<byte[],Map<String,List<Mutation>>> getMutationMap(byte[] key, String cf, Column c)
    {
        Map<byte[],Map<String,List<Mutation>>> mutationMap = new HashMap<byte[],Map<String,List<Mutation>>>();
        addToMutationMap(mutationMap, key, cf, c);
        return mutationMap;
    }

    private static void addToMutationMap(Map<byte[],Map<String,List<Mutation>>> mutationMap, byte[] key, String cf, Column c)
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

    private static void setupKeyspace(Cassandra.Iface client) throws TException, InvalidRequestException
    {
        List<CfDef> cfDefList = new ArrayList<CfDef>();
        cfDefList.add(new CfDef(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY));
        cfDefList.add(new CfDef(WordCount.KEYSPACE, WordCount.OUTPUT_COLUMN_FAMILY));

        client.system_add_keyspace(new KsDef(WordCount.KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", 1, cfDefList));
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
}
