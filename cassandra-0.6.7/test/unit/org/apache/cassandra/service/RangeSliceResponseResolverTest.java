package org.apache.cassandra.service;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitionerTest;
import org.apache.cassandra.net.Message;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class RangeSliceResponseResolverTest
{
    private static final byte[] address1 = {127, 0, 0, 1};
    private static final byte[] address2 = {127, 0, 0, 2};

    private static final byte[] messageBody = {0,0,0,2,0,36,102,50,55,50,99,98,57,54,45,57,102,53,56,45,49,49,100,102,
            45,97,52,56,98,45,51,52,49,53,57,101,48,51,54,97,55,52,0,5,73,116,101,109,115,0,8,83,116,97,110,100,97,114,
            100,0,40,111,114,103,46,97,112,97,99,104,101,46,99,97,115,115,97,110,100,114,97,46,100,98,46,109,97,114,
            115,104,97,108,46,85,84,70,56,84,121,112,101,0,0,-128,0,0,0,-128,0,0,0,0,0,0,0,0,0,0,2,0,8,99,111,110,116,
            101,110,116,115,0,0,4,-116,-13,-29,24,40,48,0,0,0,5,116,101,120,116,50,0,7,105,116,101,109,95,105,100,0,0,
            4,-116,-13,-29,24,40,48,0,0,0,36,102,50,55,50,99,98,57,54,45,57,102,53,56,45,49,49,100,102,45,97,52,56,98,
            45,51,52,49,53,57,101,48,51,54,97,55,52,0,36,102,50,54,97,97,57,49,54,45,57,102,53,56,45,49,49,100,102,45,
            97,52,56,98,45,51,52,49,53,57,101,48,51,54,97,55,52,0,5,73,116,101,109,115,0,8,83,116,97,110,100,97,114,
            100,0,40,111,114,103,46,97,112,97,99,104,101,46,99,97,115,115,97,110,100,114,97,46,100,98,46,109,97,114,
            115,104,97,108,46,85,84,70,56,84,121,112,101,0,0,-128,0,0,0,-128,0,0,0,0,0,0,0,0,0,0,2,0,8,99,111,110,116,
            101,110,116,115,0,0,4,-116,-13,-29,23,87,-96,0,0,0,5,116,101,120,116,49,0,7,105,116,101,109,95,105,100,0,0,
            4,-116,-13,-29,23,87,-96,0,0,0,36,102,50,54,97,97,57,49,54,45,57,102,53,56,45,49,49,100,102,45,97,52,56,98,
            45,51,52,49,53,57,101,48,51,54,97,55,52};

    @Test
    public void testResolve() throws Exception
    {
        InetAddress source1 = InetAddress.getByAddress(address1);
        InetAddress source2 = InetAddress.getByAddress(address2);

        List<InetAddress> sources = new ArrayList<InetAddress>();
        sources.add(source1);
        sources.add(source2);
        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver("keyspace", sources, new RandomPartitioner());

        Collection responses = new LinkedBlockingQueue<Message>();
        responses.add(new Message(source1, StageManager.RESPONSE_STAGE, StorageService.Verb.READ_RESPONSE, messageBody));
        responses.add(new Message(source2, StageManager.RESPONSE_STAGE, StorageService.Verb.READ_RESPONSE, messageBody));

        List<Row> rows = resolver.resolve(responses);
        assert rows.size() == 2;
    }

}
