package org.apache.cassandra.db;
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


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SystemTableTest
{
    @Test
    public void testLocalTokens()
    {
        // Remove all existing tokens
        SystemTable.updateTokens(Collections.<Token> emptySet());

        List<Token> tokens = new ArrayList<Token>()
        {{
            for (int i = 0; i < 9; i++)
                add(new BytesToken(ByteBufferUtil.bytes(String.format("token%d", i))));
        }};

        SystemTable.updateTokens(tokens);
        int count = 0;

        for (Token tok : SystemTable.getSavedTokens())
            assert tokens.get(count++).equals(tok);
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        BytesToken token = new BytesToken(ByteBufferUtil.bytes("token3"));
        InetAddress address = InetAddress.getByName("127.0.0.2");
        SystemTable.updateTokens(address, Collections.<Token>singletonList(token));
        assert SystemTable.loadTokens().get(address).contains(token);
        SystemTable.removeTokens(Collections.<Token>singletonList(token));
        assert !SystemTable.loadTokens().containsValue(token);
    }

    @Test
    public void testLocalHostID()
    {
        UUID firstId = SystemTable.getLocalHostId();
        UUID secondId = SystemTable.getLocalHostId();
        assert firstId.equals(secondId) : String.format("%s != %s%n", firstId.toString(), secondId.toString());
    }
}
