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

import com.google.common.base.Charsets;
import org.junit.Test;

import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SystemTableTest
{
    @Test
    public void testLocalToken()
    {
        SystemTable.updateToken(new BytesToken(ByteBufferUtil.bytes("token")));
        assert new String(((BytesToken) SystemTable.getSavedToken()).token, Charsets.UTF_8).equals("token");

        SystemTable.updateToken(new BytesToken(ByteBufferUtil.bytes("token2")));
        assert new String(((BytesToken) SystemTable.getSavedToken()).token, Charsets.UTF_8).equals("token2");
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        BytesToken token = new BytesToken(ByteBufferUtil.bytes("token3"));
        InetAddress address = InetAddress.getByName("127.0.0.2");
        SystemTable.updateToken(address, token);
        assert SystemTable.loadTokens().get(token).equals(address);
        SystemTable.removeToken(token);
        assert !SystemTable.loadTokens().containsKey(token);
    }
}
