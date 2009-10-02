/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.dht;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class CollatingOrderPreservingPartitionerTest extends PartitionerTestCase<BytesToken> {
    @Override
    public IPartitioner<BytesToken> getPartitioner()
    {
        return new CollatingOrderPreservingPartitioner();
    }

    @Override
    public BytesToken tok(String string)
    {
        // we just need some kind of byte array
        try
        {
            return new BytesToken(string.getBytes("US-ASCII"));
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String tos(BytesToken token)
    {
        return FBUtilities.bytesToHex(token.token);
    }

    /**
     * Test that a non-UTF-8 byte array can still be encoded.
     */
    @Test
    public void testTokenFactoryStringsNonUTF()
    {
        Token.TokenFactory factory = this.partitioner.getTokenFactory();
        BytesToken tok = new BytesToken((byte)0xFF, (byte)0xFF);
        assert tok.compareTo(factory.fromString(factory.toString(tok))) == 0;
    }
}
