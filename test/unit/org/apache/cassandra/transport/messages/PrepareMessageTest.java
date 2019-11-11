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

package org.apache.cassandra.transport.messages;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class PrepareMessageTest extends EncodeAndDecodeTestBase<PrepareMessage>
{
    @Test
    public void testEncodeThenDecode()
    {
        PrepareMessage origin = new PrepareMessage("SELECT * FROM keyspace.tbl WHERE name='ßètæ'", "keyspace");
        PrepareMessage newMessage = encodeThenDecode(origin, ProtocolVersion.V5);
        Assert.assertEquals(origin.toString(), newMessage.toString());
    }

    protected Message.Codec<PrepareMessage> getCodec()
    {
        return PrepareMessage.codec;
    }
}
