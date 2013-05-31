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

import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * SASL challenge sent from client to server
 */
public class SaslChallenge extends Message.Response
{
    public static final Message.Codec<SaslChallenge> codec = new Message.Codec<SaslChallenge>()
    {
        @Override
        public SaslChallenge decode(ChannelBuffer body, int version)
        {
            return new SaslChallenge(CBUtil.readBytes(body));
        }

        @Override
        public ChannelBuffer encode(SaslChallenge challenge)
        {
            return CBUtil.bytesToCB(challenge.token);
        }
    };

    private byte[] token;

    public SaslChallenge(byte[] token)
    {
        super(Message.Type.SASL_CHALLENGE);
        this.token = token;
    }

    @Override
    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public byte[] getToken()
    {
        return token;
    }
}
