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
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Indicates to the client that authentication has succeeded.
 *
 * Optionally ships some final informations from the server (as mandated by
 * SASL).
 */
public class AuthSuccess extends Message.Response
{
    public static final Message.Codec<AuthSuccess> codec = new Message.Codec<AuthSuccess>()
    {
        public AuthSuccess decode(ByteBuf body, int version)
        {
            ByteBuffer b = CBUtil.readValue(body);
            byte[] token = new byte[b.remaining()];
            b.get(token);
            return new AuthSuccess(token);
        }

        public void encode(AuthSuccess success, ByteBuf dest, int version)
        {
            CBUtil.writeValue(success.token, dest);
        }

        public int encodedSize(AuthSuccess success, int version)
        {
            return CBUtil.sizeOfValue(success.token);
        }
    };

    private byte[] token;

    public AuthSuccess(byte[] token)
    {
        super(Message.Type.AUTH_SUCCESS);
        this.token = token;
    }

    public byte[] getToken()
    {
        return token;
    }
}
