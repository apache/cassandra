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

import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class SupportedMessage extends Message.Response
{
    public static final Message.Codec<SupportedMessage> codec = new Message.Codec<SupportedMessage>()
    {
        public SupportedMessage decode(ByteBuf body, int version)
        {
            return new SupportedMessage(CBUtil.readStringToStringListMap(body));
        }

        public void encode(SupportedMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeStringToStringListMap(msg.supported, dest);
        }

        public int encodedSize(SupportedMessage msg, int version)
        {
            return CBUtil.sizeOfStringToStringListMap(msg.supported);
        }
    };

    public final Map<String, List<String>> supported;

    public SupportedMessage(Map<String, List<String>> supported)
    {
        super(Message.Type.SUPPORTED);
        this.supported = supported;
    }

    @Override
    public String toString()
    {
        return "SUPPORTED " + supported;
    }
}
