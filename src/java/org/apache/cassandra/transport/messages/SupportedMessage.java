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

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.SemanticVersion;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class SupportedMessage extends Message.Response
{
    public static final Message.Codec<SupportedMessage> codec = new Message.Codec<SupportedMessage>()
    {
        public SupportedMessage decode(ChannelBuffer body)
        {
            List<String> versions = CBUtil.readStringList(body);
            List<String> compressions = CBUtil.readStringList(body);
            return new SupportedMessage(versions, compressions);
        }

        public ChannelBuffer encode(SupportedMessage msg)
        {
            ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
            CBUtil.writeStringList(cb, msg.cqlVersions);
            CBUtil.writeStringList(cb, msg.compressions);
            return cb;
        }
    };

    public final List<String> cqlVersions;
    public final List<String> compressions;

    public SupportedMessage()
    {
        this(new ArrayList<String>(), new ArrayList<String>());
    }

    private SupportedMessage(List<String> cqlVersions, List<String> compressions)
    {
        super(Message.Type.SUPPORTED);
        this.cqlVersions = cqlVersions;
        this.compressions = compressions;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "SUPPORTED versions=" + cqlVersions + " compressions=" + compressions;
    }
}
