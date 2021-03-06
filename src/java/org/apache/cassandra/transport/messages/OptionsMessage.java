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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Compressor;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class OptionsMessage extends Message.Request
{
    public static final Message.Codec<OptionsMessage> codec = new Message.Codec<OptionsMessage>()
    {
        public OptionsMessage decode(ByteBuf body, ProtocolVersion version)
        {
            return new OptionsMessage();
        }

        public void encode(OptionsMessage msg, ByteBuf dest, ProtocolVersion version)
        {
        }

        public int encodedSize(OptionsMessage msg, ProtocolVersion version)
        {
            return 0;
        }
    };

    public OptionsMessage()
    {
        super(Message.Type.OPTIONS);
    }

    @Override
    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
    {
        List<String> cqlVersions = new ArrayList<String>();
        cqlVersions.add(QueryProcessor.CQL_VERSION.toString());

        List<String> compressions = new ArrayList<String>();
        if (Compressor.SnappyCompressor.instance != null)
            compressions.add("snappy");
        // LZ4 is always available since worst case scenario it default to a pure JAVA implem.
        compressions.add("lz4");

        Map<String, List<String>> supported = new HashMap<String, List<String>>();
        supported.put(StartupMessage.CQL_VERSION, cqlVersions);
        supported.put(StartupMessage.COMPRESSION, compressions);
        supported.put(StartupMessage.PROTOCOL_VERSIONS, ProtocolVersion.supportedVersions());

        return new SupportedMessage(supported);
    }

    @Override
    public String toString()
    {
        return "OPTIONS";
    }
}
