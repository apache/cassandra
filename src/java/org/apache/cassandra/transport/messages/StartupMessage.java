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

import java.util.EnumMap;
import java.util.Map;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.SemanticVersion;

/**
 * The initial message of the protocol.
 * Sets up a number of connection options.
 */
public class StartupMessage extends Message.Request
{
    public enum Option implements OptionCodec.Codecable<Option>
    {
        COMPRESSION(1);

        private final int id;

        private Option(int id)
        {
            this.id = id;
        }

        public int getId()
        {
            return id;
        }

        public Object readValue(ChannelBuffer cb)
        {
            switch (this)
            {
                case COMPRESSION:
                    return CBUtil.readString(cb);
                default:
                    throw new AssertionError();
            }
        }

        public void writeValue(Object value, ChannelBuffer cb)
        {
            switch (this)
            {
                case COMPRESSION:
                    assert value instanceof String;
                    cb.writeBytes(CBUtil.stringToCB((String)value));
                    break;
            }
        }

        public int serializedValueSize(Object value)
        {
            switch (this)
            {
                case COMPRESSION:
                    return 2 + ((String)value).getBytes(Charsets.UTF_8).length;
                default:
                    throw new AssertionError();
            }
        }
    }

    private static OptionCodec<Option> optionCodec = new OptionCodec<Option>(Option.class);

    public static final Message.Codec<StartupMessage> codec = new Message.Codec<StartupMessage>()
    {
        public StartupMessage decode(ChannelBuffer body)
        {
            String verString = CBUtil.readString(body);

            Map<Option, Object> options = optionCodec.decode(body);
            return new StartupMessage(verString, options);
        }

        public ChannelBuffer encode(StartupMessage msg)
        {
            ChannelBuffer vcb = CBUtil.stringToCB(msg.cqlVersion);
            ChannelBuffer ocb = optionCodec.encode(msg.options);
            return ChannelBuffers.wrappedBuffer(vcb, ocb);
        }
    };

    public final String cqlVersion;
    public final Map<Option, Object> options;

    public StartupMessage(String cqlVersion, Map<Option, Object> options)
    {
        super(Message.Type.STARTUP);
        this.cqlVersion = cqlVersion;
        this.options = options;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute()
    {
        try
        {
            connection.clientState().setCQLVersion(cqlVersion);
            if (connection.clientState().getCQLVersion().compareTo(new SemanticVersion("2.99.0")) < 0)
                throw new ProtocolException(String.format("CQL version %s is not support by the binary protocol (supported version are >= 3.0.0)", cqlVersion));

            if (options.containsKey(Option.COMPRESSION))
            {
                String compression = ((String)options.get(Option.COMPRESSION)).toLowerCase();
                if (compression.equals("snappy"))
                {
                    if (FrameCompressor.SnappyCompressor.instance == null)
                        throw new InvalidRequestException("This instance does not support Snappy compression");
                    connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
                }
                else
                {
                    throw new InvalidRequestException(String.format("Unknown compression algorithm: %s", compression));
                }
            }

            if (connection.clientState().isLogged())
                return new ReadyMessage();
            else
                return new AuthenticateMessage(DatabaseDescriptor.getAuthenticator().getClass().getName());
        }
        catch (InvalidRequestException e)
        {
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "STARTUP cqlVersion=" + cqlVersion;
    }
}
