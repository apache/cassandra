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

import java.util.concurrent.TimeoutException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

/**
 * Message to indicate an error to the client.
 *
 * Error codes are:
 *   0x0000: Server error
 *   0x0001: Protocol error
 *   0x0002: Authentication error
 *   0x0100: Unavailable exception
 *   0x0101: Timeout exception
 *   0x0200: Request exception
 */
public class ErrorMessage extends Message.Response
{
    public static final Message.Codec<ErrorMessage> codec = new Message.Codec<ErrorMessage>()
    {
        public ErrorMessage decode(ChannelBuffer body)
        {
            int code = body.readInt();
            String msg = CBUtil.readString(body);
            return new ErrorMessage(code, msg);
        }

        public ChannelBuffer encode(ErrorMessage msg)
        {
            ChannelBuffer ccb = CBUtil.intToCB(msg.code);
            ChannelBuffer mcb = CBUtil.stringToCB(msg.errorMsg);
            return ChannelBuffers.wrappedBuffer(ccb, mcb);
        }
    };

    // We need to figure error codes out (#3979)
    public final int code;
    public final String errorMsg;

    public ErrorMessage(int code, String errorMsg)
    {
        super(Message.Type.ERROR);
        this.code = code;
        this.errorMsg = errorMsg;
    }

    public static ErrorMessage fromException(Throwable t)
    {
        String msg = t.getMessage() == null ? t.toString() : t.getMessage();

        if (t instanceof TimeoutException || t instanceof TimedOutException)
            return new ErrorMessage(0x0101, msg);
        else if (t instanceof UnavailableException)
            return new ErrorMessage(0x0100, msg);
        else if (t instanceof InvalidRequestException)
            return new ErrorMessage(0x0200, msg);
        else if (t instanceof ProtocolException)
            return new ErrorMessage(0x0001, msg);
        else if (t instanceof AuthenticationException)
            return new ErrorMessage(0x0002, msg);

        logger.error("Unknown exception during request", t);
        return new ErrorMessage(0x0000, msg);
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "ERROR " + code + ": " + errorMsg;
    }
}
