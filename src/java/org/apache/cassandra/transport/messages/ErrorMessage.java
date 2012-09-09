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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ServerError;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Message to indicate an error to the client.
 */
public class ErrorMessage extends Message.Response
{
    private static final Logger logger = LoggerFactory.getLogger(ErrorMessage.class);

    public static final Message.Codec<ErrorMessage> codec = new Message.Codec<ErrorMessage>()
    {
        public ErrorMessage decode(ChannelBuffer body)
        {
            ExceptionCode code = ExceptionCode.fromValue(body.readInt());
            String msg = CBUtil.readString(body);

            TransportException te = null;
            switch (code)
            {
                case SERVER_ERROR:
                    te = new ServerError(msg);
                    break;
                case PROTOCOL_ERROR:
                    te = new ProtocolException(msg);
                    break;
                case UNAVAILABLE:
                    {
                        ConsistencyLevel cl = Enum.valueOf(ConsistencyLevel.class, CBUtil.readString(body));
                        int required = body.readInt();
                        int alive = body.readInt();
                        te = new UnavailableException(cl, required, alive);
                    }
                    break;
                case OVERLOADED:
                    te = new OverloadedException(msg);
                    break;
                case IS_BOOTSTRAPPING:
                    te = new IsBootstrappingException();
                    break;
                case TRUNCATE_ERROR:
                    te = new TruncateException(msg);
                    break;
                case WRITE_TIMEOUT:
                    {
                        ConsistencyLevel cl = Enum.valueOf(ConsistencyLevel.class, CBUtil.readString(body));
                        int received = body.readInt();
                        int blockFor = body.readInt();
                        te = new WriteTimeoutException(cl, received, blockFor, false);
                    }
                    break;
                case READ_TIMEOUT:
                    {
                        ConsistencyLevel cl = Enum.valueOf(ConsistencyLevel.class, CBUtil.readString(body));
                        int received = body.readInt();
                        int blockFor = body.readInt();
                        byte dataPresent = body.readByte();
                        te = new ReadTimeoutException(cl, received, blockFor, dataPresent != 0);
                    }
                    break;
                case SYNTAX_ERROR:
                    te = new SyntaxException(msg);
                    break;
                case UNAUTHORIZED:
                    te = new UnauthorizedException(msg);
                    break;
                case INVALID:
                    te = new InvalidRequestException(msg);
                    break;
                case CONFIG_ERROR:
                    te = new ConfigurationException(msg);
                    break;
                case ALREADY_EXISTS:
                    String ksName = CBUtil.readString(body);
                    String cfName = CBUtil.readString(body);
                    te = new AlreadyExistsException(ksName, cfName);
                    break;
            }
            return new ErrorMessage(te);
        }

        public ChannelBuffer encode(ErrorMessage msg)
        {
            ChannelBuffer ccb = CBUtil.intToCB(msg.error.code().value);
            ChannelBuffer mcb = CBUtil.stringToCB(msg.error.getMessage());

            ChannelBuffer acb = ChannelBuffers.EMPTY_BUFFER;
            switch (msg.error.code())
            {
                case UNAVAILABLE:
                    UnavailableException ue = (UnavailableException)msg.error;
                    ByteBuffer ueCl = ByteBufferUtil.bytes(ue.consistency.toString());

                    acb = ChannelBuffers.buffer(2 + ueCl.remaining() + 8);
                    acb.writeShort((short)ueCl.remaining());
                    acb.writeBytes(ueCl);
                    acb.writeInt(ue.required);
                    acb.writeInt(ue.alive);
                    break;
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    RequestTimeoutException rte = (RequestTimeoutException)msg.error;
                    ReadTimeoutException readEx = rte instanceof ReadTimeoutException
                                                ? (ReadTimeoutException)rte
                                                : null;
                    ByteBuffer rteCl = ByteBufferUtil.bytes(rte.consistency.toString());
                    acb = ChannelBuffers.buffer(2 + rteCl.remaining() + 8 + (readEx == null ? 0 : 1));
                    acb.writeShort((short)rteCl.remaining());
                    acb.writeBytes(rteCl);
                    acb.writeInt(rte.received);
                    acb.writeInt(rte.blockFor);
                    if (readEx != null)
                        acb.writeByte((byte)(readEx.dataPresent ? 1 : 0));
                    break;
                case ALREADY_EXISTS:
                    AlreadyExistsException aee = (AlreadyExistsException)msg.error;
                    acb = ChannelBuffers.wrappedBuffer(CBUtil.stringToCB(aee.ksName),
                                                       CBUtil.stringToCB(aee.cfName));
                    break;
            }
            return ChannelBuffers.wrappedBuffer(ccb, mcb, acb);
        }
    };

    // We need to figure error codes out (#3979)
    public final TransportException error;

    private ErrorMessage(TransportException error)
    {
        super(Message.Type.ERROR);
        this.error = error;
    }

    public static ErrorMessage fromException(Throwable e)
    {
        if (e instanceof TransportException)
            return new ErrorMessage((TransportException)e);

        // Unexpected exception
        logger.debug("Unexpected exception during request", e);
        return new ErrorMessage(new ServerError(e));
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "ERROR " + error.code() + ": " + error.getMessage();
    }
}
