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
package org.apache.cassandra.transport;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.transport.messages.ErrorMessage;

/**
 * Enhances {@link SimpleClient} to add custom logic to send to the server.
 */
public class WrappedSimpleClient extends SimpleClient
{
    public WrappedSimpleClient(String host, int port, ProtocolVersion version, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        super(host, port, version, encryptionOptions);
    }

    public WrappedSimpleClient(String host, int port, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        super(host, port, encryptionOptions);
    }

    public WrappedSimpleClient(String host, int port, ProtocolVersion version)
    {
        super(host, port, version);
    }

    public WrappedSimpleClient(String host, int port, ProtocolVersion version, boolean useBeta, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        super(host, port, version, useBeta, encryptionOptions);
    }

    public WrappedSimpleClient(String host, int port)
    {
        super(host, port);
    }

    public Message.Response write(ByteBuf buffer) throws InterruptedException
    {
        return write(buffer, true);
    }

    public Message.Response write(ByteBuf buffer, boolean awaitCloseOnProtocolError) throws InterruptedException
    {
        lastWriteFuture = channel.writeAndFlush(buffer);
        Message.Response response = responseHandler.responses.take();
        if (awaitCloseOnProtocolError
            && response instanceof ErrorMessage && ((ErrorMessage) response).error.code() == ExceptionCode.PROTOCOL_ERROR)
        {
            // protocol errors shutdown the connection, wait for it to close
            connection.channel().closeFuture().awaitUninterruptibly();
        }
        return response;
    }
}
