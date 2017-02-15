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

package org.apache.cassandra.net.async;

import java.net.InetSocketAddress;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.security.SSLFactory;

public class OptionalSslHandler extends ByteToMessageDecoder
{
    private final ServerEncryptionOptions encryptionOptions;

    OptionalSslHandler(ServerEncryptionOptions encryptionOptions)
    {
        this.encryptionOptions = encryptionOptions;
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        if (in.readableBytes() < 5)
        {
            // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
            // once more bytes a ready.
            return;
        }

        if (SslHandler.isEncrypted(in))
        {
            // Connection uses SSL/TLS, replace the detection handler with a SslHandler and so use encryption.
            SslContext sslContext = SSLFactory.getSslContext(encryptionOptions, true, true);
            Channel channel = ctx.channel();
            InetSocketAddress peer = encryptionOptions.require_endpoint_verification ? (InetSocketAddress) channel.remoteAddress() : null;
            SslHandler sslHandler = NettyFactory.newSslHandler(channel, sslContext, peer);
            ctx.pipeline().replace(this, NettyFactory.SSL_CHANNEL_HANDLER_NAME, sslHandler);
        }
        else
        {
            // Connection use no TLS/SSL encryption, just remove the detection handler and continue without
            // SslHandler in the pipeline.
            ctx.pipeline().remove(this);
        }
    }
}
