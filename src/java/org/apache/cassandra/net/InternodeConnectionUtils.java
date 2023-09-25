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

package org.apache.cassandra.net;

import java.nio.channels.ClosedChannelException;
import java.security.cert.Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;

/**
 * Class that contains certificate utility methods.
 */
public class InternodeConnectionUtils
{
    public static String SSL_HANDLER_NAME = "ssl";
    public static String DISCARD_HANDLER_NAME = "discard";
    public static String SSL_FACTORY_CONTEXT_DESCRIPTION = "server_encryption_options";
    private static final Logger logger = LoggerFactory.getLogger(InternodeConnectionUtils.class);

    public static Certificate[] certificates(Channel channel)
    {
        final SslHandler sslHandler = (SslHandler) channel.pipeline().get(SSL_HANDLER_NAME);
        Certificate[] certificates = null;
        if (sslHandler != null)
        {
            try
            {
                certificates = sslHandler.engine()
                                         .getSession()
                                         .getPeerCertificates();
            }
            catch (SSLPeerUnverifiedException e)
            {
                logger.debug("Failed to get peer certificates for peer {}", channel.remoteAddress(), e);
            }
        }
        return certificates;
    }

    public static boolean isSSLError(final Throwable cause)
    {
        return (cause instanceof ClosedChannelException)
               && cause.getCause() == null
               && cause.getStackTrace()[0].getClassName().contains("SslHandler")
               && cause.getStackTrace()[0].getMethodName().contains("channelInactive");
    }

    /**
     * Discard handler releases the received data silently. when internode authentication fails, the channel is closed,
     * but the pending buffered data may still be fired through the pipeline. To avoid that, authentication handler is
     * replaced with this DiscardHandler to release all the buffered data, to avoid handling unauthenticated data in the
     * following handlers.
     */
    public static class ByteBufDiscardHandler extends ChannelInboundHandlerAdapter
    {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
        {
            if (msg instanceof ByteBuf)
            {
                ((ByteBuf) msg).release();
            }
            else
            {
                ctx.fireChannelRead(msg);
            }
        }
    }
}
