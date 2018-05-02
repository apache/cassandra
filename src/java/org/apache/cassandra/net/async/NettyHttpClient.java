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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.util.CharsetUtil;

/**
 * A simple HTTP client implementation on top of Netty. How convenient!
 */
public class NettyHttpClient
{
    private final static Logger logger = LoggerFactory.getLogger(NettyHttpClient.class);

    public static CompletableFuture<NettyHttpResponse> httpGet(URI uri,
                                                               @Nullable Map<String, Object> headers,
                                                               @Nullable SslContext sslContext)
    {
        return requestAsync(uri, null, null, headers, sslContext);
    }

    public static CompletableFuture<NettyHttpResponse> httpPost(URI uri, ByteBuf payload, String mimeType,
                                                                @Nullable Map<String, Object> headers,
                                                                @Nullable SslContext sslContext)
    {
        return requestAsync(uri, payload, mimeType, headers, sslContext);
    }

    private static CompletableFuture<NettyHttpResponse> requestAsync(URI uri,
                                                                    @Nullable ByteBuf payload,
                                                                    @Nullable String mimeType,
                                                                    @Nullable Map<String, Object> headers,
                                                                    @Nullable SslContext sslContext)
    {
        if (!uri.getScheme().equalsIgnoreCase("http") && !uri.getScheme().equalsIgnoreCase("https"))
            throw new RuntimeException("Unsupported protocol: " + uri.getScheme());
        String hostname = uri.getHost();
        InetSocketAddress addr;
        if (uri.getPort() != -1) addr = new InetSocketAddress(hostname, uri.getPort());
        else addr = new InetSocketAddress(hostname, 80);
        CompletableFuture<NettyHttpResponse> response = new CompletableFuture<>();
        Bootstrap bootstrap = NettyFactory.instance.createOutboundHttpBootstrap(addr, sslContext, response);
        ChannelFuture channelFuture = bootstrap.connect();
        channelFuture.addListener((ChannelFutureListener) future -> {
            try
            {
                if (!future.isSuccess())
                {
                    response.completeExceptionally(new IOException("Failed to connect to Vault"));
                    return;
                }
                Channel ch = future.channel();
                FullHttpRequest request;
                String path = uri.normalize().getPath();
                if (payload == null)
                {
                    logger.debug("GET {}", path);
                    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
                }
                else
                {
                    logger.debug("POST ({}) {}", payload.readableBytes(), path);
                    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path);
                    request.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeType);
                    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, payload.readableBytes());
                }
                HttpHeaders hd = request.headers();
                hd.set(HttpHeaderNames.HOST, hostname);
                hd.set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
                // we may have to consider KEEP_ALIVE at some point, but now we assume CLOSE will be the more
                // efficient choice for our use cases
                hd.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);

                if (headers != null)
                {
                    for (Map.Entry<String, Object> entry : headers.entrySet())
                        if (entry.getValue() == null) hd.remove(entry.getKey());
                        else hd.set(entry.getKey(), entry.getValue());
                }

                if (payload != null)
                    request.content().clear().writeBytes(payload);

                ch.writeAndFlush(request);
            }
            catch (Throwable e)
            {
                response.completeExceptionally(e);
            }
            finally
            {
                if (payload != null)
                {
                    payload.setZero(0, payload.capacity());
                    payload.release();
                }
            }
        });

        return response;
    }

    public static class NettyHttpResponse
    {
        private final HttpResponseStatus status;
        private final boolean chunked;
        private final HttpHeaders headers;
        private String content;

        NettyHttpResponse (HttpResponse response)
        {
            this.status = response.status();
            this.chunked = HttpUtil.isTransferEncodingChunked(response);
            this.headers = response.headers();
        }

        void addContentAsString(HttpContent msg)
        {
            this.content = msg.content().toString(CharsetUtil.UTF_8);
        }

        public HttpResponseStatus getStatus()
        {
            return status;
        }

        public boolean isChunked()
        {
            return chunked;
        }

        public HttpHeaders getHeaders()
        {
            return headers;
        }

        public String getContent()
        {
            return content;
        }
    }
}
