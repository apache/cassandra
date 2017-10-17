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

package org.apache.cassandra.vault;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.fail;

/**
 * Simple Netty based HTTP server that can be used to serve static content from a specified directory for testing
 * purposes. Implementation partly based on
 * <a href="https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/http/snoop">Netty examples</a>.
 *
 */
public class LocalHttpTestServer implements AutoCloseable
{
    private final static Logger logger = LoggerFactory.getLogger(LocalHttpTestServer.class);

    final static String FIXTURES_DIR = "test/resources";

    // Mapping of prepared responses that should be returned in case specified URIs have been requested.
    // All paths must be relative to FIXTURES_DIR.
    private final static Map<String, String> uriFixtureMappings = ImmutableMap.of(
        VaultIO.URI_LOGIN, "vault/login.json"
    );

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Channel server;

    private final static int PORT = 8200;

    LocalHttpTestServer()
    {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .handler(new LoggingHandler(LogLevel.DEBUG))
         .childHandler(new HttpServerInitializer());

        try
        {
            server = b.bind(PORT).sync().channel();
            logger.debug("Local HTTP server started, listening on {}", PORT);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        try
        {
            server.close().sync();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private static class HttpServerHandler extends SimpleChannelInboundHandler<Object>
    {
        private String requestedUri;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest)
            {
                HttpRequest request = (HttpRequest) msg;
                requestedUri = request.uri();
                logger.debug("Received request for uri: {}", requestedUri);
                requestedUri = uriFixtureMappings.getOrDefault(requestedUri, requestedUri);
                logger.debug("Received request for uri (after mapping): {}", requestedUri);
            }
            if (msg instanceof LastHttpContent) {
                writeResponse(ctx);
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }

        private void writeResponse(ChannelHandlerContext ctx)
        {
            File file = new File(FIXTURES_DIR, requestedUri);
            byte[] payload;
            try
            {
                payload = Files.readAllBytes(file.toPath());
            }
            catch (IOException e)
            {
                logger.error("Error reading local fixture file", e);
                payload = e.getMessage().getBytes();
            }

            // XXX: buffer deallocation
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(payload));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");

            ctx.write(response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            fail(cause.getMessage());
            ctx.close();
        }
    }

    public static class HttpServerInitializer extends ChannelInitializer<SocketChannel>
    {
        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new HttpRequestDecoder());
            p.addLast(new HttpObjectAggregator(1024 * 1024 * 1024));
            p.addLast(new HttpResponseEncoder());
            p.addLast(new HttpContentCompressor());
            p.addLast(new HttpServerHandler());
        }
    }
}
