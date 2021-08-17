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

package org.apache.cassandra.net.proxy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class ProxyHandlerTest
{
    private final Object PAYLOAD = new Object();

    @Test
    public void testLatency() throws Throwable
    {
        test((proxyHandler, testHandler, channel) -> {
            int count = 1;
            long latency = 100;
            CountDownLatch latch = new CountDownLatch(count);
            long start = System.currentTimeMillis();
            testHandler.onRead = new Consumer<Object>()
            {
                int last = -1;
                public void accept(Object o)
                {
                    // Make sure that order is preserved
                    Assert.assertEquals(last + 1, o);
                    last = (int) o;

                    long elapsed = System.currentTimeMillis() - start;
                    Assert.assertTrue("Latency was:" + elapsed, elapsed > latency);
                    latch.countDown();
                }
            };

            proxyHandler.withLatency(latency, TimeUnit.MILLISECONDS);

            for (int i = 0; i < count; i++)
            {
                ByteBuf bb = Unpooled.buffer(Integer.BYTES);
                bb.writeInt(i);
                channel.writeAndFlush(i);
            }

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        });
    }

    @Test
    public void testNormalDelivery() throws Throwable
    {
        test((proxyHandler, testHandler, channelPipeline) -> {
            int count = 10;
            CountDownLatch latch = new CountDownLatch(count);
            AtomicLong end = new AtomicLong();
            testHandler.onRead = (o) -> {
                end.set(System.currentTimeMillis());
                latch.countDown();
            };

            for (int i = 0; i < count; i++)
                channelPipeline.writeAndFlush(PAYLOAD);
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

        });
    }

    @Test
    public void testLatencyForMany() throws Throwable
    {
        class Event {
            private final long latency;
            private final long start;
            private final int idx;

            Event(long latency, int idx)
            {
                this.latency = latency;
                this.start = System.currentTimeMillis();
                this.idx = idx;
            }
        }

        test((proxyHandler, testHandler, channel) -> {
            int count = 150;
            CountDownLatch latch = new CountDownLatch(count);
            AtomicInteger counter = new AtomicInteger();
            testHandler.onRead = new Consumer<Object>()
            {
                int lastSeen = -1;
                public void accept(Object o)
                {
                    Event e = (Event) o;
                    Assert.assertEquals(lastSeen + 1, e.idx);
                    lastSeen = e.idx;
                    long elapsed = System.currentTimeMillis() - e.start;
                    Assert.assertTrue(elapsed >= e.latency);
                    counter.incrementAndGet();
                    latch.countDown();
                }
            };

            int idx = 0;
            for (int i = 0; i < count / 3; i++)
            {
                for (long latency : new long[]{ 100, 200, 0 })
                {
                    proxyHandler.withLatency(latency, TimeUnit.MILLISECONDS);
                    CountDownLatch read = new CountDownLatch(1);
                    proxyHandler.onRead(read::countDown);
                    channel.writeAndFlush(new Event(latency, idx++));
                    Assert.assertTrue(read.await(10, TimeUnit.SECONDS));
                }
            }

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(counter.get(), count);
        });
    }

    private interface DoTest
    {
        public void doTest(InboundProxyHandler.Controller proxy, TestHandler testHandler, Channel channel) throws Throwable;
    }


    public void test(DoTest test) throws Throwable
    {
        EventLoopGroup serverGroup = new NioEventLoopGroup(1);
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);

        InboundProxyHandler.Controller controller = new InboundProxyHandler.Controller();
        InboundProxyHandler proxyHandler = new InboundProxyHandler(controller);
        TestHandler testHandler = new TestHandler();

        ServerBootstrap sb = new ServerBootstrap();
        sb.group(serverGroup)
          .channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
              @Override
              public void initChannel(LocalChannel ch)
              {
                  ch.pipeline()
                    .addLast(proxyHandler)
                    .addLast(testHandler);
              }
          })
          .childOption(ChannelOption.AUTO_READ, false);

        Bootstrap cb = new Bootstrap();
        cb.group(clientGroup)
          .channel(LocalChannel.class)
          .handler(new ChannelInitializer<LocalChannel>() {
              @Override
              public void initChannel(LocalChannel ch) throws Exception {
                  ch.pipeline()
                    .addLast(new LoggingHandler(LogLevel.TRACE));
              }
          });

        final LocalAddress addr = new LocalAddress("test");

        Channel serverChannel = sb.bind(addr).sync().channel();

        Channel clientChannel = cb.connect(addr).sync().channel();
        test.doTest(controller, testHandler, clientChannel);

        clientChannel.close();
        serverChannel.close();
        serverGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
    }


    public static class TestHandler extends ChannelInboundHandlerAdapter
    {
        private Consumer<Object> onRead = (o) -> {};
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
        {
            onRead.accept(msg);
        }
    }
}
