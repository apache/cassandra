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

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;

public class InboundProxyHandler extends ChannelInboundHandlerAdapter
{
    private final ArrayDeque<Forward> forwardQueue;
    private ScheduledFuture scheduled = null;
    private final Controller controller;
    public InboundProxyHandler(Controller controller)
    {
        this.controller = controller;
        this.forwardQueue = new ArrayDeque<>(1024);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        super.channelActive(ctx);
        ctx.read();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        controller.onDisconnect.run();

        if (scheduled != null)
        {
            scheduled.cancel(true);
            scheduled = null;
        }

        if (!forwardQueue.isEmpty())
            forwardQueue.clear();

        super.channelInactive(ctx);
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        Forward forward = controller.forwardStrategy.forward(ctx, msg);
        forwardQueue.offer(forward);
        maybeScheduleNext(ctx.channel().eventLoop());
        controller.onRead.run();
        ctx.channel().read();
    }

    private void maybeScheduleNext(EventExecutor executor)
    {
        if (forwardQueue.isEmpty())
        {
            // Ran out of items to process
            scheduled = null;
        }
        else if (scheduled == null)
        {
            // Schedule next available or let the last in line schedule it
            Forward forward = forwardQueue.poll();
            scheduled = forward.schedule(executor);
            scheduled.addListener((e) -> {
                scheduled = null;
                maybeScheduleNext(executor);
            });
        }
    }

    private static class Forward
    {
        final long arrivedAt;
        final long latency;
        final Runnable handler;

        private Forward(long arrivedAt, long latency, Runnable handler)
        {
            this.arrivedAt = arrivedAt;
            this.latency = latency;
            this.handler = handler;
        }

        ScheduledFuture schedule(EventExecutor executor)
        {
            long now = System.currentTimeMillis();
            long elapsed = now - arrivedAt;
            long runIn = latency - elapsed;

            if (runIn > 0)
                return executor.schedule(handler, runIn, TimeUnit.MILLISECONDS);
            else
                return executor.schedule(handler, 0, TimeUnit.MILLISECONDS);
        }
    }

    private static class ForwardNormally implements ForwardStrategy
    {
        static ForwardNormally instance = new ForwardNormally();

        public Forward forward(ChannelHandlerContext ctx, Object msg)
        {
            return new Forward(System.currentTimeMillis(),
                               0,
                               () -> ctx.fireChannelRead(msg));
        }
    }

    public interface ForwardStrategy
    {
        public Forward forward(ChannelHandlerContext ctx, Object msg);
    }

    private static class ForwardWithLatency implements ForwardStrategy
    {
        private final long latency;
        private final TimeUnit timeUnit;

        ForwardWithLatency(long latency, TimeUnit timeUnit)
        {
            this.latency = latency;
            this.timeUnit = timeUnit;
        }

        public Forward forward(ChannelHandlerContext ctx, Object msg)
        {
            return new Forward(System.currentTimeMillis(),
                               timeUnit.toMillis(latency),
                               () -> ctx.fireChannelRead(msg));
        }
    }

    private static class CloseAfterRead implements ForwardStrategy
    {
        private final Runnable afterClose;

        CloseAfterRead(Runnable afterClose)
        {
            this.afterClose = afterClose;
        }

        public Forward forward(ChannelHandlerContext ctx, Object msg)
        {
            return  new Forward(System.currentTimeMillis(),
                                0,
                                () -> {
                                    ctx.channel().close().syncUninterruptibly();
                                    afterClose.run();
                                });
        }
    }

    private static class TransformPayload<T> implements ForwardStrategy
    {
        private final Function<T, T> fn;

        TransformPayload(Function<T, T> fn)
        {
            this.fn = fn;
        }

        public Forward forward(ChannelHandlerContext ctx, Object msg)
        {
            return new Forward(System.currentTimeMillis(),
                               0,
                               () -> ctx.fireChannelRead(fn.apply((T) msg)));
        }
    }

    public static class Controller
    {
        private volatile InboundProxyHandler.ForwardStrategy forwardStrategy;
        private volatile Runnable onRead = () -> {};
        private volatile Runnable onDisconnect = () -> {};

        public Controller()
        {
            this.forwardStrategy = ForwardNormally.instance;
        }
        public void onRead(Runnable onRead)
        {
            this.onRead = onRead;
        }

        public void onDisconnect(Runnable onDisconnect)
        {
            this.onDisconnect = onDisconnect;
        }

        public void reset()
        {
            this.forwardStrategy = ForwardNormally.instance;
        }

        public void withLatency(long latency, TimeUnit timeUnit)
        {
            this.forwardStrategy = new ForwardWithLatency(latency, timeUnit);
        }

        public void withCloseAfterRead(Runnable afterClose)
        {
            this.forwardStrategy = new CloseAfterRead(afterClose);
        }

        public <T> void withPayloadTransform(Function<T, T> fn)
        {
            this.forwardStrategy = new TransformPayload<>(fn);
        }
    }

}
