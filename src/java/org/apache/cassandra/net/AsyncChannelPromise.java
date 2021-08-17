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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * See {@link AsyncPromise} and {@link io.netty.channel.ChannelPromise}
 *
 * This class is all boiler plate, just ensuring we return ourselves and invoke the correct Promise method.
 */
public class AsyncChannelPromise extends AsyncPromise<Void> implements ChannelPromise
{
    private final Channel channel;

    @SuppressWarnings("unused")
    public AsyncChannelPromise(Channel channel)
    {
        super(channel.eventLoop());
        this.channel = channel;
    }

    AsyncChannelPromise(Channel channel, GenericFutureListener<? extends Future<? super Void>> listener)
    {
        super(channel.eventLoop(), listener);
        this.channel = channel;
    }

    public static AsyncChannelPromise withListener(ChannelHandlerContext context, GenericFutureListener<? extends Future<? super Void>> listener)
    {
        return withListener(context.channel(), listener);
    }

    public static AsyncChannelPromise withListener(Channel channel, GenericFutureListener<? extends Future<? super Void>> listener)
    {
        return new AsyncChannelPromise(channel, listener);
    }

    public static ChannelFuture writeAndFlush(ChannelHandlerContext context, Object message, GenericFutureListener<? extends Future<? super Void>> listener)
    {
        return context.writeAndFlush(message, withListener(context.channel(), listener));
    }

    public static ChannelFuture writeAndFlush(Channel channel, Object message, GenericFutureListener<? extends Future<? super Void>> listener)
    {
        return channel.writeAndFlush(message, withListener(channel, listener));
    }

    public static ChannelFuture writeAndFlush(ChannelHandlerContext context, Object message)
    {
        return context.writeAndFlush(message, new AsyncChannelPromise(context.channel()));
    }

    public static ChannelFuture writeAndFlush(Channel channel, Object message)
    {
        return channel.writeAndFlush(message, new AsyncChannelPromise(channel));
    }

    public Channel channel()
    {
        return channel;
    }

    public boolean isVoid()
    {
        return false;
    }

    public ChannelPromise setSuccess()
    {
        return setSuccess(null);
    }

    public ChannelPromise setSuccess(Void v)
    {
        super.setSuccess(v);
        return this;
    }

    public boolean trySuccess()
    {
        return trySuccess(null);
    }

    public ChannelPromise setFailure(Throwable throwable)
    {
        super.setFailure(throwable);
        return this;
    }

    public ChannelPromise sync() throws InterruptedException
    {
        super.sync();
        return this;
    }

    public ChannelPromise syncUninterruptibly()
    {
        super.syncUninterruptibly();
        return this;
    }

    public ChannelPromise await() throws InterruptedException
    {
        super.await();
        return this;
    }

    public ChannelPromise awaitUninterruptibly()
    {
        super.awaitUninterruptibly();
        return this;
    }

    public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener)
    {
        super.addListener(listener);
        return this;
    }

    public ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners)
    {
        super.addListeners(listeners);
        return this;
    }

    public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener)
    {
        super.removeListener(listener);
        return this;
    }

    public ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners)
    {
        super.removeListeners(listeners);
        return this;
    }

    public ChannelPromise unvoid()
    {
        return this;
    }
}
