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
import io.netty.util.concurrent.Future; // checkstyle: permit this import
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

/**
 * See {@link AsyncPromise} and {@link io.netty.channel.ChannelPromise}
 *
 * This class is all boiler plate, just ensuring we return ourselves and invoke the correct Promise method.
 */
public class AsyncChannelPromise extends AsyncPromise.WithExecutor<Void> implements ChannelPromise
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

    public AsyncChannelPromise setSuccess(Void v)
    {
        super.setSuccess(v);
        return this;
    }

    public boolean trySuccess()
    {
        return trySuccess(null);
    }

    public AsyncChannelPromise setFailure(Throwable throwable)
    {
        super.setFailure(throwable);
        return this;
    }

    public AsyncChannelPromise sync() throws InterruptedException
    {
        super.sync();
        return this;
    }

    public AsyncChannelPromise syncUninterruptibly()
    {
        super.syncUninterruptibly();
        return this;
    }

    public AsyncChannelPromise await() throws InterruptedException
    {
        super.await();
        return this;
    }

    public AsyncChannelPromise awaitUninterruptibly()
    {
        super.awaitUninterruptibly();
        return this;
    }

    public AsyncChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener)
    {
        super.addListener(listener);
        return this;
    }

    public AsyncChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners)
    {
        super.addListeners(listeners);
        return this;
    }

    public AsyncChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener)
    {
        throw new UnsupportedOperationException();
    }

    public AsyncChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners)
    {
        throw new UnsupportedOperationException();
    }

    public ChannelPromise unvoid()
    {
        return this;
    }
}
