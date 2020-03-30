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

package org.apache.cassandra.distributed.impl;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

public abstract class DelegatingInvokableInstance implements IInvokableInstance
{
    protected abstract IInvokableInstance delegate();
    protected abstract IInvokableInstance delegateForStartup();
    
    @Override
    public <E extends Serializable> E transfer(E object)
    {
        return delegate().transfer(object);
    }

    @Override
    public InetSocketAddress broadcastAddress()
    {
        return config().broadcastAddress();
    }

    @Override
    public Object[][] executeInternal(String query, Object... args)
    {
        return delegate().executeInternal(query, args);
    }

    public SimpleQueryResult executeInternalWithResult(String query, Object... args)
    {
        return delegate().executeInternalWithResult(query, args);
    }

    @Override
    public UUID schemaVersion()
    {
        return delegate().schemaVersion();
    }

    @Override
    public void startup()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void schemaChangeInternal(String query)
    {
        delegate().schemaChangeInternal(query);
    }

    @Override
    public int getMessagingVersion()
    {
        return delegate().getMessagingVersion();
    }

    @Override
    public void setMessagingVersion(InetSocketAddress endpoint, int version)
    {
        delegate().setMessagingVersion(endpoint, version);
    }

    @Override
    public String getReleaseVersionString()
    {
        return delegate().getReleaseVersionString();
    }

    public void flush(String keyspace)
    {
        delegate().flush(keyspace);
    }

    public void forceCompact(String keyspace, String table)
    {
        delegate().forceCompact(keyspace, table);
    }

    @Override
    public IInstanceConfig config()
    {
        return delegate().config();
    }

    @Override
    public ICoordinator coordinator()
    {
        // TODO: stash and clear coordinator on startup/shutdown?
        return delegate().coordinator();
    }

    public IListen listen()
    {
        return delegate().listen();
    }

    @Override
    public Future<Void> shutdown()
    {
        return delegate().shutdown();
    }

    @Override
    public IIsolatedExecutor with(ExecutorService executor)
    {
        return delegate().with(executor);
    }

    @Override
    public Executor executor()
    {
        return delegate().executor();
    }

    @Override
    public void startup(ICluster cluster)
    {
        delegateForStartup().startup(cluster);
    }

    @Override
    public void postStartup()
    {
        delegateForStartup().postStartup();
    }

    @Override
    public void receiveMessage(IMessage message)
    {
        delegate().receiveMessage(message);
    }

    @Override
    public void receiveMessageWithInvokingThread(IMessage message)
    {
        delegate().receiveMessageWithInvokingThread(message);
    }

    @Override
    public <O> CallableNoExcept<Future<O>> async(CallableNoExcept<O> call)
    {
        return delegate().async(call);
    }

    @Override
    public <O> CallableNoExcept<O> sync(CallableNoExcept<O> call)
    {
        return delegate().sync(call);
    }

    @Override
    public CallableNoExcept<Future<?>> async(Runnable run)
    {
        return delegate().async(run);
    }

    @Override
    public Runnable sync(Runnable run)
    {
        return delegate().sync(run);
    }

    @Override
    public <I> Function<I, Future<?>> async(Consumer<I> consumer)
    {
        return delegate().async(consumer);
    }

    @Override
    public <I> Consumer<I> sync(Consumer<I> consumer)
    {
        return delegate().sync(consumer);
    }

    @Override
    public <I1, I2> BiFunction<I1, I2, Future<?>> async(BiConsumer<I1, I2> consumer)
    {
        return delegate().async(consumer);
    }

    @Override
    public <I1, I2> BiConsumer<I1, I2> sync(BiConsumer<I1, I2> consumer)
    {
        return delegate().sync(consumer);
    }

    @Override
    public <I1, I2, I3> TriFunction<I1, I2, I3, Future<?>> async(TriConsumer<I1, I2, I3> consumer)
    {
        return delegate().async(consumer);
    }

    @Override
    public <I1, I2, I3> TriConsumer<I1, I2, I3> sync(TriConsumer<I1, I2, I3> consumer)
    {
        return delegate().sync(consumer);
    }

    @Override
    public <I, O> Function<I, Future<O>> async(Function<I, O> f)
    {
        return delegate().async(f);
    }

    @Override
    public <I, O> Function<I, O> sync(Function<I, O> f)
    {
        return delegate().sync(f);
    }

    @Override
    public <I1, I2, O> BiFunction<I1, I2, Future<O>> async(BiFunction<I1, I2, O> f)
    {
        return delegate().async(f);
    }

    @Override
    public <I1, I2, O> BiFunction<I1, I2, O> sync(BiFunction<I1, I2, O> f)
    {
        return delegate().sync(f);
    }

    @Override
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> async(TriFunction<I1, I2, I3, O> f)
    {
        return delegate().async(f);
    }

    @Override
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, O> sync(TriFunction<I1, I2, I3, O> f)
    {
        return delegate().sync(f);
    }

    @Override
    public <I1, I2, I3, I4, O> QuadFunction<I1, I2, I3, I4, Future<O>> async(QuadFunction<I1, I2, I3, I4, O> f)
    {
        return delegate().async(f);
    }

    @Override
    public <I1, I2, I3, I4, O> QuadFunction<I1, I2, I3, I4, O> sync(QuadFunction<I1, I2, I3, I4, O> f)
    {
        return delegate().sync(f);
    }

    @Override
    public <I1, I2, I3, I4, I5, O> QuintFunction<I1, I2, I3, I4, I5, Future<O>> async(QuintFunction<I1, I2, I3, I4, I5, O> f)
    {
        return delegate().async(f);
    }

    @Override
    public <I1, I2, I3, I4, I5, O> QuintFunction<I1, I2, I3, I4, I5, O> sync(QuintFunction<I1, I2, I3, I4, I5, O> f)
    {
        return delegate().sync(f);
    }
}
