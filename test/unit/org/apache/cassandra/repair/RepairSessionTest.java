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

package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RepairSessionTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testConviction() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        // Set up RepairSession
        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> repairRange = new Range<>(p.getToken(ByteBufferUtil.bytes(0)), p.getToken(ByteBufferUtil.bytes(100)));
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);
        RepairSession session = new RepairSession(parentSessionId, sessionId,
                                                  new CommonRange(endpoints, Collections.emptySet(), Arrays.asList(repairRange)),
                                                  "Keyspace1", RepairParallelism.SEQUENTIAL,
                                                  false, false,
                                                  PreviewKind.NONE, false, "Standard1");

        // perform convict
        session.convict(remote, Double.MAX_VALUE);

        // RepairSession should throw ExecutorException with the cause of IOException when getting its value
        assertSessionFails(session);
    }

    private void assertSessionFails(RepairSession session) throws InterruptedException
    {
        try
        {
            session.get();
            fail();
        }
        catch (ExecutionException ex)
        {
            assertEquals(IOException.class, ex.getCause().getClass());
        }
    }

    private static class NoopExecutorService implements ListeningExecutorService
    {
        @Override
        public void shutdown()
        {
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return null;
        }

        @Override
        public boolean isShutdown()
        {
            return false;
        }

        @Override
        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return false;
        }

        @Override
        public <T> ListenableFuture<T> submit(Callable<T> callable)
        {
            return null;
        }

        @Override
        public ListenableFuture<?> submit(Runnable runnable)
        {
            return null;
        }

        @Override
        public <T> ListenableFuture<T> submit(Runnable runnable, T t)
        {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection) throws InterruptedException
        {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) throws InterruptedException
        {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
        {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return null;
        }

        @Override
        public void execute(Runnable command)
        {

        }
    }

    @Test
    public void testRepairingDeadNodeFails() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);
        // Mark remote as dead
        Gossiper.instance.convict(remote, Double.MAX_VALUE);

        // Set up RepairSession
        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> repairRange = new Range<>(p.getToken(ByteBufferUtil.bytes(0)), p.getToken(ByteBufferUtil.bytes(100)));
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);
        RepairSession session = new RepairSession(parentSessionId, sessionId,
                                                  new CommonRange(endpoints, Collections.emptySet(), Arrays.asList(repairRange)),
                                                  "Keyspace1", RepairParallelism.SEQUENTIAL,
                                                  false, false,
                                                  PreviewKind.NONE, false, "Standard1");

        NoopExecutorService executor = new NoopExecutorService();
        session.start(executor);

        // RepairSession should fail when trying to repair a dead node
        assertSessionFails(session);
    }
}
