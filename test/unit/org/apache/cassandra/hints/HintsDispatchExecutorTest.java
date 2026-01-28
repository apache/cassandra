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
package org.apache.cassandra.hints;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class HintsDispatchExecutorTest
{
    private static Object newDispatchHintsTask(HintsDispatchExecutor executor, HintsStore store, UUID hostId, boolean isTransfer) throws Exception
    {
        Class<?> taskClass = null;
        for (Class<?> c : HintsDispatchExecutor.class.getDeclaredClasses())
        {
            if ("DispatchHintsTask".equals(c.getSimpleName()))
            {
                taskClass = c;
                break;
            }
        }
        if (taskClass == null)
            throw new AssertionError("Could not find DispatchHintsTask inner class");

        Constructor<?> ctor = taskClass.getDeclaredConstructor(HintsDispatchExecutor.class, HintsStore.class, UUID.class, boolean.class);
        ctor.setAccessible(true);
        return ctor.newInstance(executor, store, hostId, isTransfer);
    }

    private static RateLimiter getRateLimiter(Object task) throws Exception
    {
        Field f = task.getClass().getDeclaredField("rateLimiter");
        f.setAccessible(true);
        return (RateLimiter) f.get(task);
    }

    private static void invokeMaybeUpdateRateLimiter(Object task) throws Exception
    {
        Method m = task.getClass().getDeclaredMethod("maybeUpdateRateLimiter");
        m.setAccessible(true);
        m.invoke(task);
    }

    @Test
    public void testHintedHandoffThrottleUpdatesRateLimiterInFlightTransferMode() throws Exception
    {
        int originalThrottleKiB = DatabaseDescriptor.getHintedHandoffThrottleInKiB();
        try
        {
            // Use transfer-mode (nodesCount=1) so the test doesn't need StorageService/token metadata initialized.
            DatabaseDescriptor.setHintedHandoffThrottleInKiB(1024);

            File directory = new File(Files.createTempDirectory(null));
            AtomicBoolean isPaused = new AtomicBoolean(false);
            HintsDispatchExecutor executor = new HintsDispatchExecutor(directory, 1, isPaused, (InetAddressAndPort ep) -> true);

            UUID hostId = UUID.randomUUID();
            HintsStore store = HintsStore.create(hostId, directory, ImmutableMap.of(), Collections.emptyList());

            Object task = newDispatchHintsTask(executor, store, hostId, true);
            RateLimiter rateLimiter = getRateLimiter(task);

            assertEquals(1024 * 1024.0, rateLimiter.getRate(), 0.0);

            DatabaseDescriptor.setHintedHandoffThrottleInKiB(2048);
            invokeMaybeUpdateRateLimiter(task);

            assertEquals(2048 * 1024.0, rateLimiter.getRate(), 0.0);
        }
        finally
        {
            DatabaseDescriptor.setHintedHandoffThrottleInKiB(originalThrottleKiB);
        }
    }

    @Test
    public void testHintedHandoffThrottleDisabledAndReEnabledTransferMode() throws Exception
    {
        int originalThrottleKiB = DatabaseDescriptor.getHintedHandoffThrottleInKiB();
        try
        {
            File directory = new File(Files.createTempDirectory(null));
            AtomicBoolean isPaused = new AtomicBoolean(false);
            HintsDispatchExecutor executor = new HintsDispatchExecutor(directory, 1, isPaused, (InetAddressAndPort ep) -> true);

            UUID hostId = UUID.randomUUID();
            HintsStore store = HintsStore.create(hostId, directory, ImmutableMap.of(), Collections.emptyList());

            DatabaseDescriptor.setHintedHandoffThrottleInKiB(0);
            Object task = newDispatchHintsTask(executor, store, hostId, true);
            RateLimiter rateLimiter = getRateLimiter(task);
            assertEquals(Double.MAX_VALUE, rateLimiter.getRate(), 0.0);

            DatabaseDescriptor.setHintedHandoffThrottleInKiB(1);
            invokeMaybeUpdateRateLimiter(task);
            assertEquals(1024.0, rateLimiter.getRate(), 0.0);

            DatabaseDescriptor.setHintedHandoffThrottleInKiB(0);
            invokeMaybeUpdateRateLimiter(task);
            assertEquals(Double.MAX_VALUE, rateLimiter.getRate(), 0.0);
        }
        finally
        {
            DatabaseDescriptor.setHintedHandoffThrottleInKiB(originalThrottleKiB);
        }
    }

    @Test
    public void testHintedHandoffThrottleUpdatesAccountForClusterSizeInNonTransferMode() throws Exception
    {
        int originalThrottleKiB = DatabaseDescriptor.getHintedHandoffThrottleInKiB();
        List<InetAddressAndPort> addedEndpoints = new ArrayList<>();
        try
        {
            File directory = new File(Files.createTempDirectory(null));
            AtomicBoolean isPaused = new AtomicBoolean(false);
            HintsDispatchExecutor executor = new HintsDispatchExecutor(directory, 1, isPaused, (InetAddressAndPort ep) -> true);

            UUID hostId = UUID.randomUUID();
            HintsStore store = HintsStore.create(hostId, directory, ImmutableMap.of(), Collections.emptyList());

            // Ensure token metadata has a controlled number of endpoints:
            // nodesCount = max(1, endpoints.size() - 1)
            // so endpoints=4 => nodesCount=3; endpoints=5 => nodesCount=4.
            for (int i = 1; i <= 4; i++)
            {
                InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0." + i);
                addedEndpoints.add(ep);
                StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), ep);
            }

            DatabaseDescriptor.setHintedHandoffThrottleInKiB(1024);
            Object task = newDispatchHintsTask(executor, store, hostId, false);
            RateLimiter rateLimiter = getRateLimiter(task);

            // 4 endpoints => nodesCount = 3
            assertEquals((1024 * 1024.0) / 3.0, rateLimiter.getRate(), 0.0);

            // Update throttle and verify the running task picks it up
            DatabaseDescriptor.setHintedHandoffThrottleInKiB(2048);
            invokeMaybeUpdateRateLimiter(task);
            assertEquals((2048 * 1024.0) / 3.0, rateLimiter.getRate(), 0.0);

            // Increase cluster size and verify the running task rescales without restart
            InetAddressAndPort ep5 = InetAddressAndPort.getByName("127.0.0.5");
            addedEndpoints.add(ep5);
            StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), ep5);

            invokeMaybeUpdateRateLimiter(task);
            // 5 endpoints => nodesCount = 4
            assertEquals((2048 * 1024.0) / 4.0, rateLimiter.getRate(), 0.0);
        }
        finally
        {
            // Best-effort cleanup of TokenMetadata changes so tests don't leak state.
            for (InetAddressAndPort ep : addedEndpoints)
            {
                try
                {
                    StorageService.instance.getTokenMetadata().removeEndpoint(ep);
                }
                catch (Throwable t)
                {
                    // ignore cleanup failures; other tests should not depend on these endpoints existing
                }
            }
            DatabaseDescriptor.setHintedHandoffThrottleInKiB(originalThrottleKiB);
        }
    }
}

