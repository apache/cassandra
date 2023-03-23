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

package org.apache.cassandra.utils.concurrent;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.assertj.core.api.Assertions;

public class TimerTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testWaitForExpiration() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);

        Future<Void> result = Timer.INSTANCE.onTimeout(latch::countDown, 1, TimeUnit.SECONDS);
        result.get(3, TimeUnit.SECONDS);

        Assertions.assertThat(latch.await(0, TimeUnit.MILLISECONDS)).isTrue();
        Assertions.assertThat(result.isDone()).isTrue();
    }

    @Test
    public void testCancelExpiration() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);

        Future<Void> result = Timer.INSTANCE.onTimeout(latch::countDown, 1, TimeUnit.SECONDS);
        result.cancel(true);

        Assertions.assertThat(latch.await(3, TimeUnit.SECONDS)).isFalse();
        Assertions.assertThat(result.isDone()).isFalse();
        Assertions.assertThat(result.isCancelled()).isTrue();
    }

    @Test
    public void testExecutorsLocalPropagation() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);

        ClientWarn.instance.set(new ClientWarn.State());
        ClientWarn.instance.warn("test");
        Tracing.instance.set(Tracing.instance.get(Tracing.instance.newSession(ClientState.forInternalCalls(), Tracing.TraceType.NONE)));
        UUID sessionId = Tracing.instance.get().sessionId;
        Future<Void> result = Timer.INSTANCE.onTimeout(() ->
                                                 {
                                                     if (ClientWarn.instance.getWarnings().contains("test") &&
                                                         Tracing.instance.get().sessionId.equals(sessionId))
                                                         latch.countDown();
                                                 }, 1, TimeUnit.SECONDS, ExecutorLocals.create());

        result.get(3, TimeUnit.SECONDS);

        Assertions.assertThat(latch.await(0, TimeUnit.MILLISECONDS)).isTrue();
        Assertions.assertThat(result.isDone()).isTrue();
    }
}
