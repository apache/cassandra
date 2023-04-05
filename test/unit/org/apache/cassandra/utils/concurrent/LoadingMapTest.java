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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.Throwables;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.assertj.core.api.Assertions.assertThat;

public class LoadingMapTest
{
    private LoadingMap<Integer, String> map;
    private final ExecutorPlus executor = executorFactory().pooled("TEST", 10);
    private final CyclicBarrier b1 = new CyclicBarrier(2);
    private final CyclicBarrier b2 = new CyclicBarrier(2);

    private Future<String> f1, f2;

    @Before
    public void beforeTest()
    {
        map = new LoadingMap<>();
    }

    @After
    public void afterTest() throws TimeoutException
    {
        Instant deadline = now().plus(Duration.ofSeconds(5));
        while (executor.getPendingTaskCount() > 0 || executor.getActiveTaskCount() > 0)
        {
            if (now().isAfter(deadline))
                throw new TimeoutException();

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }

        b1.reset();
        b2.reset();
        f1 = f2 = null;
    }

    @Test
    public void loadForDifferentKeysShouldNotBlockEachOther() throws Exception
    {
        f1 = submitLoad(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter loading function

        f2 = submitLoad(2, "two", b2, null);
        await().untilAsserted(() -> assertThat(b2.getNumberWaiting()).isGreaterThan(0)); // wait until we enter loading function

        assertThat(map.get(1)).isNotNull();
        assertThat(map.get(2)).isNotNull();
        assertThat(map.getIfReady(1)).isNull();
        assertThat(map.getIfReady(2)).isNull();
        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        // since we were able to enter both loading functions, it means the can work concurrently

        b2.await();
        assertFuture(f2, "two");

        assertThat(f1).isNotDone();

        b1.await();
        assertFuture(f1, "one");

        assertThat(map.getIfReady(1)).isEqualTo("one");
        assertThat(map.getIfReady(2)).isEqualTo("two");
    }

    @Test
    public void loadInsideLoadShouldNotCauseDeadlock()
    {
        String v = map.blockingLoadIfAbsent(1, () -> {
            assertThat(map.blockingLoadIfAbsent(2, () -> "two")).isEqualTo("two");
            return "one";
        });

        assertThat(v).isEqualTo("one");

        assertThat(map.getIfReady(1)).isEqualTo("one");
        assertThat(map.getIfReady(2)).isEqualTo("two");
    }

    @Test
    public void unloadForDifferentKeysShouldNotBlockEachOther() throws Exception
    {
        initMap();

        f1 = submitUnload(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter unloading function

        f2 = submitUnload(2, "two", b2, null);
        await().untilAsserted(() -> assertThat(b2.getNumberWaiting()).isGreaterThan(0)); // wait until we enter unloading function

        assertThat(map.get(1)).isNotNull();
        assertThat(map.get(2)).isNotNull();
        assertThat(map.getIfReady(1)).isNull();
        assertThat(map.getIfReady(2)).isNull();
        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        // since we were able to enter both unloading functions, it means the can work concurrently

        b2.await();
        assertFuture(f2, "two");

        assertThat(f1).isNotDone();

        b1.await();
        assertFuture(f1, "one");

        assertThat(map.get(1)).isNull();
        assertThat(map.get(2)).isNull();
    }

    @Test
    public void unloadInsideUnloadShouldNotCauseDeadlock() throws LoadingMap.UnloadExecutionException
    {
        initMap();

        String v = map.blockingUnloadIfPresent(1, v1 -> {
            assertThat(map.getIfReady(1)).isNull();

            try
            {
                assertThat(map.blockingUnloadIfPresent(2, v2 -> assertThat(map.getIfReady(2)).isNull())).isEqualTo("two");
            }
            catch (LoadingMap.UnloadExecutionException e)
            {
                throw Throwables.unchecked(e);
            }
        });

        assertThat(v).isEqualTo("one");

        assertThat(map.get(1)).isNull();
        assertThat(map.get(2)).isNull();
    }

    @Test
    public void twoConcurrentLoadAttemptsFirstOneShouldWin() throws Exception
    {
        f1 = submitLoad(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter loading function

        f2 = submitLoad(1, "two", b2, null);
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        b1.await();

        assertFutures("one", "one");
        assertThat(map.getIfReady(1)).isEqualTo("one");
        assertThat(b2.getNumberWaiting()).isZero();
    }

    @Test
    public void twoConcurrentUnloadAttemptsFirstOneShouldWin() throws Exception
    {
        initMap();
        f1 = submitUnload(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter unloading function

        f2 = submitUnload(1, "one", b2, null);

        assertFuture(f2, null); // f2 should return immediately

        b1.await(); // let f1 continue
        assertFuture(f1, "one");

        assertThat(map.getIfReady(1)).isNull();
        assertThat(b2.getNumberWaiting()).isZero();
    }

    @Test
    public void loadWhileUnloading() throws Exception
    {
        initMap();

        f1 = submitUnload(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter unloading function

        f2 = submitLoad(1, "two", null, null);
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        b1.await();

        assertFutures("one", "two");
        assertThat(map.getIfReady(1)).isEqualTo("two");
    }

    @Test
    public void unloadWhileLoading() throws Exception
    {
        f1 = submitLoad(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter loading function

        f2 = submitUnload(1, "one", null, null);
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        b1.await();

        assertFutures("one", "one");
        assertThat(map.getIfReady(1)).isNull();
    }

    @Test
    public void loadWhenExists()
    {
        initMap();

        f1 = submitLoad(1, "three", b1, null);
        assertThat(b1.getNumberWaiting()).isZero();
        assertFuture(f1, "one");
    }

    @Test
    public void unloadWhenMissing()
    {
        f1 = submitUnload(1, null, b1, null);
        assertThat(b1.getNumberWaiting()).isZero();
        assertFuture(f1, null);
    }

    @Test
    public void nullLoad()
    {
        f1 = submitLoad(1, null, null, null);
        f1.awaitThrowUncheckedOnInterrupt(5, TimeUnit.SECONDS);
        assertThat(f1.cause()).isInstanceOf(NullPointerException.class);

        assertThat(map.get(1)).isNull();
        assertThat(map.get(2)).isNull();
    }

    @Test
    public void failedLoad()
    {
        f1 = submitLoad(1, null, null, () -> {
            throw new RuntimeException("abc");
        });
        f1.awaitThrowUncheckedOnInterrupt(5, TimeUnit.SECONDS);
        assertThat(f1.cause()).isInstanceOf(RuntimeException.class);
        assertThat(f1.cause()).hasMessage("abc");

        assertThat(map.get(1)).isNull();
        assertThat(map.get(2)).isNull();
    }

    @Test
    public void failedUnload()
    {
        initMap();

        f1 = submitUnload(1, "one", null, () -> {
            throw new RuntimeException("abc");
        });
        f1.awaitThrowUncheckedOnInterrupt(5, TimeUnit.SECONDS);
        assertThat(f1.cause()).isInstanceOf(LoadingMap.UnloadExecutionException.class);
        LoadingMap.UnloadExecutionException ex = (LoadingMap.UnloadExecutionException) f1.cause();

        assertThat(ex).hasRootCauseInstanceOf(RuntimeException.class);
        assertThat(ex).hasRootCauseMessage("abc");
        assertThat((String) ex.value()).isEqualTo("one");

        assertThat(map.get(1)).isNull();
    }

    @Test
    public void fuzzTest()
    {
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger state = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(10);
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < 5; i++)
        {
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted() && !stop.get())
                {
                    try
                    {
                        barrier.await();
                        Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(50), TimeUnit.MILLISECONDS);
                        map.blockingLoadIfAbsent(1, () -> {
                            int s = state.get();
                            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(50, 100), TimeUnit.MILLISECONDS);
                            if (!state.compareAndSet(s, s + 1))
                                failures.incrementAndGet();
                            if (ThreadLocalRandom.current().nextInt(100) < 10)
                                return null;
                            if (ThreadLocalRandom.current().nextInt(100) < 10)
                                throw new RuntimeException();
                            return "one";
                        });
                    }
                    catch (InterruptedException e)
                    {
                        break;
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            });
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted() && !stop.get())
                {
                    try
                    {
                        barrier.await();
                        Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(50), TimeUnit.MILLISECONDS);
                        map.blockingUnloadIfPresent(1, v -> {
                            int s = state.incrementAndGet();
                            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(50, 100), TimeUnit.MILLISECONDS);
                            if (!state.compareAndSet(s, s + 1))
                                failures.incrementAndGet();
                            if (ThreadLocalRandom.current().nextInt(100) < 10)
                                throw new RuntimeException();
                        });
                    }
                    catch (InterruptedException e)
                    {
                        break;
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            });
        }

        Uninterruptibles.sleepUninterruptibly(15, TimeUnit.SECONDS);
        stop.set(true);
        while (executor.getActiveTaskCount() > 0)
        {
            barrier.reset();
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
        assertThat(failures.get()).isZero();
        assertThat(state.get()).isGreaterThan(0);
    }

    private void assertFuture(Future<String> f, String v)
    {
        assertThat(f.awaitThrowUncheckedOnInterrupt(5, TimeUnit.SECONDS)).isTrue();
        f.rethrowIfFailed();
        assertThat(f.getNow()).isEqualTo(v);
    }

    private void assertFutures(String v1, String v2)
    {
        assertFuture(f1, v1);
        assertFuture(f2, v2);
    }

    private Future<String> submitLoad(int key, String value, CyclicBarrier b, Throwables.DiscreteAction<?> extraAction)
    {
        return executor.submit(() -> map.blockingLoadIfAbsent(key, () -> {
            Throwable a = null;
            if (extraAction != null)
                a = Throwables.perform(a, extraAction);
            if (b != null)
                a = Throwables.perform(a, b::await);
            if (a != null)
                throw Throwables.unchecked(a);
            return value;
        }));
    }

    private Future<String> submitUnload(int key, String expectedValue, CyclicBarrier b, Throwables.DiscreteAction<?> extraAction)
    {
        return executor.submit(() -> map.blockingUnloadIfPresent(key, v -> {
            assertThat(v).isEqualTo(expectedValue);
            Throwable a = null;
            if (extraAction != null)
                a = Throwables.perform(a, extraAction);
            if (b != null)
                a = Throwables.perform(a, b::await);
            if (a != null)
                throw Throwables.unchecked(a);
        }));
    }

    private void initMap()
    {
        map.blockingLoadIfAbsent(1, () -> "one");
        map.blockingLoadIfAbsent(2, () -> "two");

        assertThat(map.getIfReady(1)).isEqualTo("one");
        assertThat(map.getIfReady(2)).isEqualTo("two");
    }

    private ConditionFactory await()
    {
        return Awaitility.await().pollDelay(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS);
    }
}