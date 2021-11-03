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
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.utils.Throwables;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.fail;

public class LoadingMapTest
{
    private LoadingMap<Integer, String> map;
    private final DebuggableThreadPoolExecutor executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("TEST", 10);
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
        Instant deadline = Instant.now().plus(Duration.ofSeconds(5));
        while (executor.getPendingTaskCount() > 0 || executor.getActiveTaskCount() > 0)
        {
            if (Instant.now().isAfter(deadline))
                throw new TimeoutException();

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }

        b1.reset();
        b2.reset();
        f1 = f2 = null;
    }

    @Test
    public void compute()
    {
        assertThat(map.compute(1, (k, v) -> {
            assertThat(k).isEqualTo(1);
            assertThat(v).isNull();
            return "one";
        })).isEqualTo("one");
        assertThat(map.get(1)).isEqualTo("one");

        assertThat(map.compute(1, (k, v) -> {
            assertThat(k).isEqualTo(1);
            assertThat(v).isEqualTo("one");
            return "1";
        })).isEqualTo("1");
        assertThat(map.get(1)).isEqualTo("1");

        assertThat(map.compute(1, (k, v) -> {
            assertThat(k).isEqualTo(1);
            assertThat(v).isEqualTo("1");
            return null;
        })).isNull();
        assertThat(map.get(1)).isNull();

        assertThat(map.compute(1, (k, v) -> {
            assertThat(k).isEqualTo(1);
            assertThat(v).isNull();
            return null;
        })).isNull();
        assertThat(map.get(1)).isNull();
    }

    @Test
    public void computeIfAbsentOrIfMissing()
    {
        assertThat(map.computeIfPresent(1, (k, v) -> {
            fail();
            return "one";
        })).isNull();
        assertThat(map.get(1)).isNull();

        assertThat(map.computeIfAbsent(1, k -> {
            assertThat(k).isEqualTo(1);
            return "one";
        })).isEqualTo("one");
        assertThat(map.get(1)).isEqualTo("one");

        assertThat(map.computeIfAbsent(1, k -> {
            fail();
            return "1";
        })).isEqualTo("one");
        assertThat(map.get(1)).isEqualTo("one");

        assertThat(map.computeIfPresent(1, (k, v) -> {
            assertThat(k).isEqualTo(1);
            assertThat(v).isEqualTo("one");
            return "1";
        })).isEqualTo("1");
        assertThat(map.get(1)).isEqualTo("1");

        assertThat(map.computeIfPresent(1, (k, v) -> {
            assertThat(k).isEqualTo(1);
            assertThat(v).isEqualTo("1");
            return null;
        })).isNull();
        assertThat(map.get(1)).isNull();

        assertThat(map.computeIfPresent(1, (k, v) -> {
            fail();
            return "one";
        })).isNull();
        assertThat(map.get(1)).isNull();
    }

    @Test
    public void values() throws Exception
    {
        map.computeIfAbsent(1, ignored -> "one");
        map.computeIfAbsent(2, ignored -> "two");
        map.computeIfAbsent(3, ignored -> "three");
        executor.submit(() -> map.compute(2, (k, v) -> {
            Throwables.maybeFail(b1::await);
            return "2";
        }));
        executor.submit(() -> map.compute(3, (k, v) -> {
            Throwables.maybeFail(b2::await);
            return null;
        }));

        Stream<String> s = map.valuesStream(); // we should not need to wait for the stream
        Future<Set<String>> f = executor.submit(() -> s.collect(Collectors.toSet()));
        assertThat(f).isNotDone();
        b1.await();
        assertThat(f).isNotDone();
        b2.await();
        await().untilAsserted(() -> assertThat(f.get()).containsExactlyInAnyOrder("one", "2"));
    }

    @Test
    public void loadForDifferentKeysShouldNotBlockEachOther() throws Exception
    {
        f1 = submitLoad(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter loading function

        f2 = submitLoad(2, "two", b2, null);
        await().untilAsserted(() -> assertThat(b2.getNumberWaiting()).isGreaterThan(0)); // wait until we enter loading function

        assertThat(map.getUnsafe(1)).isNotNull().isNotDone();
        assertThat(map.getUnsafe(2)).isNotNull().isNotDone();
        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        // since we were able to enter both loading functions, it means the can work concurrently

        b2.await();
        assertFuture(f2, "two");

        assertThat(f1).isNotDone();

        b1.await();
        assertFuture(f1, "one");

        assertThat(map.get(1)).isEqualTo("one");
        assertThat(map.get(2)).isEqualTo("two");
    }

    @Test
    public void loadInsideLoadShouldNotCauseDeadlock()
    {
        String v = map.compute(1, (ignoredKey, ignoredExisting) -> {
            assertThat(map.compute(2, (_ignoredKey, _ignoredExisting) -> "two")).isEqualTo("two");
            return "one";
        });

        assertThat(v).isEqualTo("one");

        assertThat(map.get(1)).isEqualTo("one");
        assertThat(map.get(2)).isEqualTo("two");
    }

    @Test
    public void unloadForDifferentKeysShouldNotBlockEachOther() throws Exception
    {
        initMap();

        f1 = submitUnload(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter unloading function

        f2 = submitUnload(2, "two", b2, null);
        await().untilAsserted(() -> assertThat(b2.getNumberWaiting()).isGreaterThan(0)); // wait until we enter unloading function

        assertThat(map.getUnsafe(1)).isNotNull().isNotDone();
        assertThat(map.getUnsafe(2)).isNotNull().isNotDone();
        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        // since we were able to enter both unloading functions, it means the can work concurrently

        b2.await();
        assertFuture(f2, "two");

        assertThat(f1).isNotDone();

        b1.await();
        assertFuture(f1, "one");

        assertThat(map.getUnsafe(1)).isNull();
        assertThat(map.getUnsafe(2)).isNull();
    }

    @Test
    public void unloadInsideUnloadShouldNotCauseDeadlock()
    {
        initMap();
        AtomicReference<String> removed1 = new AtomicReference<>();
        AtomicReference<String> removed2 = new AtomicReference<>();

        assertThat(map.compute(1, (ignoredKey1, v1) -> {
            if (v1 == null)
                return null;

            assertThat(map.getUnsafe(1)).isNotDone();
            assertThat(map.compute(2, (ignoredKey2, v2) -> {
                if (v2 == null)
                    return null;

                assertThat(map.getUnsafe(2)).isNotDone();
                removed2.set(v2);
                return null;
            })).isNull();
            assertThat(removed2.get()).isEqualTo("two");
            removed1.set(v1);
            return null;
        })).isNull();

        assertThat(removed1.get()).isEqualTo("one");

        assertThat(map.getUnsafe(1)).isNull();
        assertThat(map.getUnsafe(2)).isNull();
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
        assertThat(map.get(1)).isEqualTo("one");
        assertThat(b2.getNumberWaiting()).isZero();
    }

    @Test
    public void twoConcurrentUnloadAttemptsFirstOneShouldWin() throws Exception
    {
        initMap();
        f1 = submitUnload(1, "one", b1, null);
        await().untilAsserted(() -> assertThat(b1.getNumberWaiting()).isGreaterThan(0)); // wait until we enter unloading function

        f2 = submitUnload(1, "one", b2, null);
        assertThat(f2).isNotDone();

        b1.await(); // let f1 continue
        assertFuture(f1, "one");

        assertThat(map.get(1)).isNull();
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
        assertThat(map.get(1)).isEqualTo("two");
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
        assertThat(map.get(1)).isNull();
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
        f1 = submitLoad2(1, null, null, null);
        assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> f1.get(5, TimeUnit.SECONDS))
                                                           .withCauseInstanceOf(NullPointerException.class);

        assertThat(map.get(1)).isNull();
        assertThat(map.get(2)).isNull();
    }

    @Test
    public void failedLoad()
    {
        f1 = submitLoad(1, null, null, () -> {
            throw new RuntimeException("abc");
        });
        assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> f1.get(5, TimeUnit.SECONDS))
                                                           .withCauseInstanceOf(RuntimeException.class)
                                                           .withMessageContaining("abc");

        assertThat(map.getUnsafe(1)).isNull();
        assertThat(map.getUnsafe(2)).isNull();
    }

    @Test
    public void failedUnload()
    {
        initMap();

        f1 = submitUnload(1, "one", null, () -> {
            throw new RuntimeException("abc");
        });

        assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> f1.get(5, TimeUnit.SECONDS))
                                                           .withCauseInstanceOf(RuntimeException.class)
                                                           .withMessageContaining("abc");

        assertThat(map.get(1)).isEqualTo("one");
        assertThat(map.get(2)).isEqualTo("two");
    }

    @Test
    public void failedUnload2()
    {
        initMap();

        f1 = submitUnload2(1, "one", null, () -> {
            throw new RuntimeException("abc");
        });
        assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> f1.get(5, TimeUnit.SECONDS))
                                                           .withCauseInstanceOf(LoadingMap.UnloadExecutionException.class)
                                                           .withRootCauseInstanceOf(RuntimeException.class)
                                                           .withMessageContaining("abc")
                                                           .matches(ex -> {

                                                               return ((LoadingMap.UnloadExecutionException) ex.getCause()).value().equals("one");
                                                           });

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
                        map.compute(1, (ignoredKey, existing) -> {
                            if (existing != null)
                                return existing;
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
                        map.compute(1, (ignoredKey, v) -> {
                            if (v == null)
                                return null;
                            int s = state.incrementAndGet();
                            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(50, 100), TimeUnit.MILLISECONDS);
                            if (!state.compareAndSet(s, s + 1))
                                failures.incrementAndGet();
                            if (ThreadLocalRandom.current().nextInt(100) < 10)
                                throw new RuntimeException();
                            return null;
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
        try
        {
            assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo(v);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw Throwables.unchecked(e);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    private void assertFutures(String v1, String v2)
    {
        assertFuture(f1, v1);
        assertFuture(f2, v2);
    }

    private Future<String> submitLoad(int key, String value, CyclicBarrier b, Throwables.DiscreteAction<?> extraAction)
    {
        return executor.submit(() -> map.compute(key, (ignoredKey, existing) -> {
            if (existing != null)
                return existing;

            Throwable t = null;
            if (extraAction != null)
                t = Throwables.perform(t, extraAction);
            if (b != null)
                t = Throwables.perform(t, b::await);
            if (t != null)
                throw Throwables.unchecked(t);
            return value;
        }));
    }

    private Future<String> submitLoad2(int key, String value, CyclicBarrier b, Throwables.DiscreteAction<?> extraAction)
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
        return executor.submit(() -> {
            AtomicReference<String> removedValue = new AtomicReference<>();
            map.compute(key, (ignoredKey, v) -> {
                if (v == null)
                    return null;
                removedValue.set(v);
                assertThat(v).isEqualTo(expectedValue);
                Throwable t = null;
                if (extraAction != null)
                    t = Throwables.perform(t, extraAction);
                if (b != null)
                    t = Throwables.perform(t, b::await);
                if (t != null)
                    throw Throwables.unchecked(t);
                return null;
            });
            return removedValue.get();
        });
    }

    private Future<String> submitUnload2(int key, String expectedValue, CyclicBarrier b, Throwables.DiscreteAction<?> extraAction)
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
        map.compute(1, (ignoredKey, ignoredValue) -> "one");
        map.compute(2, (ignoredKey, ignoredValue) -> "two");

        assertThat(map.get(1)).isEqualTo("one");
        assertThat(map.get(2)).isEqualTo("two");
    }

    private ConditionFactory await()
    {
        return Awaitility.await().pollDelay(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS);
    }
}
