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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;

import accord.coordinate.Exhausted;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.impl.IntKey;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.Blocking;
import org.assertj.core.api.Condition;
import org.mockito.Mockito;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.AccordService.doWithRetries;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AccordServiceTest
{
    @Test
    public void retryExpectedFailures() throws InterruptedException
    {
        Blocking blocking = Mockito.mock(Blocking.class);
        class Task implements Supplier<Seekables>
        {
            private int attempts = 0;

            @Override
            public Seekables get()
            {
                switch (attempts)
                {
                    case 0:
                        attempts++;
                        throw new Timeout(null, null);
                    case 1:
                        attempts++;
                        throw AccordService.newBarrierTimeout(TxnId.NONE, true);
                    case 2:
                        attempts++;
                        throw new Preempted(null, null);
                    case 3:
                        attempts++;
                        throw AccordService.newBarrierPreempted(TxnId.NONE, true);
                    case 4:
                        attempts++;
                        throw new Exhausted(null, null);
                    case 5:
                        attempts++;
                        throw AccordService.newBarrierExhausted(TxnId.NONE, true);
                    default:
                        return Ranges.of(IntKey.range(1, 2));
                }
            }
        }
        Task failing = new Task();
        assertThat(doWithRetries(blocking, failing, Integer.MAX_VALUE, 100, 1000)).isEqualTo(Ranges.of(IntKey.range(1,2)));
        verify(blocking).sleep(100);
        verify(blocking).sleep(200);
        verify(blocking).sleep(400);
        verify(blocking).sleep(800);
        verify(blocking, times(2)).sleep(1000); // hit max backoff, so stays at 1k
    }

    @Test
    public void retryThrowsTimeout()
    {
        Blocking blocking = Mockito.mock(Blocking.class);
        qt().check(rs -> {
            List<Runnable> timeoutFailures = new ArrayList<>(4);
            timeoutFailures.add(() -> {throw new Timeout(null, null);});
            timeoutFailures.add(() -> {throw AccordService.newBarrierTimeout(TxnId.NONE, true);});
            timeoutFailures.add(() -> {throw new Preempted(null, null);});
            timeoutFailures.add(() -> {throw AccordService.newBarrierPreempted(TxnId.NONE, true);});
            Collections.shuffle(timeoutFailures, rs.asJdkRandom());
            Iterator<Runnable> it = timeoutFailures.iterator();
            Supplier<Seekables> failing = () -> {
                if (!it.hasNext()) throw new IllegalStateException("Called too many times");
                it.next().run(); // this throws...
                return Ranges.EMPTY;
            };
            assertThatThrownBy(() -> doWithRetries(blocking, failing, timeoutFailures.size(), 100, 1000)).is(new Condition<>(AccordService::isTimeout, "timeout"));
            assertThat(it).isExhausted();
        });
    }

    @Test
    public void retryThrowsNonTimeout()
    {
        Blocking blocking = Mockito.mock(Blocking.class);
        qt().check(rs -> {
            List<Runnable> timeoutFailures = new ArrayList<>(5);
            timeoutFailures.add(() -> {throw new Timeout(null, null);});
            timeoutFailures.add(() -> {throw AccordService.newBarrierTimeout(TxnId.NONE, true);});
            timeoutFailures.add(() -> {throw new Preempted(null, null);});
            timeoutFailures.add(() -> {throw AccordService.newBarrierPreempted(TxnId.NONE, true);});
            timeoutFailures.add(() -> {throw new Exhausted(null, null);});
            Collections.shuffle(timeoutFailures, rs.asJdkRandom());
            Iterator<Runnable> it = timeoutFailures.iterator();
            Supplier<Seekables> failing = () -> {
                if (!it.hasNext()) throw new IllegalStateException("Called too many times");
                it.next().run(); // this throws...
                return Ranges.EMPTY;
            };
            assertThatThrownBy(() -> doWithRetries(blocking, failing, timeoutFailures.size(), 100, 1000)).isInstanceOf(Exhausted.class);
            assertThat(it).isExhausted();
        });
    }

    @Test
    public void retryShortCircuitError()
    {
        class Unexpected implements Runnable
        {
            final boolean isError;

            Unexpected(boolean isError)
            {
                this.isError = isError;
            }

            @Override
            public void run()
            {
                if (isError) throw new AssertionError();
                throw new NullPointerException();
            }
        }
        qt().check(rs -> {
            List<Runnable> failures = new ArrayList<>(6);
            failures.add(() -> {throw new Timeout(null, null);});
            failures.add(() -> {throw AccordService.newBarrierTimeout(TxnId.NONE, true);});
            failures.add(() -> {throw new Preempted(null, null);});
            failures.add(() -> {throw AccordService.newBarrierPreempted(TxnId.NONE, true);});
            failures.add(() -> {throw new Exhausted(null, null);});
            boolean isError = rs.nextBoolean();
            failures.add(new Unexpected(isError));
            Collections.shuffle(failures, rs.asJdkRandom());
            int unexpectedIndex = -1;
            for (int i = 0; i < failures.size(); i++)
            {
                if (failures.get(i) instanceof Unexpected)
                {
                    unexpectedIndex = i;
                    break;
                }
            }
            Iterator<Runnable> it = failures.iterator();
            Supplier<Seekables> failing = () -> {
                if (!it.hasNext()) throw new IllegalStateException("Called too many times");
                it.next().run(); // this throws...
                return Ranges.EMPTY;
            };
            Blocking blocking = Mockito.mock(Blocking.class);
            assertThatThrownBy(() -> doWithRetries(blocking, failing, failures.size(), 100, 1000)).isInstanceOf(isError ? AssertionError.class : NullPointerException.class);
            verify(blocking, times(unexpectedIndex)).sleep(Mockito.anyLong());
        });
    }
}