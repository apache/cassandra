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

package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.UncheckedTimeoutException;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraIncomingFile;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.io.sstable.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableZeroCopyWriter;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Shared;
import org.awaitility.Awaitility;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class StreamFailureLogsFailureDueToSessionTimeoutTest extends AbstractStreamFailureLogs
{
    @Test
    public void failureDueToSessionTimeout() throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withInstanceInitializer(BBStreamTimeoutHelper::install)
                                      .withConfig(c -> c.with(Feature.values())
                                                        // when die, this will try to halt JVM, which is easier to validate in the test
                                                        // other levels require checking state of the subsystems
                                                        .set("stream_transfer_task_timeout", "1ms"))
                                      .start())
        {

            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY)"));

            ForkJoinPool.commonPool().execute(() -> triggerStreaming(cluster));
            try
            {
                Awaitility.await("Did not see stream running or timed out")
                          .atMost(3, TimeUnit.MINUTES)
                          .until(() -> State.STREAM_IS_RUNNING.await(false) || searchForLog(cluster.get(1), false, "Session timed out"));
            }
            finally
            {
                State.UNBLOCK_STREAM.signal();
            }
            Awaitility.await("Unable to find 'Session timed out'")
                      .atMost(1, TimeUnit.MINUTES)
                      .until(() -> searchForLog(cluster.get(1), false, "Session timed out"));
        }
    }

    @Shared
    public static class State
    {
        public static final TestCondition STREAM_IS_RUNNING = new TestCondition();
        public static final TestCondition UNBLOCK_STREAM = new TestCondition();
    }

    @Shared
    public static class TestCondition
    {
        private volatile boolean signaled = false;

        public void await()
        {
            await(true);
        }

        public boolean await(boolean throwOnTimeout)
        {
            long deadlineNanos = Clock.Global.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (!signaled)
            {
                long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - Clock.Global.nanoTime());
                if (remainingMillis <= 0)
                {
                    if (throwOnTimeout) throw new UncheckedTimeoutException("Condition not met within 1 minute");
                    return false;
                }
                // await may block signal from triggering notify, so make sure not to block for more than 500ms
                remainingMillis = Math.min(remainingMillis, 500);
                synchronized (this)
                {
                    try
                    {
                        this.wait(remainingMillis);
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                }
            }
            return true;
        }

        public void signal()
        {
            signaled = true;
            synchronized (this)
            {
                this.notify();
            }
        }
    }

    public static class BBStreamTimeoutHelper
    {
        @SuppressWarnings("unused")
        public static int writeDirectlyToChannel(ByteBuffer buf, @SuperCall Callable<Integer> zuper) throws Exception
        {
            if (isCaller(SSTableZeroCopyWriter.class.getName(), "write"))
            {
                State.STREAM_IS_RUNNING.signal();
                State.UNBLOCK_STREAM.await();
            }
            // different context; pass through
            return zuper.call();
        }

        @SuppressWarnings("unused")
        public static boolean append(UnfilteredRowIterator partition, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (isCaller(CassandraIncomingFile.class.getName(), "read")) // handles compressed and non-compressed
            {
                State.STREAM_IS_RUNNING.signal();
                State.UNBLOCK_STREAM.await();
            }
            // different context; pass through
            return zuper.call();
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != FAILING_NODE)
                return;
            new ByteBuddy().rebase(SequentialWriter.class)
                           .method(named("writeDirectlyToChannel").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBStreamTimeoutHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(RangeAwareSSTableWriter.class)
                           .method(named("append").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBStreamTimeoutHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

    }
}
