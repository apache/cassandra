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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraIncomingFile;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableZeroCopyWriter;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Shared;
import org.assertj.core.api.Assertions;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class StreamFailureTest extends TestBaseImpl
{

    private static final int FAILINGNODE = 2;

    private static final Logger logger = LoggerFactory.getLogger(StreamFailureTest.class);


    @Test
    public void failureInTheMiddleWithUnknown() throws IOException
    {
        streamTest(true, "java.lang.RuntimeException: TEST", FAILINGNODE);
    }

    @Test
    public void failureInTheMiddleWithEOF() throws IOException
    {
        streamTest(false, "Session peer /127.0.0.1:7012 Failed because there was an java.nio.channels.ClosedChannelException with state=STREAMING", FAILINGNODE);
    }

    @Test
    public void failureDueToSessionFailed() throws IOException
    {
        streamTest(true,"Remote peer /127.0.0.2:7012 failed stream session", 1);
    }

    @Test
    public void failureDueToSessionTimeout() throws IOException
    {
        streamTest2("Failed because the session timed out");
    }

    private void streamTest(boolean zeroCopyStreaming, String reason, Integer failedNode) throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withInstanceInitializer(BBHelper::install)
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set("stream_entire_sstables", zeroCopyStreaming)
                                                        // when die, this will try to halt JVM, which is easier to validate in the test
                                                        // other levels require checking state of the subsystems
                                                        .set("disk_failure_policy", "die"))
                                      .start())
        {
            init(cluster);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY)"));

            triggerStreaming(cluster, zeroCopyStreaming);
            // make sure disk failure policy is not triggered

            IInvokableInstance failingNode = cluster.get(failedNode);

            searchForLog(failingNode, reason);
        }
    }

    private void streamTest2(String reason) throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withInstanceInitializer(BB::install)
                                      .withConfig(c -> c.with(Feature.values())
                                                        // when die, this will try to halt JVM, which is easier to validate in the test
                                                        // other levels require checking state of the subsystems
                                                        .set("timeout_delay", "1ms"))
                                      .start())
        {

            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY)"));

            ForkJoinPool.commonPool().execute(() -> triggerStreaming(cluster, true));
            State.STREAM_IS_RUNNING.await();
            logger.info("Streaming is running... time to wake it up");
            State.UNBLOCK_STREAM.signal();

            IInvokableInstance failingNode = cluster.get(1);

            searchForLog(failingNode, reason);


        }
    }


    private void triggerStreaming(Cluster cluster, boolean expectedEntireSSTable)
    {
        IInvokableInstance node1 = cluster.get(1);
        IInvokableInstance node2 = cluster.get(2);

        // repair will do streaming IFF there is a mismatch; so cause one
        for (int i = 0; i < 10; i++)
            node1.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), i); // timestamp won't match, causing a mismatch

        // trigger streaming; expected to fail as streaming socket closed in the middle (currently this is an unrecoverable event)
        //Blocks until the stream is complete
        node2.nodetoolResult("repair", "-full", KEYSPACE, "tbl").asserts().failure();
    }

    private void searchForLog(IInvokableInstance failingNode, String reason)
    {
        LogResult<List<String>> result = failingNode.logs().grepForErrors(-1, Pattern.compile("Stream failed:"));
        // grepForErrors will include all ERROR logs even if they don't match the pattern; for this reason need to filter after the fact
        List<String> matches = result.getResult();

        matches = matches.stream().filter(s -> s.startsWith("WARN") && s.contains("Stream failed")).collect(Collectors.toList());
        logger.info("Stream failed logs found: {}", String.join("\n", matches));

        Assertions.assertThat(matches)
                  .describedAs("node%d expected 1 element but was not true", failingNode.config().num()).hasSize(1);
        String logLine = matches.get(0);
        Assertions.assertThat(logLine).contains(reason);

        Matcher match = Pattern.compile(".*\\[Stream #(.*)\\]").matcher(logLine);
        if (!match.find()) throw new AssertionError("Unable to parse: " + logLine);
        UUID planId = UUID.fromString(match.group(1));
        SimpleQueryResult qr = failingNode.executeInternalWithResult("SELECT * FROM system_views.streaming WHERE id=?", planId);
        Assertions.assertThat(qr.hasNext()).isTrue();
        Assertions.assertThat(qr.next().getString("failure_cause")).contains(reason);
    }

    @Shared
    public static class TestCondition
    {
        private volatile boolean signaled = false;

        public void await()
        {
            while (!signaled)
            {
                synchronized (this)
                {
                    try
                    {
                        this.wait();
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                }
            }
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

    @Shared
    public static class State
    {
        public static final TestCondition STREAM_IS_RUNNING = new TestCondition();
        public static final TestCondition UNBLOCK_STREAM = new TestCondition();

    }

    public static class BB
    {
        @SuppressWarnings("unused")
        public static int writeDirectlyToChannel(ByteBuffer buf, @SuperCall Callable<Integer> zuper) throws Exception
        {
            if (isCaller(BigTableZeroCopyWriter.class.getName(), "write"))
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
                throw new java.nio.channels.ClosedChannelException();
            // different context; pass through
            return zuper.call();
        }

        private static boolean isCaller(String klass, String method)
        {
            //TODO is there a cleaner way to check this?
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            System.out.println("CURRENTTTT THREAD: " + Thread.currentThread().getName());
            for (int i = 0; i < stack.length; i++)
            {
                StackTraceElement e = stack[i];
                System.out.println("KLASS: " + klass + " e.getClassName " + e.getClassName() + " e.getMethodName " + e.getMethodName());
                if (klass.equals(e.getClassName()) && method.equals(e.getMethodName()))
                    return true;
            }
            return false;
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != FAILINGNODE)
                return;
            new ByteBuddy().rebase(SequentialWriter.class)
                           .method(named("writeDirectlyToChannel").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(RangeAwareSSTableWriter.class)
                           .method(named("append").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

    }

    public static class BBHelper
    {
        @SuppressWarnings("unused")
        public static int writeDirectlyToChannel(ByteBuffer buf, @SuperCall Callable<Integer> zuper) throws Exception
        {
            if (isCaller(BigTableZeroCopyWriter.class.getName(), "write"))
                //throw new java.nio.channels.ClosedChannelException();
                throw new RuntimeException("TEST");
            // different context; pass through
            return zuper.call();
        }

        @SuppressWarnings("unused")
        public static boolean append(UnfilteredRowIterator partition, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (isCaller(CassandraIncomingFile.class.getName(), "read")) // handles compressed and non-compressed
                throw new java.nio.channels.ClosedChannelException();
            // different context; pass through
            return zuper.call();
        }

        private static boolean isCaller(String klass, String method)
        {
            //TODO is there a cleaner way to check this?
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            System.out.println("CURRENT THREAD: " + Thread.currentThread().getName());
            for (int i = 0; i < stack.length; i++)
            {
                StackTraceElement e = stack[i];
                System.out.println("KLASS: " + klass + " e.getClassName " + e.getClassName() + " e.getMethodName " + e.getMethodName());
                if (klass.equals(e.getClassName()) && method.equals(e.getMethodName()))
                    return true;
            }
            return false;
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != FAILINGNODE)
                return;
            new ByteBuddy().rebase(SequentialWriter.class)
                           .method(named("writeDirectlyToChannel").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(RangeAwareSSTableWriter.class)
                           .method(named("append").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

        }
    }
}
