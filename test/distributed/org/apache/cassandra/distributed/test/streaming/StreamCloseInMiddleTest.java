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
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraIncomingFile;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableZeroCopyWriter;
import org.apache.cassandra.io.util.SequentialWriter;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class StreamCloseInMiddleTest extends TestBaseImpl
{
    @Test
    public void zeroCopy() throws IOException
    {
        streamClose(true);
    }

    @Test
    public void notZeroCopy() throws IOException
    {
        streamClose(false);
    }

    private void streamClose(boolean zeroCopyStreaming) throws IOException
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
            assertNoNodeShutdown(cluster);

            // now bootstrap a new node; streaming will fail
            IInvokableInstance node3 = ClusterUtils.addInstance(cluster, cluster.get(1).config(), c -> c.set("auto_bootstrap", true));
            node3.startup();
            for (String line : Arrays.asList("Error while waiting on bootstrap to complete. Bootstrap will have to be restarted", // bootstrap failed
                                             "Some data streaming failed. Use nodetool to check bootstrap state and resume")) // didn't join ring because bootstrap failed
                Assertions.assertThat(node3.logs().grep(line).getResult())
                          .hasSize(1);

            assertNoNodeShutdown(cluster);
        }
    }

    private void assertNoNodeShutdown(Cluster cluster)
    {
        AssertionError t = null;
        for (IInvokableInstance i : cluster.stream().collect(Collectors.toList()))
        {
            try
            {
                Assertions.assertThat(i.isShutdown()).describedAs("%s was shutdown; this is not expected", i).isFalse();
                Assertions.assertThat(i.killAttempts()).describedAs("%s saw kill attempts; this is not expected", i).isEqualTo(0);
            }
            catch (AssertionError t2)
            {
                if (t == null)
                    t = t2;
                else
                    t.addSuppressed(t2);
            }
        }
        if (t != null)
            throw t;
    }

    private static void triggerStreaming(Cluster cluster, boolean expectedEntireSSTable)
    {
        IInvokableInstance node1 = cluster.get(1);
        IInvokableInstance node2 = cluster.get(2);

        // repair will do streaming IFF there is a mismatch; so cause one
        for (int i = 0; i < 10; i++)
            node1.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), i); // timestamp won't match, causing a mismatch

        // trigger streaming; expected to fail as streaming socket closed in the middle (currently this is an unrecoverable event)
        node2.nodetoolResult("repair", "-full", KEYSPACE, "tbl").asserts().failure();

        assertStreamingType(node2, expectedEntireSSTable);
    }

    private static void assertStreamingType(IInvokableInstance node, boolean expectedEntireSSTable)
    {
        String key = "org.apache.cassandra.metrics.Streaming.%s./127.0.0.1.7012";
        long entire = node.metrics().getCounter(String.format(key, "EntireSSTablesStreamedIn"));
        long partial = node.metrics().getCounter(String.format(key, "PartialSSTablesStreamedIn"));
        if (expectedEntireSSTable)
        {
            Assertions.assertThat(partial).isEqualTo(0);
            Assertions.assertThat(entire).isGreaterThan(0);
        }
        else
        {
            Assertions.assertThat(partial).isGreaterThan(0);
            Assertions.assertThat(entire).isEqualTo(0);
        }
    }

    public static class BBHelper
    {
        @SuppressWarnings("unused")
        public static int writeDirectlyToChannel(ByteBuffer buf, @SuperCall Callable<Integer> zuper) throws Exception
        {
            if (isCaller(SSTableZeroCopyWriter.class.getName(), "write"))
                throw new java.nio.channels.ClosedChannelException();
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
            for (int i = 0; i < stack.length; i++)
            {
                StackTraceElement e = stack[i];
                if (klass.equals(e.getClassName()) && method.equals(e.getMethodName()))
                    return true;
            }
            return false;
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
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