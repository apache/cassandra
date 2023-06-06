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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableZeroCopyWriter;
import org.apache.cassandra.io.util.SequentialWriter;
import org.assertj.core.api.Assertions;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class AbstractStreamFailureLogs extends TestBaseImpl
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractStreamFailureLogs.class);

    protected static final int FAILING_NODE = 2;

    protected void streamTest(boolean zeroCopyStreaming, String reason, Integer failedNode) throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withInstanceInitializer(BBStreamHelper::install)
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set("stream_entire_sstables", zeroCopyStreaming)
                                                        // when die, this will try to halt JVM, which is easier to validate in the test
                                                        // other levels require checking state of the subsystems
                                                        .set("disk_failure_policy", "die"))
                                      .start())
        {
            init(cluster);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY)"));

            triggerStreaming(cluster);
            // make sure disk failure policy is not triggered

            IInvokableInstance failingNode = cluster.get(failedNode);

            searchForLog(failingNode, reason);
        }
    }

    protected void triggerStreaming(Cluster cluster)
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

    protected void searchForLog(IInvokableInstance failingNode, String reason)
    {
        searchForLog(failingNode, true, reason);
    }

    protected boolean searchForLog(IInvokableInstance failingNode, boolean failIfNoMatch, String reason)
    {
        LogResult<List<String>> result = failingNode.logs().grepForErrors(-1, Pattern.compile("Stream failed:"));
        // grepForErrors will include all ERROR logs even if they don't match the pattern; for this reason need to filter after the fact
        List<String> matches = result.getResult();

        matches = matches.stream().filter(s -> s.startsWith("WARN")).collect(Collectors.toList());
        logger.info("Stream failed logs found: {}", String.join("\n", matches));
        if (matches.isEmpty() && !failIfNoMatch)
            return false;

        Assertions.assertThat(matches)
                  .describedAs("node%d expected to find %s but could not", failingNode.config().num(), reason)
                  .hasSize(1);
        String logLine = matches.get(0);
        Assertions.assertThat(logLine).contains(reason);

        Matcher match = Pattern.compile(".*\\[Stream #(.*)\\]").matcher(logLine);
        if (!match.find()) throw new AssertionError("Unable to parse: " + logLine);
        UUID planId = UUID.fromString(match.group(1));
        SimpleQueryResult qr = failingNode.executeInternalWithResult("SELECT * FROM system_views.streaming WHERE id=?", planId);
        Assertions.assertThat(qr.hasNext()).isTrue();
        Assertions.assertThat(qr.next().getString("failure_cause")).contains(reason);
        return true;
    }

    public static class BBStreamHelper
    {
        @SuppressWarnings("unused")
        public static int writeDirectlyToChannel(ByteBuffer buf, @SuperCall Callable<Integer> zuper) throws Exception
        {
            if (isCaller(SSTableZeroCopyWriter.class.getName(), "write"))
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

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != FAILING_NODE)
                return;
            new ByteBuddy().rebase(SequentialWriter.class)
                           .method(named("writeDirectlyToChannel").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBStreamHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(RangeAwareSSTableWriter.class)
                           .method(named("append").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBStreamHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

        }
    }

    protected static boolean isCaller(String klass, String method)
    {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        for (int i = 0; i < stack.length; i++)
        {
            StackTraceElement e = stack[i];
            if (klass.equals(e.getClassName()) && method.equals(e.getMethodName()))
                return true;
        }
        return false;
    }
}
