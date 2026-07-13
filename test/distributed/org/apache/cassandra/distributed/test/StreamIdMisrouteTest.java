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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Envelope;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.config.DatabaseDescriptor.clientInitialization;
import static org.apache.cassandra.config.DatabaseDescriptor.getNativeTransportPort;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Regression test for CASSANDRA-21508. A coordinator that load-sheds a request which waited past its
 * native-transport queue deadline must stamp the OVERLOADED error with the timed-out request's own
 * stream id. Before the fix the error went out on stream id 0, which misroutes it to an unrelated
 * in-flight request and can escalate into the client-side "column-shift" read corruption (a value
 * from one table decoded against another query's column definitions under the v5 skip-metadata
 * optimization).
 *
 * <p>The original defect: {@code Dispatcher.processRequest} load-sheds a request that has waited in the
 * Native-Transport-Requests queue longer than {@code native_transport_timeout} by returning
 *
 * <pre>
 *   ErrorMessage.fromException(new OverloadedException("Query timed out before it could start"))
 * </pre>
 * <p>
 * without calling {@code setStreamId(request.getStreamId())}. {@code ErrorMessage.streamId} therefore
 * kept its default of 0, and the error frame was written to the client on stream id 0 instead of the
 * timed-out request's real stream id. The fix routes every response through a central stamping step
 * and makes {@code ErrorMessage.fromException} require the stream id at the call site.
 *
 * <p>{@link #loadShedErrorIsStampedWithRequestStreamId()} proves the fix on the wire: a request sent on
 * a non-zero stream id is load-shed and the OVERLOADED error comes back on that same stream id (not 0).
 * Against the unpatched server this assertion fails with the error arriving on stream id 0.
 *
 * <p>Why the stream id matters: on a busy connection stream id 0 is almost always in use, so an error
 * mis-stamped with 0 is applied to an unrelated in-flight request. The client then frees and reuses
 * stream id 0 for a new query, and when the original query's real rows arrive on it they are decoded
 * positionally against the reusing query's cached column definitions (skip-metadata carries no column
 * info) - shifting each value into the wrong column and either throwing in a codec or silently
 * returning a plausible-but-wrong value.
 */
public class StreamIdMisrouteTest extends TestBaseImpl
{
    private static final int TIMED_OUT_STREAM_ID = 42; // any non-zero id; before the fix the reply was forced to 0

    @BeforeClass
    public static void initClientSide()
    {
        // SimpleClient runs in the test classloader; it needs DatabaseDescriptor initialized here to
        // build the native-protocol pipeline. Without this, connect() fails with a ClosedChannelException.
        clientInitialization();
    }

    /**
     * Proves the fix directly: under native-transport queue backlog, the load-shed OVERLOADED error is
     * returned on the stream id of the request that actually timed out, not on stream id 0. Against the
     * unpatched server this fails with the error arriving on stream id 0.
     */
    @Test
    public void loadShedErrorIsStampedWithRequestStreamId() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build().withNodes(1)
                                           .withInstanceInitializer(SlowSelect::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       // one NTR thread so a slow query blocks the queue head
                                                                       .set("native_transport_max_threads", 1)
                                                                       // short deadline so a queued request is shed quickly
                                                                       .set("native_transport_timeout", "150ms")
                                                                       .set("read_request_timeout", "500ms")
                                                                       .set("range_request_timeout", "500ms"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));

            InetSocketHost host = hostAndPort(cluster);
            ExecutorService executor = Executors.newCachedThreadPool();

            // Two independent connections. A SimpleClient's response queue is a SynchronousQueue, so
            // concurrent execute() calls on ONE client would race; we use one client to build the
            // backlog and a second, dedicated client for the request whose stream id we assert on.
            try (SimpleClient filler = SimpleClient.builder(host.address, host.port)
                                                   .protocolVersion(ProtocolVersion.V5).build().connect(false);
                 SimpleClient victim = SimpleClient.builder(host.address, host.port)
                                                   .protocolVersion(ProtocolVersion.V5).build().connect(false))
            {
                // Warm up both connections while the server is still fast.
                filler.execute(query(withKeyspace("SELECT * FROM %s.tbl"), 1));
                victim.execute(query(withKeyspace("SELECT * FROM %s.tbl"), 1));

                // From here every non-internal SELECT sleeps 1s, far past the 150ms queue deadline.
                cluster.get(1).runOnInstance(() -> Assert.assertTrue(SlowSelect.enabled.compareAndSet(false, true)));

                // Saturate the single NTR worker and pile several requests behind it, so anything
                // enqueued now waits well beyond native_transport_timeout before a worker frees up.
                List<Future<?>> backlog = new ArrayList<>();
                for (int i = 0; i < 8; i++)
                {
                    int streamId = 10 + i;
                    backlog.add(executor.submit(() ->
                                                filler.execute(query(withKeyspace("SELECT * FROM %s.tbl"), streamId), false)));
                }

                // Let the backlog form and the queue-time clock run past the 150ms deadline.
                TimeUnit.MILLISECONDS.sleep(800);

                // This request enqueues behind a queue that is already older than the deadline, so the
                // worker load-sheds it immediately when it is dequeued. It goes out on stream id 42.
                Message.Response shed =
                victim.execute(query(withKeyspace("SELECT * FROM %s.tbl"), TIMED_OUT_STREAM_ID), false);

                Assert.assertTrue("Expected an OVERLOADED error, got: " + shed,
                                  shed instanceof ErrorMessage
                                  && ((ErrorMessage) shed).error instanceof OverloadedException);

                // The fix: the request went out on stream id 42, and the load-shed error now comes back
                // on stream id 42 too because Dispatcher stamps every response with the request's id.
                Assert.assertEquals("Load-shed OVERLOADED error should carry the request's own stream id (" +
                                    TIMED_OUT_STREAM_ID + ')',
                                    TIMED_OUT_STREAM_ID, shed.getSource().header.streamId);

                // Drain the backlog so the connection can close cleanly.
                cluster.get(1).runOnInstance(() -> SlowSelect.enabled.set(false));
                for (Future<?> f : backlog)
                {
                    try
                    {
                        f.get(30, TimeUnit.SECONDS);
                    }
                    catch (Exception ignored)
                    { /* shed/timed out */ }
                }
            }
            finally
            {
                cluster.get(1).runOnInstance(() -> SlowSelect.enabled.set(false));
                executor.shutdownNow();
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    private static QueryMessage query(String cql, int streamId)
    {
        QueryMessage msg = new QueryMessage(cql, QueryOptions.forInternalCalls(
        org.apache.cassandra.db.ConsistencyLevel.ONE, Collections.emptyList()));
        msg.setSource(new Envelope(Envelope.Header.dummy(streamId, Message.Type.QUERY), null));
        return msg;
    }

    private static InetSocketHost hostAndPort(Cluster cluster)
    {
        // Node 1's native transport binds on its broadcast address; the port is whatever the in-JVM
        // provisioning assigned (9042 for the default multi-interface strategy). Derive both from the
        // instance config so this works regardless of provisioning strategy.
        String address = cluster.get(1).config().broadcastAddress().getAddress().getHostAddress();
        int port = cluster.get(1).callOnInstance(
        () -> getNativeTransportPort());
        return new InetSocketHost(address, port);
    }

    private static final class InetSocketHost
    {
        final String address;
        final int port;

        InetSocketHost(String address, int port)
        {
            this.address = address;
            this.port = port;
        }
    }

    /**
     * ByteBuddy interceptor that makes client-issued SELECTs sleep past the queue deadline, so a
     * request queued behind one is load-shed by Dispatcher. Mirrors OverloadTest.SlowSelect.
     */
    public static class SlowSelect
    {
        static final AtomicBoolean enabled = new AtomicBoolean(false);

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(SelectStatement.class)
                           .method(named("execute").and(takesArguments(QueryState.class, QueryOptions.class, Dispatcher.RequestTime.class)))
                           .intercept(MethodDelegation.to(SlowSelect.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static ResultMessage.Rows execute(QueryState state, QueryOptions options,
                                                 Dispatcher.RequestTime requestTime,
                                                 @SuperCall Callable<ResultMessage.Rows> zuper) throws Exception
        {
            if (enabled.get() && !state.getClientState().isInternal)
                TimeUnit.SECONDS.sleep(1);
            return zuper.call();
        }
    }
}
