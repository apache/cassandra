/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_READ_ITERATION_DELAY_MS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Checks how the aggregation timeout, the native transport timeout and the read (verb) timeout interact for aggregation
 * queries, which are internally executed as a sequence of sub-pages (see {@code AggregationQueryPager}).
 */
public class AggregationQueryPagerTimeoutTest extends TestBaseImpl
{
    private static final int AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS = 2000;
    private static final int AGGREGATION_READ_TIMEOUT_MILLIS = 1000;
    /**
     * Delay injected by {@code TEST_READ_ITERATION_DELAY_MS} for every partition and every row a replica reads.
     */
    private static final int AGGREGATION_ROW_DELAY_MILLIS = 50;

    /**
     * An aggregation is made of many sub-pages, and its total duration is bounded by aggregation timeout,
     * not by native transport timeout or the verb timeout, both of which are expected to apply to each
     * individual sub-page only.
     * <p>
     * Setup (see {@link #loadUniformRows}): 100 rows of about 500 bytes, read in 1KiB sub-pages of two rows each.
     * <ul>
     *   <li>Every sub-page takes at least {@code 100ms}, which is a fraction of the 1s read timeout.</li>
     *   <li>The aggregation takes at least {@code 100 * 50ms = 5s}, which is more than the native transport timeout and
     *       the read timeout. If these two timeouts applied to the aggregation as a whole, it could not succeed.</li>
     *   <li>The 60s aggregation timeout is far above that, so the aggregation is expected to succeed.</li>
     * </ul>
     */
    @Test
    public void aggregationOutlivesNativeTransportAndReadTimeouts() throws Throwable
    {
        AggregationResult result = runAggregation("60000ms", AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS, AGGREGATION_READ_TIMEOUT_MILLIS,
                                                  cluster -> loadUniformRows(cluster, 100));
        Assert.assertTrue("Aggregation was expected to succeed, but timed out", result.succeeded);
        Assert.assertTrue("Aggregation should have taken longer than native transport timeout, but took " + result.elapsedMillis + "ms",
                          result.elapsedMillis > AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS);
    }

    /**
     * Aggregation timeout still bounds the total duration of an aggregation.
     * <p>
     * Setup: the same data as above (100 rows, two rows per 1KiB sub-page, every sub-page taking at least 100ms), with
     * a 4s aggregation timeout, which is deliberately between the 2s native transport timeout and the time the
     * aggregation needs. The aggregation takes at least {@code 100 * 50ms = 5s}, which is more than the 4s aggregation timeout, so
     * it cannot complete in time and is guaranteed to time out.
     */
    @Test
    public void aggregationTimeoutIsStillEnforced() throws Throwable
    {
        long aggregationTimeoutMillis = 4000;
        Assert.assertTrue(aggregationTimeoutMillis > AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS);

        AggregationResult result = runAggregation(aggregationTimeoutMillis + "ms", AGGREGATION_NATIVE_TRANSPORT_TIMEOUT_MILLIS,
                                                  AGGREGATION_READ_TIMEOUT_MILLIS, cluster -> loadUniformRows(cluster, 100));

        Assert.assertFalse("Aggregation was expected to time out", result.succeeded);
        Assert.assertTrue("Timeout was not triggered by the aggregation timeout", result.aggregationTimeoutLogged);
    }

    /**
     * Verb timeout is still applied to every sub-page, the first few sub-pages are fast,
     * but a later one takes longer than the read timeout and has to fail the aggregation, even though neither the
     * aggregation timeout nor the native transport timeout is reached.
     * <p>
     * Setup: 1s read timeout, 60s native transport and aggregation timeouts.
     * <ul>
     *   <li>Sub-pages 1 to 5 have two large rows each and take at least 100ms.
     *       Each is a fraction of the read timeout and should succeed.</li>
     *   <li>Sub-page 6 has all 30 tiny rows and takes at least 1.5s, which is more than the
     *       1s read timeout, so it is guaranteed to exceed it.</li>
     *   <li>Neither of the other two timeouts (60s) can have fired, which the missing aggregation timeout log line
     *       confirms.</li>
     * </ul>
     * Run the same data with a 60s read timeout later, which shows that the failure is caused by the read timeout.
     */
    @Test
    public void readTimeoutIsStillEnforcedForLaterSubPages() throws Throwable
    {
        AggregationResult result = runAggregation("60000ms", 60000, AGGREGATION_READ_TIMEOUT_MILLIS, AggregationQueryPagerTimeoutTest::loadFastThenSlowSubPage);

        Assert.assertFalse("Aggregation was expected to time out, because one of its sub-pages exceeds the read timeout", result.succeeded);
        Assert.assertFalse("Timeout should not have been triggered by the aggregation timeout", result.aggregationTimeoutLogged);

        AggregationResult control = runAggregation("60000ms", 60000, 60000, AggregationQueryPagerTimeoutTest::loadFastThenSlowSubPage);
        Assert.assertTrue("Aggregation was expected to succeed with a larger read timeout", control.succeeded);
    }

    /**
     * The aggregation timeout is measured from the start of the request.
     * <p>
     * Setup: 30 rows of about 500 bytes in 1KiB sub-pages of two rows each, which the
     * aggregation reads in at least {@code 30 * 50ms = 1.5s} and in well under 3.5s. The aggregation timeout is 3.5s and
     * the native transport and read timeouts are 60s, so they cannot interfere.
     * <ul>
     *   <li>With a 3s delay injected before the select statement is executed,
     *       the aggregation still needs at least 1.5s, which together makes at least 4.5s since the start of
     *       the request, more than the aggregation timeout, so it is guaranteed to time out.</li>
     * </ul>
     */
    @Test
    public void aggregationTimeoutIsMeasuredFromRequestStart() throws Throwable
    {
        AggregationResult result = runAggregation("3500ms", 60000, 60000, 3000, cluster -> loadUniformRows(cluster, 30));
        Assert.assertFalse("Aggregation was expected to time out", result.succeeded);
        Assert.assertTrue("Timeout was not triggered by the aggregation timeout", result.aggregationTimeoutLogged);
    }

    private static class AggregationResult
    {
        final boolean succeeded;
        final long elapsedMillis;
        final boolean aggregationTimeoutLogged;

        AggregationResult(boolean succeeded, long elapsedMillis, boolean aggregationTimeoutLogged)
        {
            this.succeeded = succeeded;
            this.elapsedMillis = elapsedMillis;
            this.aggregationTimeoutLogged = aggregationTimeoutLogged;
        }
    }


    private interface AggregationData
    {
        int load(Cluster cluster);
    }

    /**
     * {@code numOfRows} rows of 500 bytes each
     */
    private static int loadUniformRows(Cluster cluster, int numOfRows)
    {
        for (int i = 0; i < numOfRows; i++)
            insertRow(cluster, i, String.join("", Collections.nCopies(500, "x")));
        return numOfRows;
    }

    /**
     * 10 large rows (500 bytes each), which make 5 sub-pages of two rows
     * followed by 30 tiny rows (1 byte each) that all fit in a single 6th sub-page,
     * which takes at least {@code 1.5s}.
     */
    private static int loadFastThenSlowSubPage(Cluster cluster)
    {
        for (int i = 0; i < 10; i++)
            insertRow(cluster, i, String.join("", Collections.nCopies(500, "x")));
        for (int i = 10; i < 40; i++)
            insertRow(cluster, i, "x");
        return 40;
    }

    private static void insertRow(Cluster cluster, int ck, String value)
    {
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), ConsistencyLevel.ONE, ck, value);
    }

    private AggregationResult runAggregation(String aggregationTimeout,
                                             int nativeTransportTimeoutMillis,
                                             int readTimeoutMillis,
                                             AggregationData data) throws Throwable
    {
        return runAggregation(aggregationTimeout, nativeTransportTimeoutMillis, readTimeoutMillis, 0, data);
    }

    private AggregationResult runAggregation(String aggregationTimeout,
                                             int nativeTransportTimeoutMillis,
                                             int readTimeoutMillis,
                                             long preExecutionDelayMillis,
                                             AggregationData data) throws Throwable
    {
        // Each row read on a replica takes at least AGGREGATION_ROW_DELAY_MILLIS.
        try (WithProperties ignored = new WithProperties().set(TEST_READ_ITERATION_DELAY_MS, AGGREGATION_ROW_DELAY_MILLIS);
             Cluster control = init(Cluster.build().withNodes(1)
                                           .withInstanceInitializer(PreExecutionDelay::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       .set("read_request_timeout", readTimeoutMillis + "ms")
                                                                       .set("range_request_timeout", readTimeoutMillis + "ms")
                                                                       .set("native_transport_timeout", nativeTransportTimeoutMillis + "ms")
                                                                       .set("aggregation_request_timeout", aggregationTimeout)
                                                                       .set("aggregation_subpage_size", "1KiB"))
                                           .start()))
        {
            control.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))"));
            int rows = data.load(control);
            control.get(1).runOnInstance(() -> PreExecutionDelay.delayMillis.set(preExecutionDelayMillis));

            // No retries, so that the elapsed time is the time it took for the first attempt to fail
            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                            .addContactPoint(control.get(1).broadcastAddress().getHostString())
                                                                                            .withSocketOptions(new SocketOptions().setReadTimeoutMillis(120000))
                                                                                            .withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(5000))
                                                                                            .withProtocolVersion(ProtocolVersion.V5)
                                                                                            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                                                                            .build();
                 Session session = cluster.connect(KEYSPACE))
            {
                long start = System.nanoTime();
                boolean succeeded;
                try
                {
                    Assert.assertEquals(rows, session.execute("SELECT count(*) FROM tbl WHERE pk = 1").one().getLong(0));
                    succeeded = true;
                }
                catch (ReadTimeoutException | ReadFailureException e)
                {
                    succeeded = false;
                }
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                // checkTimeout() in AggregationQueryPager logs this when it is the aggregation timeout that fired
                boolean aggregationTimeoutLogged = !control.get(1).logs().grep("Aggregation query timeout triggered").getResult().isEmpty();
                return new AggregationResult(succeeded, elapsedMillis, aggregationTimeoutLogged);
            }
        }
    }

    /**
     * Delays the execution of client select statements by {@link #delayMillis}. The delay happens after the request has
     * started and before the pager is created.
     */
    public static class PreExecutionDelay
    {
        static final AtomicLong delayMillis = new AtomicLong(0);

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(SelectStatement.class)
                           .method(named("execute").and(takesArguments(QueryState.class, QueryOptions.class, Dispatcher.RequestTime.class)))
                           .intercept(MethodDelegation.to(PreExecutionDelay.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static ResultMessage.Rows execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime, @SuperCall Callable<ResultMessage.Rows> r) throws Exception
        {
            long delay = delayMillis.get();
            if (delay > 0 && !state.getClientState().isInternal)
                Thread.sleep(delay);
            return r.call();
        }
    }
}
