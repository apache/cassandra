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

package org.apache.cassandra.distributed.test.tcm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Concurrent CQL DDL submitted to a CMS member fails with a SERVER_ERROR even though the request
 * deadline has seconds left, because the retry <i>attempt</i> cap is reached first.
 *
 * <p>Failure message observed against a real single-node 6.0 build:
 * <pre>Could not perform commit after 12 attempts. Time remaining: 4231ms</pre>
 *
 * <p>Expected: a schema change either succeeds or fails only once its deadline
 * ({@code request_timeout}, 10s by default) is actually exhausted.
 * Actual: it fails after 12 attempts with several seconds of deadline still remaining, so the
 * configured timeout budget is silently not honoured for DDL.
 *
 * <p>Mechanism:
 * <ul>
 *   <li>{@code ClusterMetadataService.getRetryPolicy()} (src/java/org/apache/cassandra/tcm/ClusterMetadataService.java:727-731)
 *       gives {@code Transformation.Kind.SCHEMA_CHANGE} a deadline of {@code request_timeout} but leaves the wait
 *       strategy as the shared {@code Retry.DEFAULT_STRATEGY}, built from {@code cms_retry_delay}
 *       (src/java/org/apache/cassandra/config/Config.java:205, {@code retries=10}); with
 *       src/java/org/apache/cassandra/service/RetryStrategy.java:215 that is {@code maxAttempts = 11}, past which
 *       {@code computeWait} returns -1.</li>
 *   <li>Every concurrent commit reads the same {@code log.waitForHighestConsecutive()} epoch and CASes at epoch+1
 *       (src/java/org/apache/cassandra/tcm/AbstractLocalProcessor.java:62,115), so all but one loser per round
 *       burns an attempt, hits {@code maybeSleep()} returning false at :134 and breaks out of the loop, and the
 *       SERVER_ERROR is built at :156-161 while {@code retryPolicy.remainingNanos()} is still positive.</li>
 * </ul>
 *
 * <p>Reproduction is load- and hardware-sensitive: the losers only pile up faster than 11 attempts can absorb
 * when enough DDL is in flight at once. Counts below are of the concurrently submitted {@code CREATE TABLE}
 * statements only ({@code WORKERS * TABLES_PER_WORKER}); the per-worker keyspaces are created serially up front and
 * are not included. Measured against a real single-node build in a 4 CPU / 4 GiB Docker container (JDK 21, one CMS
 * member, RF=1, default config): N=1/2/4 passed, N=8 failed 3 of 144, N=22 failed 10 of 396, N=48 failed 34 of 864,
 * every failure reporting exactly 12 attempts with 3.3-5.7 s of deadline remaining. This in-JVM form was then swept
 * on a 16-core host (JDK 21): N=2 and N=4 passed, N=8 failed 1 of 144 on three consecutive runs, and N=48 failed
 * 311-332 of 864 - again always at exactly 12 attempts, with 3.4-5.4 s remaining. Onset concurrency rises with core
 * count, so {@link #WORKERS} defaults to the highest measured level and is overridable with
 * {@code -Dcassandra.test.concurrent_schema_commit.workers=N}.
 *
 * <p><b>Note on the oracle.</b> This test asserts that <i>every</i> submitted statement succeeds, which is a stronger
 * requirement than "no commit stops before its deadline". Removing only the attempt cap does not make it pass: the
 * losers then consume their whole deadline instead, and statements still fail under enough concurrency. That is
 * deliberate for now — the failure report distinguishes the two stop reasons — but if the agreed scope of the fix is
 * only the attempt-cap incoherence, this assertion should be narrowed to that before the test is committed. As it
 * stands it also measures the underlying epoch CAS contention, which is a separate concern.
 */
public class ConcurrentSchemaCommitTest extends TestBaseImpl
{
    /** Concurrent DDL submitters. Default is the highest level from the measured sweep described above. */
    private static final int DEFAULT_WORKERS = 48;

    /** Tables each worker creates in its own (pre-created) keyspace. */
    private static final int TABLES_PER_WORKER = 18;

    private static final String WORKERS_PROPERTY = "cassandra.test.concurrent_schema_commit.workers";

    private static final int WORKERS = workerCount();

    /** Matches the retry-exhaustion failure text built in AbstractLocalProcessor.commit(). */
    private static final Pattern ATTEMPT_CAP = Pattern.compile("Could not perform commit after (\\d+) attempts\\. Time remaining: (-?\\d+)ms");

    /**
     * Matches a local admission timeout by a request which never reached {@code tryCommitOne}. Distinct from
     * {@link #ATTEMPT_CAP} because it says nothing about the state of the distributed log, and retrying it is
     * unambiguously safe.
     */
    private static final Pattern NEVER_PROPOSED = Pattern.compile("Timed out after (\\d+)ms waiting to submit commit\\. No proposal was made");

    /**
     * Matches a local admission timeout by a request which had already proposed on an earlier iteration, so whether
     * the transformation was committed is unknown. Also an admission timeout, but it must not be reported as
     * {@link #NEVER_PROPOSED}: retrying it is not known to be safe.
     */
    private static final Pattern OUTCOME_UNKNOWN = Pattern.compile("Timed out after (\\d+)ms waiting to submit commit\\. " +
                                                                   "A proposal was made during one of the preceding (\\d+) attempts");

    /**
     * Submits {@code WORKERS * TABLES_PER_WORKER} schema changes concurrently against the single (CMS member) node
     * and asserts that every single one of them succeeded. Unlike
     * {@code DistributedLogTest.testConcurrentCommit()}, no commit exception is swallowed: each statement is
     * accounted for, and every failure is reported with its attempt count and remaining deadline, separated into
     * those which stopped early and those which used their whole budget.
     */
    @Test
    public void concurrentSchemaChangesShouldNotExhaustAttemptsBeforeDeadline() throws Throwable
    {
        // Single node is a CMS member by default, so DDL commits take the local (AbstractLocalProcessor) path.
        // Retry and timeout settings are deliberately left at their defaults, since the attempt limit under test comes
        // from the default cms_retry_delay. Note that raising cms_default_max_retries would NOT change this: while
        // cms_retry_delay is non-null it supplies the whole strategy, and cms_default_max_retries is not consulted.
        try (Cluster cluster = Cluster.build(1).start())
        {
            // Keyspaces are created serially and up front so that every concurrently submitted statement below is
            // independent, and a failure of one cannot cascade into "keyspace doesn't exist" failures of others.
            for (int w = 0; w < WORKERS; w++)
            {
                cluster.schemaChange("CREATE KEYSPACE " + keyspace(w) +
                                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            }

            List<String> failures = new CopyOnWriteArrayList<>();
            AtomicInteger succeeded = new AtomicInteger();
            CountDownLatch startBarrier = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>(WORKERS);
            ExecutorService executor = Executors.newFixedThreadPool(WORKERS);
            try
            {
                for (int w = 0; w < WORKERS; w++)
                {
                    final int worker = w;
                    futures.add(executor.submit(() -> {
                        startBarrier.await();
                        for (int t = 0; t < TABLES_PER_WORKER; t++)
                        {
                            submit(cluster, failures, succeeded,
                                   "CREATE TABLE " + keyspace(worker) + ".tbl_" + t + " (k int PRIMARY KEY, v text)");
                        }
                        return null;
                    }));
                }

                startBarrier.countDown();

                // Collect every future so that no worker failure can be lost.
                for (Future<?> future : futures)
                    future.get(10, TimeUnit.MINUTES);
            }
            finally
            {
                executor.shutdownNow();
            }

            int submitted = WORKERS * TABLES_PER_WORKER;
            if (!failures.isEmpty())
                fail(report(submitted, succeeded.get(), failures));
            assertEquals("Every submitted schema change must be accounted for", submitted, succeeded.get());
        }
    }

    private static String keyspace(int worker)
    {
        return "concurrent_ddl_ks_" + worker;
    }

    private static void submit(Cluster cluster, List<String> failures, AtomicInteger succeeded, String statement)
    {
        try
        {
            cluster.coordinator(1).execute(statement, ConsistencyLevel.ONE);
            succeeded.incrementAndGet();
        }
        catch (Throwable t)
        {
            // Exceptions come from the instance class loader, so they cannot be caught by type here; match on text.
            failures.add(statement + " => " + describe(t));
        }
    }

    private static String describe(Throwable t)
    {
        StringBuilder sb = new StringBuilder();
        Throwable cause = t;
        while (cause != null)
        {
            if (sb.length() > 0)
                sb.append(" <- ");
            sb.append(cause.getClass().getName()).append(": ").append(cause.getMessage());
            if (cause.getCause() == cause)
                break;
            cause = cause.getCause();
        }
        return sb.toString();
    }

    private static String report(int submitted, int succeeded, List<String> failures)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(failures.size()).append(" of ").append(submitted)
          .append(" concurrent schema changes failed at concurrency ").append(WORKERS)
          .append(" (").append(succeeded).append(" succeeded).");

        // The commit failure message is the same whether the retry policy stopped because it ran out of attempts or
        // because the deadline expired, so the two are distinguished by the remaining time it reports. Only a failure
        // with time still remaining demonstrates the defect this test is about; one reporting no remaining time simply
        // used its whole budget, which is a contention symptom rather than an early giveup.
        int gaveUpEarly = 0;
        int deadlineExpired = 0;
        int neverProposed = 0;
        int outcomeUnknown = 0;
        for (String failure : failures)
        {
            // Both of the following are admission timeouts, but they differ in whether the request had already
            // proposed, and therefore in whether retrying it is safe. Check the stronger claim first.
            Matcher unknown = OUTCOME_UNKNOWN.matcher(failure);
            if (unknown.find())
            {
                outcomeUnknown++;
                sb.append("\n  outcome unknown (local admission timeout after ").append(unknown.group(1))
                  .append("ms, after proposing within ").append(unknown.group(2)).append(" attempts)");
                continue;
            }

            Matcher never = NEVER_PROPOSED.matcher(failure);
            if (never.find())
            {
                neverProposed++;
                sb.append("\n  never proposed (local admission timeout after ").append(never.group(1)).append("ms)");
                continue;
            }

            Matcher matcher = ATTEMPT_CAP.matcher(failure);
            if (matcher.find())
            {
                long remainingMillis = Long.parseLong(matcher.group(2));
                if (remainingMillis > 0)
                {
                    gaveUpEarly++;
                    sb.append("\n  gave up with deadline remaining: attempts=").append(matcher.group(1))
                      .append(", remaining=").append(remainingMillis).append("ms");
                }
                else
                {
                    deadlineExpired++;
                    sb.append("\n  deadline exhausted: attempts=").append(matcher.group(1))
                      .append(", remaining=").append(remainingMillis).append("ms");
                }
            }
            else
            {
                sb.append("\n  other failure: ").append(failure);
            }
        }

        if (gaveUpEarly > 0)
        {
            sb.append("\n\n").append(gaveUpEarly)
              .append(" failure(s) stopped on the retry attempt cap while the request deadline was still unexpired.")
              .append(" See ClusterMetadataService.getRetryPolicy() and AbstractLocalProcessor.commit().");
        }
        if (deadlineExpired > 0)
        {
            sb.append("\n\n").append(deadlineExpired)
              .append(" failure(s) consumed the entire request deadline while retrying. These do not demonstrate an")
              .append(" early giveup; they indicate the epoch CAS contention in AbstractLocalProcessor.commit().");
        }
        if (neverProposed > 0)
        {
            sb.append("\n\n").append(neverProposed)
              .append(" failure(s) never proposed at all, having timed out waiting for local admission. That is")
              .append(" overload of this node's serialised proposal path rather than epoch CAS contention: the")
              .append(" offered DDL rate exceeds what one CMS member can commit within the request deadline.")
              .append(" Retrying these is unambiguously safe.");
        }
        if (outcomeUnknown > 0)
        {
            sb.append("\n\n").append(outcomeUnknown)
              .append(" failure(s) timed out waiting for local admission after having already proposed on an earlier")
              .append(" attempt, so whether the transformation was committed to the distributed log is unknown.")
              .append(" Unlike the above, these are not known to be safe to retry.");
        }
        return sb.toString();
    }

    private static int workerCount()
    {
        String configured = System.getProperty(WORKERS_PROPERTY); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        return configured == null || configured.isEmpty() ? DEFAULT_WORKERS : Integer.parseInt(configured);
    }
}
