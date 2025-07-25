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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProtocolModifiers;
import accord.primitives.TxnId.FastPath;
import accord.primitives.TxnId.FastPaths;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.virtual.AccordDebugKeyspace;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.metrics.AccordMetrics;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.RatioGaugeSet;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.exceptions.AccordReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.AccordWritePreemptedException;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.AssertionUtils;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;


public class AccordMetricsTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMetricsTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(Function.identity(), 2);
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> {
            AccordService.instance().setCacheSize(0);
            ProtocolModifiers.Toggles.setPermittedFastPaths(new FastPaths(FastPath.Unoptimised));
        }));
        for (int i = 0; i < SHARED_CLUSTER.size(); i++) // initialize metrics
            logger.trace(SHARED_CLUSTER.get(i + 1).callOnInstance(() -> AccordMetrics.readMetrics.toString() + AccordMetrics.writeMetrics.toString()));
    }

    String writeCql()
    {
        return "BEGIN TRANSACTION\n" +
               "  LET val = (SELECT v FROM " + qualifiedAccordTableName + " WHERE k=? AND c=?);\n" +
               "  SELECT val.v;\n" +
               "  UPDATE " + qualifiedAccordTableName + " SET v = v + 1 WHERE k=? AND c=?;\n" +
               "COMMIT TRANSACTION";
    }

    String readCql()
    {
        return "BEGIN TRANSACTION\n" +
               "  LET val1 = (SELECT v FROM " + qualifiedAccordTableName + " WHERE k=? AND c=?);\n" +
               "  LET val2 = (SELECT v FROM " + qualifiedAccordTableName + " WHERE k=? AND c=?);\n" +
               "  SELECT val1.v, val2.v;\n" +
               "COMMIT TRANSACTION";
    }

    Map<Integer, Map<String, Long>> countingMetrics0;

    @Before
    public void beforeTest()
    {
        SHARED_CLUSTER.filters().reset();
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH " + TransactionalMode.full.asCqlParam());
    }

    @Test
    public void testRegularMetrics()
    {
        countingMetrics0 = getMetrics();
        assertCoordinatorMetrics(0, "rw", 0, 0, 0, 0, 0);
        SHARED_CLUSTER.coordinator(1).executeWithResult(writeCql(), ConsistencyLevel.ALL, 0, 0, 0, 0);
        assertClientMetrics(0, "AccordWrite", 0, 0);
        assertClientMetrics(1, "AccordWrite", 0, 0);
        assertCoordinatorMetrics(0, "rw", 1, 0, 0, 0, 0);
        assertCoordinatorMetrics(1, "rw", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "rw", 1, 1, 1);
        assertReplicaMetrics(1, "rw", 1, 1, 1);
        assertZeroMetrics("ro", "AccordRead");

        countingMetrics0 = getMetrics();
        SHARED_CLUSTER.coordinator(1).executeWithResult(readCql(), ConsistencyLevel.ALL, 0, 0, 1, 1);
        assertClientMetrics(0, "AccordRead", 0, 0);
        assertClientMetrics(1, "AccordRead", 0, 0);
        assertCoordinatorMetrics(0, "ro", 1, 0, 0, 0, 0);
        assertCoordinatorMetrics(1, "ro", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "ro", 0, 1, 1);
        assertReplicaMetrics(1, "ro", 0, 1, 1);
        assertZeroMetrics("rw", "AccordWrite");
    }

    @Test
    public void testPreemptionMetrics()
    {
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        IMessageFilters.Matcher delay = (from, to, m) -> {
            exec.schedule(() -> {
                System.err.println("Receiving...");
                SHARED_CLUSTER.get(to).receiveMessageWithInvokingThread(m);
            }, 10L, TimeUnit.SECONDS);
            return true;
        };
        IMessageFilters.Filter preacceptDelay = SHARED_CLUSTER.filters().outbound().verbs(Verb.ACCORD_PRE_ACCEPT_REQ.id).from(1).to(1)
                                                            .messagesMatching(delay)
                                                            .drop();

        String originalAccordRetryTxnDelay = SHARED_CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getAccordRecoverTxnDelay);
        SHARED_CLUSTER.forEach(() -> DatabaseDescriptor.setAccordRecoverTxnDelay("100ms"));
        String originalAccordExpireTxnDelay = SHARED_CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getAccordExpireTxnDelay);
        SHARED_CLUSTER.forEach(() -> DatabaseDescriptor.setAccordExpireTxnDelay("12s"));
        long originalWriteRpcTimeoutMillis = SHARED_CLUSTER.get(1).callOnInstance(() -> DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS));
        SHARED_CLUSTER.forEach(() -> DatabaseDescriptor.setWriteRpcTimeout(12_000));
        SHARED_CLUSTER.forEach(() -> DatabaseDescriptor.setReadRpcTimeout(12_000));

        try
        {
            countingMetrics0 = getMetrics();
            try
            {
                SHARED_CLUSTER.coordinator(1).executeWithResult(writeCql(), ConsistencyLevel.ALL, 0, 0, 0, 0);
                fail("expected to fail");
            }
            catch (RuntimeException ex)
            {
                Assertions.assertThat(ex).is(AssertionUtils.rootCauseIs(AccordWritePreemptedException.class));
            }

            assertClientMetrics(0, "AccordWrite", 1, 0);
            assertClientMetrics(1, "AccordWrite", 0, 0);
            // TODO (required): reenable or remove metric
//            assertCoordinatorMetrics(0, "rw", 0, 0, 1, 0, 0);
            assertCoordinatorMetrics(1, "rw", 0, 0, 0, 0, 0);
            assertReplicaMetrics(0, "rw", 0, 0, 0);
            assertReplicaMetrics(1, "rw", 0, 0, 0);

            assertZeroMetrics("ro", "AccordRead");

            countingMetrics0 = getMetrics();
            try
            {
                SHARED_CLUSTER.coordinator(1).executeWithResult(readCql(), ConsistencyLevel.ALL, 0, 0, 1, 1);
                fail("expected to fail");
            }
            catch (RuntimeException ex)
            {
                Assertions.assertThat(ex).is(AssertionUtils.rootCauseIs(AccordReadPreemptedException.class));
            }

            assertClientMetrics(0, "AccordRead", 1, 0);
            assertClientMetrics(1, "AccordRead", 0, 0);
            // TODO (required): reenable or remove metric
//            assertCoordinatorMetrics(0, "ro", 0, 0, 1, 0, 0);
            assertCoordinatorMetrics(1, "ro", 0, 0, 0, 0, 0);
            assertReplicaMetrics(0, "ro", 0, 0, 0);
            assertReplicaMetrics(1, "ro", 0, 0, 0);

            assertZeroMetrics("rw", "AccordWrite");
        }
        finally
        {
            SHARED_CLUSTER.forEach(() -> DatabaseDescriptor.setAccordExpireTxnDelay(originalAccordExpireTxnDelay));
            SHARED_CLUSTER.forEach(() -> DatabaseDescriptor.setAccordRecoverTxnDelay(originalAccordRetryTxnDelay));
            SHARED_CLUSTER.forEach(() -> DatabaseDescriptor.setWriteRpcTimeout(originalWriteRpcTimeoutMillis));
            preacceptDelay.off();
            exec.shutdown();
        }
    }

    @Test
    public void testTimeoutMetrics()
    {
        IMessageFilters.Filter preAcceptFilter = SHARED_CLUSTER.filters().outbound().verbs(Verb.ACCORD_PRE_ACCEPT_REQ.id).from(1).to(2).drop();
        preAcceptFilter.on();

        countingMetrics0 = getMetrics();
        try
        {
            SHARED_CLUSTER.coordinator(1).executeWithResult(readCql(), ConsistencyLevel.ALL, 0, 0, 1, 1);
            fail("expected to fail");
        }
        catch (RuntimeException ex)
        {
            Assertions.assertThat(ex).is(AssertionUtils.rootCauseIs(ReadTimeoutException.class));
        }

        assertClientMetrics(0, "AccordRead", 0, 1);
        assertClientMetrics(1, "AccordRead", 0, 0);
        // TODO (required): reenable or remove metric
//        assertCoordinatorMetrics(0, "ro", 0, 0, 0, 1, 0);
        assertCoordinatorMetrics(1, "ro", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "ro", 0, 0, 0);
        assertReplicaMetrics(1, "ro", 0, 0, 0);

        assertZeroMetrics("rw", "AccordWrite");

        countingMetrics0 = getMetrics();
        try
        {
            SHARED_CLUSTER.coordinator(1).executeWithResult(writeCql(), ConsistencyLevel.ALL, 0, 0, 0, 0);
            fail("expected to fail");
        }
        catch (RuntimeException ex)
        {
            Assertions.assertThat(ex).is(AssertionUtils.rootCauseIs(WriteTimeoutException.class));
        }

        assertClientMetrics(0, "AccordWrite", 0, 1);
        assertClientMetrics(1, "AccordWrite", 0, 0);
        // TODO (required): reenable or remove metric
//        assertCoordinatorMetrics(0, "rw", 0, 0, 0, 1, 0);
        assertCoordinatorMetrics(1, "rw", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "rw", 0, 0, 0);
        assertReplicaMetrics(1, "rw", 0, 0, 0);

        assertZeroMetrics("ro","AccordRead");
    }

    private void assertZeroMetrics(String scope, String clientScope)
    {
        for (int i = 0; i < SHARED_CLUSTER.size(); i++)
        {
            assertClientMetrics(0, clientScope, 0, 0);
            assertCoordinatorMetrics(i, scope, 0, 0, 0, 0, 0);
            assertReplicaMetrics(i, scope, 0, 0, 0);
        }
    }

    private void assertClientMetrics(int node, String scope, long preempts, long timeouts)
    {
        DefaultNameFactory nameFactory = new DefaultNameFactory("ClientRequest", scope);
        Map<String, Long> metrics = diff(countingMetrics0).get(node);
        logger.info("Metrics for node {} / {}: {}", node, scope, metrics);
        Function<String, Long> metric = n -> metrics.get(nameFactory.createMetricName(n).getMetricName());
        assertThat(metric.apply("Preempted")).isEqualTo(preempts);
        assertThat(metric.apply("Timeouts")).isEqualTo(timeouts);

        // Verify that coordinator metrics are published to the appropriate virtual table:
//        SimpleQueryResult res = SHARED_CLUSTER.get(node + 1)
//                                              .executeInternalWithResult("SELECT * FROM system_metrics.accord_coordinator_group WHERE scope = ?", scope);
//        while (res.hasNext())
//        {
//            Row metricRow = res.next();
//            String name = metricRow.getString("name");
//            assertThat(metrics).containsKey(name);
//        }
    }

    private void assertCoordinatorMetrics(int node, String scope, long fastPaths, long slowPaths, long preempts, long timeouts, long recoveries)
    {
        DefaultNameFactory nameFactory = new DefaultNameFactory(AccordMetrics.ACCORD_COORDINATOR, scope);
        Map<String, Long> metrics = diff(countingMetrics0).get(node);
        logger.info("Metrics for node {} / {}: {}", node, scope, metrics);
        Function<String, Long> metric = n -> metrics.get(nameFactory.createMetricName(n).getMetricName());
        assertThat(metric.apply(AccordMetrics.FAST_PATHS)).isEqualTo(fastPaths);
        assertThat(metric.apply(AccordMetrics.SLOW_PATHS)).isEqualTo(slowPaths);
        assertThat(metric.apply(AccordMetrics.PREEMPTED)).isEqualTo(preempts);
        assertThat(metric.apply(AccordMetrics.TIMEOUTS)).isEqualTo(timeouts);
        assertThat(metric.apply(AccordMetrics.RECOVERY_DELAY)).isEqualTo(recoveries);
        assertThat(metric.apply(AccordMetrics.RECOVERY_TIME)).isEqualTo(recoveries);
        assertThat(metric.apply(AccordMetrics.COORDINATOR_DEPENDENCIES)).isEqualTo(fastPaths + slowPaths);

        // Verify that coordinator metrics are published to the appropriate virtual table:
        SimpleQueryResult res = SHARED_CLUSTER.get(node + 1)
                                              .executeInternalWithResult("SELECT * FROM system_metrics.accord_coordinator_group WHERE scope = ?", scope);
        while (res.hasNext())
        {
            Row metricRow = res.next();
            String name = metricRow.getString("name");
            assertThat(metrics).containsKey(name);
        }

        if ((fastPaths + slowPaths) > 0)
        {
            String fastPathToTotalName = nameFactory.createMetricName(AccordMetrics.FAST_PATH_TO_TOTAL + "." + RatioGaugeSet.MEAN_RATIO).getMetricName();
            assertThat((double) SHARED_CLUSTER.get(1).metrics().getGauge(fastPathToTotalName)).isEqualTo((double) fastPaths / (double) (fastPaths + slowPaths), Offset.offset(0.01d));
        }
    }

    private void assertReplicaMetrics(int node, String scope, long stable, long executions, long applications)
    {
        DefaultNameFactory nameFactory = new DefaultNameFactory(AccordMetrics.ACCORD_REPLICA, scope);
        Map<String, Long> metrics = diff(countingMetrics0).get(node);
        Function<String, Long> metric = n -> metrics.get(nameFactory.createMetricName(n).getMetricName());
        assertThat(metric.apply(AccordMetrics.REPLICA_STABLE_LATENCY)).isLessThanOrEqualTo(stable);
        assertThat(metric.apply(AccordMetrics.REPLICA_PREAPPLY_LATENCY)).isEqualTo(executions);
        assertThat(metric.apply(AccordMetrics.REPLICA_APPLY_LATENCY)).isEqualTo(applications);
        assertThat(metric.apply(AccordMetrics.REPLICA_APPLY_DURATION)).isEqualTo(scope.equals("rw") ? applications : 0);
        assertThat(metric.apply(AccordMetrics.REPLICA_DEPENDENCIES)).isEqualTo(executions);

        // Verify that replica metrics are published to the appropriate virtual table:
        SimpleQueryResult vtableResults = SHARED_CLUSTER.get(node + 1)
                                              .executeInternalWithResult("SELECT * FROM system_metrics.accord_replica_group WHERE scope = ?", scope);

        while (vtableResults.hasNext())
        {
            Row metricRow = vtableResults.next();
            String name = metricRow.getString("name");
            assertThat(metrics).containsKey(name);
        }

        // Verify that per-store global cache stats are published to the appropriate virtual table:
        SimpleQueryResult storeCacheResults =
            SHARED_CLUSTER.get(node + 1).executeInternalWithResult(format("SELECT * FROM %s.%s", SchemaConstants.VIRTUAL_ACCORD_DEBUG, AccordDebugKeyspace.EXECUTOR_CACHE));
        assertThat(storeCacheResults).hasNext();
    }

    private Map<Integer, Map<String, Long>> getMetrics()
    {
        Map<Integer, Map<String, Long>> metrics = new HashMap<>();
        for (int i = 0; i < SHARED_CLUSTER.size(); i++)
        {
            Map<String, Long> map = SHARED_CLUSTER.get(i + 1).metrics().getCounters(name -> name.startsWith("org.apache.cassandra.metrics.Accord") || (name.startsWith("org.apache.cassandra.metrics.ClientRequest") && (name.endsWith("AccordRead") || name.endsWith("AccordWrite"))));
            SHARED_CLUSTER.get(i + 1).metrics().getGauges(name -> name.startsWith("org.apache.cassandra.metrics.AccordReplica"))
                                               .forEach((key, value) -> map.put(key, (Long)value));
            metrics.put(i, map);
        }
        return metrics;
    }

    private Map<Integer, Map<String, Long>> diff(Map<Integer, Map<String, Long>> prev)
    {
        Map<Integer, Map<String, Long>> curr = getMetrics();
        Map<Integer, Map<String, Long>> diff = new HashMap<>();
        for (int i = 0; i < SHARED_CLUSTER.size(); i++)
        {
            Map<String, Long> prevNode = prev.get(i);
            Map<String, Long> currNode = curr.get(i);
            Map<String, Long> diffNode = new HashMap<>();
            for (Map.Entry<String, Long> currEntry : currNode.entrySet())
            {
                Long prevVal = prevNode.get(currEntry.getKey());
                if (prevVal != null)
                    diffNode.put(currEntry.getKey(), currEntry.getValue() - prevVal);
            }
            diff.put(i, diffNode);
        }
        return diff;
    }
}
