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
import java.util.function.Function;

import com.google.common.base.Throwables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.metrics.AccordMetrics;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.RatioGaugeSet;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.assertj.core.data.Offset;

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
        AccordTestBase.setupClass();
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));
        for (int i = 0; i < SHARED_CLUSTER.size(); i++) // initialize metrics
            logger.trace(SHARED_CLUSTER.get(i + 1).callOnInstance(() -> AccordMetrics.readMetrics.toString() + AccordMetrics.writeMetrics.toString()));
    }

    String writeCql()
    {
        return "BEGIN TRANSACTION\n" +
               "  LET val = (SELECT v FROM " + currentTable + " WHERE k=? AND c=?);\n" +
               "  SELECT val.v;\n" +
               "  UPDATE " + currentTable + " SET v = v + 1 WHERE k=? AND c=?;\n" +
               "COMMIT TRANSACTION";
    }

    String readCql()
    {
        return "BEGIN TRANSACTION\n" +
               "  LET val = (SELECT v FROM " + currentTable + " WHERE k=? AND c=?);\n" +
               "  SELECT val.v;\n" +
               "COMMIT TRANSACTION";
    }

    Map<Integer, Map<String, Long>> countingMetrics0;

    @Before
    public void beforeTest()
    {
        SHARED_CLUSTER.filters().reset();
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + currentTable + " (k int, c int, v int, PRIMARY KEY (k, c))");
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + currentTable + " (k, c, v) VALUES (0, 0, 0)", ConsistencyLevel.ALL);
    }

    @Test
    public void testRegularMetrics() throws Exception
    {
        countingMetrics0 = getMetrics();
        SHARED_CLUSTER.coordinator(1).executeWithResult(writeCql(), ConsistencyLevel.ALL, 0, 0, 0, 0);
        assertCoordinatorMetrics(0, "rw", 1, 0, 0, 0, 0);
        assertCoordinatorMetrics(1, "rw", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "rw", 1, 1, 1);
        assertReplicaMetrics(1, "rw", 1, 1, 1);
        assertZeroMetrics("ro");

        countingMetrics0 = getMetrics();
        SHARED_CLUSTER.coordinator(1).executeWithResult(readCql(), ConsistencyLevel.ALL, 0, 0);
        assertCoordinatorMetrics(0, "ro", 1, 0, 0, 0, 0);
        assertCoordinatorMetrics(1, "ro", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "ro", 1, 1, 0);
        assertReplicaMetrics(1, "ro", 1, 1, 0);
        assertZeroMetrics("rw");
    }

    @Test
    public void testPreemptionMetrics()
    {
        IMessageFilters.Filter commitFilter1 = SHARED_CLUSTER.filters().outbound().verbs(Verb.ACCORD_COMMIT_REQ.id).from(1).to(1).drop();
        IMessageFilters.Filter commitFilter2 = SHARED_CLUSTER.filters().outbound().verbs(Verb.ACCORD_COMMIT_REQ.id).from(1).to(2).drop();
        commitFilter1.on();
        commitFilter2.on();

        countingMetrics0 = getMetrics();
        try
        {
            SHARED_CLUSTER.coordinator(1).executeWithResult(writeCql(), ConsistencyLevel.ALL, 0, 0, 0, 0);
            fail("expected to fail");
        }
        catch (RuntimeException ex)
        {
            assertThat(Throwables.getCausalChain(ex).stream().map(t -> t.getClass().getName())).contains(WritePreemptedException.class.getName());
        }

        assertCoordinatorMetrics(0, "rw", 1, 0, 1, 0, 0);
        assertCoordinatorMetrics(1, "rw", 0, 0, 0, 0, 1);
        assertReplicaMetrics(0, "rw", 1, 1, 1);
        assertReplicaMetrics(1, "rw", 1, 1, 1);

        assertZeroMetrics("ro");

        countingMetrics0 = getMetrics();
        try
        {
            SHARED_CLUSTER.coordinator(1).executeWithResult(readCql(), ConsistencyLevel.ALL, 0, 0);
            fail("expected to fail");
        }
        catch (RuntimeException ex)
        {
            assertThat(Throwables.getCausalChain(ex).stream().map(t -> t.getClass().getName())).contains(ReadPreemptedException.class.getName());
        }

        assertCoordinatorMetrics(0, "ro", 1, 0, 1, 0, 0);
        assertCoordinatorMetrics(1, "ro", 0, 0, 0, 0, 1);
        assertReplicaMetrics(0, "ro", 1, 1, 0);
        assertReplicaMetrics(1, "ro", 1, 1, 0);

        assertZeroMetrics("rw");
    }

    @Test
    public void testTimeoutMetrics()
    {
        IMessageFilters.Filter preAcceptFilter = SHARED_CLUSTER.filters().outbound().verbs(Verb.ACCORD_PRE_ACCEPT_REQ.id).from(1).to(2).drop();
        preAcceptFilter.on();

        countingMetrics0 = getMetrics();
        try
        {
            SHARED_CLUSTER.coordinator(1).executeWithResult(readCql(), ConsistencyLevel.ALL, 0, 0);
            fail("expected to fail");
        }
        catch (RuntimeException ex)
        {
            assertThat(Throwables.getCausalChain(ex).stream().map(t -> t.getClass().getName())).contains(ReadTimeoutException.class.getName());
        }

        assertCoordinatorMetrics(0, "ro", 0, 0, 0, 1, 0);
        assertCoordinatorMetrics(1, "ro", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "ro", 0, 0, 0);
        assertReplicaMetrics(1, "ro", 0, 0, 0);

        assertZeroMetrics("rw");

        countingMetrics0 = getMetrics();
        try
        {
            SHARED_CLUSTER.coordinator(1).executeWithResult(writeCql(), ConsistencyLevel.ALL, 0, 0, 0, 0);
            fail("expected to fail");
        }
        catch (RuntimeException ex)
        {
            assertThat(Throwables.getCausalChain(ex).stream().map(t -> t.getClass().getName())).contains(WriteTimeoutException.class.getName());
        }

        assertCoordinatorMetrics(0, "rw", 0, 0, 0, 1, 0);
        assertCoordinatorMetrics(1, "rw", 0, 0, 0, 0, 0);
        assertReplicaMetrics(0, "rw", 0, 0, 0);
        assertReplicaMetrics(1, "rw", 0, 0, 0);

        assertZeroMetrics("ro");
    }

    private void assertZeroMetrics(String scope)
    {
        for (int i = 0; i < SHARED_CLUSTER.size(); i++)
        {
            assertCoordinatorMetrics(i, scope, 0, 0, 0, 0, 0);
            assertReplicaMetrics(i, scope, 0, 0, 0);
        }
    }

    private void assertCoordinatorMetrics(int node, String scope, long fastPaths, long slowPaths, long preempts, long timeouts, long recoveries)
    {
        DefaultNameFactory nameFactory = new DefaultNameFactory(AccordMetrics.ACCORD_COORDINATOR, scope);
        Map<String, Long> metrics = diff(countingMetrics0).get(node);
        logger.info("Metrics for node {} / {}: {}", node, scope, metrics);
        Function<String, Long> metric = n -> metrics.get(nameFactory.createMetricName(n).getMetricName());
        assertThat(metric.apply(AccordMetrics.FAST_PATHS)).isEqualTo(fastPaths);
        assertThat(metric.apply(AccordMetrics.SLOW_PATHS)).isEqualTo(slowPaths);
        assertThat(metric.apply(AccordMetrics.PREEMPTS)).isEqualTo(preempts);
        assertThat(metric.apply(AccordMetrics.TIMEOUTS)).isEqualTo(timeouts);
        assertThat(metric.apply(AccordMetrics.RECOVERY_DELAY)).isEqualTo(recoveries);
        assertThat(metric.apply(AccordMetrics.RECOVERY_TIME)).isEqualTo(recoveries);
        assertThat(metric.apply(AccordMetrics.DEPENDENCIES)).isEqualTo(fastPaths + slowPaths);

        if ((fastPaths + slowPaths) > 0)
        {
            String fastPathToTotalName = nameFactory.createMetricName(AccordMetrics.FAST_PATH_TO_TOTAL + "." + RatioGaugeSet.MEAN_RATIO).getMetricName();
            assertThat((double) SHARED_CLUSTER.get(1).metrics().getGauge(fastPathToTotalName)).isEqualTo((double) fastPaths / (double) (fastPaths + slowPaths), Offset.offset(0.01d));
        }
    }

    private void assertReplicaMetrics(int node, String scope, long commits, long executions, long applications)
    {
        DefaultNameFactory nameFactory = new DefaultNameFactory(AccordMetrics.ACCORD_REPLICA, scope);
        Map<String, Long> metrics = diff(countingMetrics0).get(node);
        Function<String, Long> metric = n -> metrics.get(nameFactory.createMetricName(n).getMetricName());
        assertThat(metric.apply(AccordMetrics.COMMIT_LATENCY)).isEqualTo(commits);
        assertThat(metric.apply(AccordMetrics.EXECUTE_LATENCY)).isEqualTo(executions);
        assertThat(metric.apply(AccordMetrics.APPLY_LATENCY)).isEqualTo(applications);
        assertThat(metric.apply(AccordMetrics.APPLY_DURATION)).isEqualTo(applications);
        assertThat(metric.apply(AccordMetrics.PARTIAL_DEPENDENCIES)).isEqualTo(executions);
    }

    private Map<Integer, Map<String, Long>> getMetrics()
    {
        Map<Integer, Map<String, Long>> metrics = new HashMap<>();
        for (int i = 0; i < SHARED_CLUSTER.size(); i++)
            metrics.put(i, SHARED_CLUSTER.get(i + 1).metrics().getCounters(name -> name.startsWith("org.apache.cassandra.metrics.accord-")));
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
