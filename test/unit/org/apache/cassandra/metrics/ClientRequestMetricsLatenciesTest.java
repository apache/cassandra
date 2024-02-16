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

package org.apache.cassandra.metrics;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.NODE_LOCAL;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.utils.Throwables.merge;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * This test verifies (and documents) the relationship between specific queries
 * and the latency-measuring parts of client request metrics that get updated as a result
 * of handling such queries.
 * <p/>
 * The test is tightly coupled with the {@link ClientRequestsMetrics} class, and the implementation
 * of user query handling, but the author perceives the documentation value alone is worth that sacrifice
 * (after all the client request metrics are like a public API).
 * <p/>
 * The core of the test is the
 * {@link #processQueryAndCheckMetricsWereBumped(String, ConsistencyLevel, ClientRequestMetrics...)}
 * function, which runs the CQL query in a specific CL and then verifies that the specified client
 * request metrics are bumped (and only them).
 */
public class ClientRequestMetricsLatenciesTest
{
    private static final Logger logger = LoggerFactory.getLogger(ClientRequestMetricsLatenciesTest.class);
    private static final String ksName = "client_requests_metrics_test";
    private static final String cfName = "test_table";
    private static ClientRequestsMetrics clientMetrics;

    @BeforeClass
    public static void beforeTest() throws ConfigurationException
    {
        CassandraRelevantProperties.ENABLE_NODELOCAL_QUERIES.setBoolean(true);

        SchemaLoader.loadSchema();

        StorageService.instance.initServer(0);

        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                   "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                                   ksName);
        QueryProcessor.process(cql, ONE);

        cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int, v1 int, v2 int, PRIMARY KEY (k, v1))", ksName, cfName);
        QueryProcessor.process(cql, ONE);

        cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s_counter (k int PRIMARY KEY, ct counter)", ksName, cfName);
        QueryProcessor.process(cql, ONE);

        clientMetrics = ClientRequestsMetricsProvider.instance.metrics(ksName);
    }

    @Test
    public void testWriteMetrics()
    {
        List.of(ONE, QUORUM, LOCAL_QUORUM, ALL, ANY, NODE_LOCAL).forEach(cl -> {
            processQueryAndCheckMetricsWereBumped("INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (7, 7, 7);",
                                                  cl,
                                                  clientMetrics.writeMetrics,
                                                  clientMetrics.writeMetricsForLevel(cl));
            processQueryAndCheckMetricsWereBumped("BEGIN BATCH " +
                                                  "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 5, 5); " +
                                                  "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 6, 6); " +
                                                  "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 8, 8); " +
                                                  "APPLY BATCH",
                                                  cl,
                                                  clientMetrics.writeMetrics,
                                                  clientMetrics.writeMetricsForLevel(cl));
        });

        List.of(ONE, QUORUM, LOCAL_QUORUM, ALL, NODE_LOCAL).forEach(cl -> {
            processQueryAndCheckMetricsWereBumped("UPDATE %1$s.%2$s_counter SET ct = ct + 1 WHERE k=123;",
                                                  cl,
                                                  clientMetrics.writeMetrics,
                                                  clientMetrics.writeMetricsForLevel(cl));
        });
    }

    @Test
    public void testReadMetrics()
    {
        List.of(ONE, QUORUM, LOCAL_QUORUM, ALL, NODE_LOCAL).forEach(cl -> {
            processQueryAndCheckMetricsWereBumped("SELECT * FROM %1$s.%2$s WHERE k=123 and v1=234;",
                                                  cl,
                                                  clientMetrics.readMetrics,
                                                  clientMetrics.readMetricsForLevel(cl));
        });

        QueryProcessor.process(String.format("UPDATE %1$s.%2$s_counter SET ct = ct + 1 WHERE k=123;", ksName, cfName), ONE);
        List.of(ONE, QUORUM, LOCAL_QUORUM, ALL, NODE_LOCAL).forEach(cl -> {
            processQueryAndCheckMetricsWereBumped("SELECT ct FROM %1$s.%2$s_counter WHERE k=123;",
                                                  cl,
                                                  clientMetrics.readMetrics,
                                                  clientMetrics.readMetricsForLevel(cl));
        });
    }

    @Test
    public void testRangeMetrics()
    {
        List.of(ONE, QUORUM, LOCAL_QUORUM, ALL).forEach(cl -> {
            processQueryAndCheckMetricsWereBumped("SELECT * FROM %1$s.%2$s;",
                                                  cl,
                                                  clientMetrics.rangeMetrics);
        });
    }

    @Test
    public void testCASWriteMetrics()
    {
        List.of(ONE, QUORUM, LOCAL_QUORUM, ALL, ANY).forEach(cl -> {
            QueryProcessor.process(String.format("TRUNCATE %1$s.%2$s;", ksName, cfName), ALL);
            processQueryAndCheckMetricsWereBumped("INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (7, 7, 7) IF NOT EXISTS;",
                                                  cl,
                                                  clientMetrics.readMetrics,
                                                  clientMetrics.readMetricsForLevel(QUORUM),
                                                  clientMetrics.casWriteMetrics,
                                                  clientMetrics.writeMetricsForLevel(SERIAL));
            processQueryAndCheckMetricsWereBumped("UPDATE %1$s.%2$s SET v2=123 WHERE k=7 AND v1=7 IF v2=71;",
                                                  cl,
                                                  clientMetrics.readMetrics,
                                                  clientMetrics.readMetricsForLevel(QUORUM),
                                                  clientMetrics.casWriteMetrics,
                                                  clientMetrics.writeMetricsForLevel(SERIAL));
            processQueryAndCheckMetricsWereBumped("UPDATE %1$s.%2$s SET v2=123 WHERE k=7 AND v1=7 IF v2=7;",
                                                  cl,
                                                  clientMetrics.readMetrics,
                                                  clientMetrics.readMetricsForLevel(QUORUM),
                                                  clientMetrics.casWriteMetrics,
                                                  clientMetrics.writeMetricsForLevel(SERIAL));
        });
    }

    @Test
    public void testCASReadMetrics()
    {
        processQueryAndCheckMetricsWereBumped("SELECT * FROM %1$s.%2$s WHERE k=7 AND v1=7;",
                                              SERIAL,
                                              clientMetrics.casReadMetrics,
                                              clientMetrics.readMetrics,
                                              clientMetrics.readMetricsForLevel(SERIAL));
        processQueryAndCheckMetricsWereBumped("SELECT * FROM %1$s.%2$s WHERE k=7 AND v1=7;",
                                              LOCAL_SERIAL,
                                              clientMetrics.casReadMetrics,
                                              clientMetrics.readMetrics,
                                              clientMetrics.readMetricsForLevel(LOCAL_SERIAL));
    }

    @Test
    public void testViewWriteMetrics()
    {
        String cql = String.format("CREATE MATERIALIZED VIEW %1$s.example_view AS\n" +
                                   "SELECT *\n" +
                                   "FROM %1$s.%2$s\n" +
                                   "WHERE k IS NOT NULL\n" +
                                   "  AND v1 IS NOT NULL\n" +
                                   "  AND v2 IS NOT NULL\n" +
                                   "PRIMARY KEY (v2, k, v1)\n",
                                   ksName, cfName);
        QueryProcessor.process(cql, ONE);

        // Unfortunately, accesses to the system keyspaces are counted as client requests in ClientRequestsMetrics.
        // To work around this let's wait until the view build is finished and system writes complete.
        await().atMost(5, TimeUnit.SECONDS).pollDelay(0, TimeUnit.SECONDS).until(
            () -> QueryProcessor.process(String.format("SELECT status FROM system_distributed.view_build_status WHERE keyspace_name='%1$s' AND view_name='example_view';", ksName),
                                         ONE)
                                .one().getString("status").equals("SUCCESS")

        );

        try {
            List.of(ONE, QUORUM, LOCAL_QUORUM, ALL, ANY).forEach(cl -> {
                QueryProcessor.process(String.format("TRUNCATE %1$s.%2$s;", ksName, cfName), ALL);
                processQueryAndCheckMetricsWereBumped("INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (7, 7, 7) IF NOT EXISTS;",
                                                      cl,
                                                      clientMetrics.readMetrics,
                                                      clientMetrics.readMetricsForLevel(QUORUM),
                                                      clientMetrics.casWriteMetrics,
                                                      clientMetrics.writeMetricsForLevel(SERIAL),
                                                      clientMetrics.viewWriteMetrics);
                processQueryAndCheckMetricsWereBumped("UPDATE %1$s.%2$s SET v2=123 WHERE k=7 AND v1=7 IF v2=7;",
                                                      cl,
                                                      clientMetrics.readMetrics,
                                                      clientMetrics.readMetricsForLevel(QUORUM),
                                                      clientMetrics.casWriteMetrics,
                                                      clientMetrics.writeMetricsForLevel(SERIAL),
                                                      clientMetrics.viewWriteMetrics);

                logger.info("Dont expect metric bump for view write when the view is not updated");
                processQueryAndCheckMetricsWereBumped("INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (7, 7, 7) IF NOT EXISTS;",
                                                      cl,
                                                      clientMetrics.readMetrics,
                                                      clientMetrics.readMetricsForLevel(QUORUM),
                                                      clientMetrics.casWriteMetrics,
                                                      clientMetrics.writeMetricsForLevel(SERIAL));
                processQueryAndCheckMetricsWereBumped("UPDATE %1$s.%2$s SET v2=123 WHERE k=7 AND v1=7 IF v2=7;",
                                                      cl,
                                                      clientMetrics.readMetrics,
                                                      clientMetrics.readMetricsForLevel(QUORUM),
                                                      clientMetrics.casWriteMetrics,
                                                      clientMetrics.writeMetricsForLevel(SERIAL));

            });

            List.of(ONE, QUORUM, LOCAL_QUORUM, ALL, ANY, NODE_LOCAL).forEach(cl -> {
                QueryProcessor.process(String.format("TRUNCATE %1$s.%2$s;", ksName, cfName), ALL);
                processQueryAndCheckMetricsWereBumped("INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (7, 7, 7);",
                                                      cl,
                                                      clientMetrics.writeMetrics,
                                                      clientMetrics.writeMetricsForLevel(cl),
                                                      clientMetrics.viewWriteMetrics);
                processQueryAndCheckMetricsWereBumped("BEGIN BATCH " +
                                                      "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 5, 5); " +
                                                      "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 6, 6); " +
                                                      "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 8, 8); " +
                                                      "APPLY BATCH",
                                                      cl,
                                                      clientMetrics.writeMetrics,
                                                      clientMetrics.writeMetricsForLevel(cl),
                                                      clientMetrics.viewWriteMetrics);

                // expect bump of view write metrics when the view is updated with the same values
                processQueryAndCheckMetricsWereBumped("INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (7, 7, 7);",
                                                      cl,
                                                      clientMetrics.writeMetrics,
                                                      clientMetrics.writeMetricsForLevel(cl),
                                                      clientMetrics.viewWriteMetrics);
                processQueryAndCheckMetricsWereBumped("BEGIN BATCH " +
                                                      "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 5, 5); " +
                                                      "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 6, 6); " +
                                                      "  INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (5, 8, 8); " +
                                                      "APPLY BATCH",
                                                      cl,
                                                      clientMetrics.writeMetrics,
                                                      clientMetrics.writeMetricsForLevel(cl),
                                                      clientMetrics.viewWriteMetrics);
            });
        }
        finally
        {
            QueryProcessor.process(String.format("DROP MATERIALIZED VIEW IF EXISTS %1$s.example_view", ksName), ONE);
        }
    }

    /**
     With NODE_LOCAL consistency level we don't bump the same metrics as with other consistency levels
     see {@link ClientRequestMetricsLatenciesTest#testRangeMetrics()} and CASSANDRA-19379.
     */
    @Test
    public void testRangeMetricsWithNODE_LOCAL()
    {
        processQueryAndCheckMetricsWereBumped("SELECT * FROM %1$s.%2$s;",
                                              NODE_LOCAL,
                                              clientMetrics.readMetrics,
                                              clientMetrics.readMetricsForLevel(NODE_LOCAL));
    }

    /**
      With NODE_LOCAL consistency level we don't bump the same metrics as with other consistency levels
      see {@link ClientRequestMetricsLatenciesTest#testCASWriteMetrics()} and CASSANDRA-19379.
    */
    @Test
    public void testCASWriteMetricsWithNODE_LOCAL()
    {
        QueryProcessor.process(String.format("TRUNCATE %1$s.%2$s;", ksName, cfName), ALL);

        processQueryAndCheckMetricsWereBumped("INSERT INTO %1$s.%2$s (k, v1, v2) VALUES (7, 7, 7) IF NOT EXISTS;",
                                              NODE_LOCAL,
                                              clientMetrics.writeMetrics,
                                              clientMetrics.writeMetricsForLevel(NODE_LOCAL));
        processQueryAndCheckMetricsWereBumped("UPDATE %1$s.%2$s SET v2=123 WHERE k=7 AND v1=7 IF v2=71;",
                                              NODE_LOCAL,
                                              clientMetrics.writeMetrics,
                                              clientMetrics.writeMetricsForLevel(NODE_LOCAL));
    }

    private void processQueryAndCheckMetricsWereBumped(String cql, ConsistencyLevel consistencyLevel, ClientRequestMetrics... expectedBumpedMetric)
    {
        logger.info("Processing query {} with cl={}", cql, consistencyLevel.name());
        try
        {
            // snapshot metrics prior to query
            HashMap<NamedMetric, Long> beforeExecution = getMetricsSnapshot(m -> m.executionTimeMetrics);
            HashMap<NamedMetric, Long> beforeServiceTime = getMetricsSnapshot(m -> m.serviceTimeMetrics);

            // process query
            QueryProcessor.process(String.format(cql, ksName, cfName), consistencyLevel);

            // check if the metrics were bumped
            // since MV update is async, let's allow waiting for the metrics to settle
            await().atMost(5, TimeUnit.SECONDS).pollDelay(0, TimeUnit.SECONDS).untilAsserted(() -> {
                // get metrics snapshot
                HashMap<NamedMetric, Long> afterExecution = getMetricsSnapshot(m -> m.executionTimeMetrics);
                HashMap<NamedMetric, Long> afterServiceTime = getMetricsSnapshot(m -> m.serviceTimeMetrics);

                // compare snapshots; only expect expectedBumpedMetric to be bumped
                Throwable verificationError = null;
                for (NamedMetric namedMetric : beforeExecution.keySet())
                {
                    try
                    {
                        if (Arrays.stream(expectedBumpedMetric).anyMatch(bumped -> namedMetric.metric == bumped))
                        {
                            assertBumpOf(namedMetric, beforeExecution, afterExecution);
                            assertBumpOf(namedMetric, beforeServiceTime, afterServiceTime);
                        }
                        else
                        {
                            assertNoBumpOf(namedMetric, beforeExecution, afterExecution);
                            assertNoBumpOf(namedMetric, beforeServiceTime, afterServiceTime);
                        }
                    }
                    catch (AssertionError e)
                    {
                        logger.error("Assertion failed for metric {}", namedMetric, e);
                        verificationError = merge(verificationError, e);
                    }
                }
                if (verificationError != null)
                {
                    String errorMessage = String.format("Metrics bumping verification failed for %s, cl %s", cql, consistencyLevel.name());
                    throw new AssertionError(errorMessage, verificationError);
                }
            });
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void assertBumpOf(NamedMetric namedMetric, HashMap<NamedMetric, Long> before, HashMap<NamedMetric, Long> after)
    {
        String errorMessage = String.format("Expected bump of %s metric; before processing: %d, after processing: %d", namedMetric.name, before.get(namedMetric), after.get(namedMetric));
        assertEquals(errorMessage, 1, after.get(namedMetric) - before.get(namedMetric));
    }

    private void assertNoBumpOf(NamedMetric metric, HashMap<NamedMetric, Long> before, HashMap<NamedMetric, Long> after)
    {
        String errorMessage = String.format("Did not expect bump of %s metric; before processing: %d, after processing: %d", metric.name, before.get(metric), after.get(metric));
        assertEquals(errorMessage, before.get(metric), after.get(metric));
    }

    private HashMap<NamedMetric, Long> getMetricsSnapshot(Function<ClientRequestMetrics, LatencyMetrics> latencyMetrics) throws IllegalAccessException
    {
        // This is brittle etc.
        // However, I find the ability to verify which scenarios bump which metrics to be very useful,
        // and I'm willing to pay for it by having this somewhat ugly test method, which may break if
        // the implementation of ClientRequestsMetrics changes.
        HashMap<NamedMetric, Long> snapshot = new HashMap<>();
        for (Field field : ClientRequestsMetrics.class.getDeclaredFields())
        {
            field.setAccessible(true);
            if (ClientRequestMetrics.class.isAssignableFrom(field.getType()))
            {
                ClientRequestMetrics requestMetrics = (ClientRequestMetrics) field.get(clientMetrics);
                snapshot.put(new NamedMetric(requestMetrics, field.getName()),
                             latencyMetrics.apply(requestMetrics).latency.getCount());
            }
            else if (field.getType().equals(Map.class))
            {
                Map<ConsistencyLevel, ClientRequestMetrics> clMetrics = (Map<ConsistencyLevel, ClientRequestMetrics>) field.get(clientMetrics);
                if (clMetrics != null)
                {
                    for (ConsistencyLevel cl: clMetrics.keySet())
                    {
                        snapshot.put(new NamedMetric(clMetrics.get(cl), String.format("%s[%s]", field.getName(), cl.name())),
                                     latencyMetrics.apply(clMetrics.get(cl)).latency.getCount());
                    }
                }
            }
        }
        return snapshot;
    }

    private static class NamedMetric
    {
        private final ClientRequestMetrics metric;
        private final String name;

        public NamedMetric(ClientRequestMetrics metric, String name)
        {
            this.metric = metric;
            this.name = name;
        }

        @Override
        public String toString()
        {
            return "NamedMetric{" +
                   "name='" + name + '\'' +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NamedMetric that = (NamedMetric) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }
    }
}
