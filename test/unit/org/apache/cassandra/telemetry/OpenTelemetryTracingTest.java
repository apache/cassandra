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

package org.apache.cassandra.telemetry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.StorageService;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.semconv.DbAttributes;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Single node OpenTelemetry tracing test.
 */
public class OpenTelemetryTracingTest extends CQLTester
{
    @Rule
    public final OpenTelemetryRule otelTesting = OpenTelemetryRule.create();

    private static final String OTEL_TEST_TABLE_NAME = "otel_test";
    private static final String OTEL_TEST_COUNTER_TABLE_NAME = "otel_test_counter";

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        CQLTester.setUpClass();
        // For counter-mutations, we need to enable coordinator execution
        StorageService.instance.setRpcReady(true);
    }

    @AfterClass
    public static void tearDownClass()
    {
        CQLTester.disableCoordinatorExecution();
        Telemetry.setOpenTelemetryUnsafe(OpenTelemetry.noop());
    }

    @Before
    public void setUp()
    {
        Telemetry.setOpenTelemetryUnsafe(otelTesting.getOpenTelemetry());
        CQLTester.requireNetwork();
        createTable(KEYSPACE, "CREATE TABLE IF NOT EXISTS %s (key text PRIMARY KEY, value text)", OTEL_TEST_TABLE_NAME);
        createTable(KEYSPACE, "CREATE TABLE IF NOT EXISTS %s (key text PRIMARY KEY, count counter)", OTEL_TEST_COUNTER_TABLE_NAME);
    }

    /**
     * Test Span creation for a single partition read with cassandra tracing enabled.
     */
    @Test
    public void testSpanForSinglePartitionReadWithTracing()
    {
        int pageSize = 100;
        ConsistencyLevel cl = ConsistencyLevel.LOCAL_QUORUM;
        Session session = sessionNet();

        String query = String.format("SELECT * FROM %s.%s WHERE key = 'a'", KEYSPACE, OTEL_TEST_TABLE_NAME);
        Statement simpleStatement = new SimpleStatement(query)
                                    .setConsistencyLevel(cl)
                                    .setFetchSize(pageSize)
                                    .enableTracing();
        ResultSet result = session.execute(simpleStatement);
        QueryTrace cassandraTrace = result.getExecutionInfo().getQueryTrace();

        List<SpanData> spans = otelTesting.getSpans();

        assertThat(spans).hasSize(2);

        // Spans are not guaranteed to be returned in order.
        Map<String, SpanData> spansByName = spans.stream().collect(Collectors.toMap(SpanData::getName, Function.identity()));

        SpanData readSpan = spansByName.get(String.format("READ_REQ %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData coordinatorSpan = spansByName.get(String.format("QUERY SELECT %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));

        assertThat(readSpan).isNotNull();
        assertThat(coordinatorSpan).isNotNull();

        assertThat(readSpan.getTraceId()).isEqualTo(coordinatorSpan.getTraceId());
        assertThat(readSpan.getParentSpanId()).isEqualTo(coordinatorSpan.getSpanId());

        Map<AttributeKey<?>, Object> querySpanAttrs = coordinatorSpan.getAttributes().asMap();
        assertThat(querySpanAttrs).containsEntry(DbAttributes.DB_SYSTEM_NAME, CassandraAttributes.DB_SYSTEM_NAME_CASSANDRA);
        assertThat(querySpanAttrs).containsEntry(CassandraAttributes.CASSANDRA_QUERY_TYPE, "QUERY");
        // Page size is a Long attribute type
        assertThat(querySpanAttrs).containsEntry(CassandraAttributes.CASSANDRA_PAGE_SIZE, (long) pageSize);
        // Consistency level in span attributes is in lowercase
        assertThat(querySpanAttrs).containsEntry(CassandraAttributes.CASSANDRA_CONSISTENCY_LEVEL, cl.name().toLowerCase());
        // Attribute should not leak raw query
        assertThat(querySpanAttrs).doesNotContainKey(DbAttributes.DB_QUERY_TEXT);

        // Together, spans contain the same number of events as Cassandra trace
        int eventsCount = spans.stream().mapToInt(SpanData::getTotalRecordedEvents).sum();
        assertThat(eventsCount).isEqualTo(cassandraTrace.getEvents().size());
    }

    /**
     * Test Span creation for a partition range read with cassandra tracing enabled.
     */
    @Test
    public void testSpanForPartitionRangeReadWithTracing()
    {
        Session session = sessionNet();

        String query = String.format("SELECT * FROM %s.%s LIMIT 5", KEYSPACE, OTEL_TEST_TABLE_NAME);
        Statement simpleStatement = new SimpleStatement(query).enableTracing();
        ResultSet result = session.execute(simpleStatement);
        QueryTrace cassandraTrace = result.getExecutionInfo().getQueryTrace();

        List<SpanData> spans = otelTesting.getSpans();

        assertThat(spans).hasSize(2);

        // Spans are not guaranteed to be returned in order.
        Map<String, SpanData> spansByName = spans.stream().collect(Collectors.toMap(SpanData::getName, Function.identity()));

        SpanData readSpan = spansByName.get(String.format("READ_REQ %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData coordinatorSpan = spansByName.get(String.format("QUERY SELECT %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));

        assertThat(readSpan).isNotNull();
        assertThat(coordinatorSpan).isNotNull();

        assertThat(readSpan.getTraceId()).isEqualTo(coordinatorSpan.getTraceId());
        // coordinator span
        //     |-> replica span (read)
        assertThat(readSpan.getParentSpanId()).isEqualTo(coordinatorSpan.getSpanId());

        int eventsCount = spans.stream().mapToInt(SpanData::getTotalRecordedEvents).sum();
        assertThat(eventsCount).isEqualTo(cassandraTrace.getEvents().size());
    }

    /**
     * Test Span creation for mutation with cassandra tracing enabled.
     */
    @Test
    public void testSpanForMutationWithTracing()
    {
        Session session = sessionNet();

        String query = String.format("INSERT INTO %s.%s (key, value) VALUES ('key', 'value')", KEYSPACE, OTEL_TEST_TABLE_NAME);
        Statement simpleStatement = new SimpleStatement(query).enableTracing();
        ResultSet result = session.execute(simpleStatement);
        QueryTrace cassandraTrace = result.getExecutionInfo().getQueryTrace();

        List<SpanData> spans = otelTesting.getSpans();

        assertThat(spans).hasSize(2);

        // Spans are not guaranteed to be returned in order.
        Map<String, SpanData> spansByName = spans.stream().collect(Collectors.toMap(SpanData::getName, Function.identity()));

        SpanData mutationSpan = spansByName.get(String.format("MUTATION_REQ %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData coordinatorSpan = spansByName.get(String.format("QUERY INSERT %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));

        assertThat(mutationSpan).isNotNull();
        assertThat(coordinatorSpan).isNotNull();

        assertThat(mutationSpan.getTraceId()).isEqualTo(coordinatorSpan.getTraceId());
        // coordinator span
        //     |-> replica span (mutation)
        assertThat(mutationSpan.getParentSpanId()).isEqualTo(coordinatorSpan.getSpanId());

        int eventsCount = spans.stream().mapToInt(SpanData::getTotalRecordedEvents).sum();
        assertThat(eventsCount).isEqualTo(cassandraTrace.getEvents().size());
    }

    /**
     * Test Span creation for counter-mutation with cassandra tracing enabled.
     */
    @Test
    public void testSpanForCounterMutationWithTracing()
    {
        Session session = sessionNet();

        String query = String.format("UPDATE %s.%s SET count += 1 WHERE key = 'a'", KEYSPACE, OTEL_TEST_COUNTER_TABLE_NAME);
        Statement simpleStatement = new SimpleStatement(query).enableTracing();
        ResultSet result = session.execute(simpleStatement);
        QueryTrace cassandraTrace = result.getExecutionInfo().getQueryTrace();

        List<SpanData> spans = otelTesting.getSpans();

        assertThat(spans).hasSize(2);

        // Spans are not guaranteed to be returned in order.
        Map<String, SpanData> spansByName = spans.stream().collect(Collectors.toMap(SpanData::getName, Function.identity()));

        SpanData counterMutationSpan = spansByName.get(String.format("COUNTER_MUTATION_REQ %s.%s", KEYSPACE, OTEL_TEST_COUNTER_TABLE_NAME));
        SpanData coordinatorSpan = spansByName.get(String.format("QUERY UPDATE %s.%s", KEYSPACE, OTEL_TEST_COUNTER_TABLE_NAME));

        assertThat(counterMutationSpan).isNotNull();
        assertThat(coordinatorSpan).isNotNull();

        assertThat(counterMutationSpan.getTraceId()).isEqualTo(coordinatorSpan.getTraceId());
        // coordinator span
        //     |-> replica span (counter mutation)
        assertThat(counterMutationSpan.getParentSpanId()).isEqualTo(coordinatorSpan.getSpanId());

        int eventsCount = spans.stream().mapToInt(SpanData::getTotalRecordedEvents).sum();
        assertThat(eventsCount).isEqualTo(cassandraTrace.getEvents().size());
    }

    /**
     * Test Span creation for batch mutation with cassandra tracing enabled.
     */
    @Test
    public void testSpanForLoggedBatchMutationWithTracing()
    {
        Session session = sessionNet();

        String query1 = String.format("INSERT INTO %s.%s (key, value) VALUES ('k1', 'v1')", KEYSPACE, OTEL_TEST_TABLE_NAME);
        String query2 = String.format("INSERT INTO %s.%s (key, value) VALUES ('k2', 'v2')", KEYSPACE, OTEL_TEST_TABLE_NAME);
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(new SimpleStatement(query1));
        batch.add(new SimpleStatement(query2));
        batch.enableTracing();
        ResultSet result = session.execute(batch);
        QueryTrace cassandraTrace = result.getExecutionInfo().getQueryTrace();

        List<SpanData> spans = otelTesting.getSpans();

        // Since this is a logged batch with two partitions, we have:
        // 1. coordinator span
        // 2. a replica span for batchlog store
        // 3. two replica spans for actual mutations
        // 4. a replica span for batchlog remove (optional, since this is async)
        assertThat(spans.size()).isGreaterThanOrEqualTo(4);
        assertThat(spans.size()).isLessThanOrEqualTo(5);

        String batchlogStoreSpanName = "MUTATION_REQ Batchlog store";
        String mutationSpanName = String.format("MUTATION_REQ %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME);
        String coordinatorSpanName = String.format("BATCH LOGGED BATCH INSERT %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME);
        String batchlogRemoveSpanName = "MUTATION_REQ Batchlog remove";

        // Coordinator should be unique
        List<SpanData> coordinatorSpans = spans.stream()
                                               .filter(s -> s.getName().equals(coordinatorSpanName))
                                               .collect(Collectors.toList());
        assertThat(coordinatorSpans).hasSize(1);
        SpanData coordinatorSpan = coordinatorSpans.get(0);

        // Batchlog store mutation span should be 1
        List<SpanData> batchlogStoreSpans = spans.stream()
                                            .filter(s -> s.getName().equals(batchlogStoreSpanName))
                                            .collect(Collectors.toList());
        assertThat(batchlogStoreSpans).hasSize(1);
        SpanData batchlogSpan = batchlogStoreSpans.get(0);
        assertThat(batchlogSpan.getTraceId()).isEqualTo(coordinatorSpan.getTraceId());
        assertThat(batchlogSpan.getParentSpanId()).isEqualTo(coordinatorSpan.getSpanId());

        // Replica mutation spans can be multiple
        List<SpanData> mutationSpans = spans.stream()
                                            .filter(s -> s.getName().equals(mutationSpanName))
                                            .collect(Collectors.toList());
        assertThat(mutationSpans).hasSize(2);
        for (SpanData mutationSpan : mutationSpans)
        {
            assertThat(mutationSpan.getTraceId()).isEqualTo(coordinatorSpan.getTraceId());
            assertThat(mutationSpan.getParentSpanId()).isEqualTo(coordinatorSpan.getSpanId());
        }

        // if batchlog remove span is present, validate it
        List<SpanData> batchlogRemoveSpans = spans.stream()
                                                 .filter(s -> s.getName().equals(batchlogRemoveSpanName))
                                                 .collect(Collectors.toList());
        if (!batchlogRemoveSpans.isEmpty())
        {
            assertThat(batchlogRemoveSpans).hasSize(1);
            SpanData batchlogRemoveSpan = batchlogRemoveSpans.get(0);
            assertThat(batchlogRemoveSpan.getTraceId()).isEqualTo(coordinatorSpan.getTraceId());
            // Batchlog remove is submitted from the mutation stage of the last successful mutation
            // so should not be from the coordinator span
            assertThat(batchlogRemoveSpan.getParentSpanId()).isNotEqualTo(coordinatorSpan.getSpanId());
        }

        int eventsCount = spans.stream().mapToInt(SpanData::getTotalRecordedEvents).sum();
        assertThat(eventsCount).isEqualTo(cassandraTrace.getEvents().size());
    }

    /**
     * Test Span creation of query from trace context propagation.
     */
    @Test
    public void testSpanFromContextPropagation()
    {
        ConsistencyLevel cl = ConsistencyLevel.LOCAL_QUORUM;
        Session session = sessionNet();

        String clientSpanName = "testSpanFromContextPropagation";
        Span span = otelTesting.getOpenTelemetry()
                               .getTracer("openTelemetryTracingTest")
                               .spanBuilder(clientSpanName)
                               .startSpan();
        try (Scope ignore = span.makeCurrent())
        {
            Map<String, ByteBuffer> payload = createTracingPayload();
            String query = String.format("SELECT * FROM %s.%s WHERE key = 'a'", KEYSPACE, OTEL_TEST_TABLE_NAME);
            Statement simpleStatement = new SimpleStatement(query)
                                        .setConsistencyLevel(cl)
                                        .setOutgoingPayload(payload);
            session.execute(simpleStatement);
        }
        finally
        {
            span.end();
        }

        List<SpanData> spans = otelTesting.getSpans();
        // Should contain: client span, coordinator span and replica span
        assertThat(spans).hasSize(3);

        // Spans are not guaranteed to be returned in order.
        Map<String, SpanData> spansByName = spans.stream().collect(Collectors.toMap(SpanData::getName, Function.identity()));

        SpanData replicaSpan = spansByName.get(String.format("READ_REQ %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData coordinatorSpan = spansByName.get(String.format("QUERY SELECT %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData clientSpan = spansByName.get(clientSpanName);

        assertThat(replicaSpan).isNotNull();
        assertThat(coordinatorSpan).isNotNull();
        assertThat(clientSpan).isNotNull();

        // They all should have the same trace ID
        assertThat(replicaSpan.getTraceId()).isEqualTo(clientSpan.getTraceId());
        assertThat(coordinatorSpan.getTraceId()).isEqualTo(clientSpan.getTraceId());
        // client span
        //   |-> coordinator span
        //        |-> replica span
        assertThat(coordinatorSpan.getParentSpanId()).isEqualTo(clientSpan.getSpanId());
        assertThat(replicaSpan.getParentSpanId()).isEqualTo(coordinatorSpan.getSpanId());
    }

    /**
     * Test Span creation of preparing and executing a query from trace context propagation.
     */
    @Test
    public void testSpanForPrepareAndExecuteFromContextPropagation()
    {
        Session session = sessionNet();

        String clientSpanName = "testSpanForPrepareAndExecuteFromContextPropagation";
        String query = String.format("SELECT * FROM %s.%s WHERE key = ?", KEYSPACE, OTEL_TEST_TABLE_NAME);
        Span span = otelTesting.getOpenTelemetry()
                               .getTracer("openTelemetryTracingTest")
                               .spanBuilder(clientSpanName)
                               .startSpan();
        try (Scope ignore = span.makeCurrent())
        {
            Map<String, ByteBuffer> payload = createTracingPayload();
            SimpleStatement simpleStatement = new SimpleStatement(query);
            simpleStatement.setOutgoingPayload(payload);
            PreparedStatement prepared = session.prepare(simpleStatement);

            BoundStatement bound = prepared.bind().setString(0, "a");
            bound.setOutgoingPayload(payload);
            session.execute(bound);
        }
        finally
        {
            span.end();
        }

        List<SpanData> spans = otelTesting.getSpans();

        // Should contain: client span, prepare span, execute span and replica span
        assertThat(spans).hasSize(4);

        // Spans are not guaranteed to be returned in order.
        Map<String, SpanData> spansByName = spans.stream().collect(Collectors.toMap(SpanData::getName, Function.identity()));

        SpanData replicaSpan = spansByName.get(String.format("READ_REQ %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData coordinatorPrepareSpan = spansByName.get(String.format("PREPARE SELECT %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData coordinatorExecuteSpan = spansByName.get(String.format("EXECUTE SELECT %s.%s", KEYSPACE, OTEL_TEST_TABLE_NAME));
        SpanData clientSpan = spansByName.get(clientSpanName);

        assertThat(replicaSpan).isNotNull();
        assertThat(coordinatorPrepareSpan).isNotNull();
        assertThat(coordinatorExecuteSpan).isNotNull();
        assertThat(clientSpan).isNotNull();

        // All spans should have the same trace ID
        assertThat(replicaSpan.getTraceId()).isEqualTo(clientSpan.getTraceId());
        assertThat(coordinatorPrepareSpan.getTraceId()).isEqualTo(clientSpan.getTraceId());
        assertThat(coordinatorExecuteSpan.getTraceId()).isEqualTo(clientSpan.getTraceId());
        // client span
        //   |-> coordinator prepare span
        //   |-> coordinator execute span
        //           |-> replica span
        assertThat(replicaSpan.getParentSpanId()).isEqualTo(coordinatorExecuteSpan.getSpanId());
        assertThat(coordinatorExecuteSpan.getParentSpanId()).isEqualTo(clientSpan.getSpanId());
        assertThat(coordinatorPrepareSpan.getParentSpanId()).isEqualTo(clientSpan.getSpanId());

        // Prepare and Execute spanes can contain parameterized query in attributes
        Map<AttributeKey<?>, Object> prepareSpanAttrs = coordinatorPrepareSpan.getAttributes().asMap();
        assertThat(prepareSpanAttrs).containsEntry(DbAttributes.DB_QUERY_TEXT, query);
        Map<AttributeKey<?>, Object> executeSpanAttrs = coordinatorExecuteSpan.getAttributes().asMap();
        assertThat(executeSpanAttrs).containsEntry(DbAttributes.DB_QUERY_TEXT, query);
    }

    private Map<String, ByteBuffer> createTracingPayload()
    {
        Map<String, ByteBuffer> payload = new HashMap<>();
        otelTesting.getOpenTelemetry()
                   .getPropagators()
                   .getTextMapPropagator()
                   .inject(Context.current(), payload, (carrier, key, value) ->
                   {
                       if (carrier != null)
                           carrier.put(key, ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
                   });
        return payload;
    }
}
