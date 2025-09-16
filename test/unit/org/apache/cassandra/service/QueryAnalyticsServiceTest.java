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

package org.apache.cassandra.service;

import junit.framework.TestCase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.db.marshal.Int32Type;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ParameterizedClass;

import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;
import org.apache.cassandra.service.QueryAnalyticsDatapoint;
import java.lang.reflect.Method;

public class QueryAnalyticsServiceTest extends TestCase
{

    private QueryAnalyticsService queryAnalyticsService;

    @Mock
    private QueryAnalyticsConfig mockConfig;

    @Mock
    private ReadCommand mockReadCommand;

    @Mock
    private SinglePartitionReadCommand mockSinglePartitionReadCommand;

    private TableMetadata tableMetadata;

    @Mock
    private QueryAnalyticsDataProducer mockDataProducer;

    @Mock
    private DecoratedKey mockDecoratedKey;

    @Mock
    private Token mockToken;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        DatabaseDescriptor.daemonInitialization();

        when(mockConfig.isQueryAnalyticsEnabled()).thenReturn(true);
        when(mockConfig.getLogsEnabled()).thenReturn(true);

        // Mock the new producer configuration structure
        ParameterizedClass mockProducerConfig = new ParameterizedClass();
        mockProducerConfig.class_name = "org.apache.cassandra.service.QueryAnalyticsServiceTest$TestDataProducer";
        mockProducerConfig.parameters = new HashMap<>();
        mockProducerConfig.parameters.put("kafka_topic", "cassandra-query-analytics");
        when(mockConfig.getProducer()).thenReturn(mockProducerConfig);

        DatabaseDescriptor.setValueForConfig("query_analytics", mockConfig);

        QueryAnalyticsService.setup();
        queryAnalyticsService = QueryAnalyticsService.instance;

        queryAnalyticsService.dataProducer = mockDataProducer;

        when(mockDecoratedKey.toCQLString(any(TableMetadata.class))).thenReturn("id = 12345");
        when(mockSinglePartitionReadCommand.partitionKey()).thenReturn(mockDecoratedKey);

        tableMetadata = TableMetadata.builder("test_keyspace", "test_table")
            .addPartitionKeyColumn("id", Int32Type.instance)
            .build();

        assertNotNull("Config should not be null after setup", queryAnalyticsService.config);
    }

    public void testProcessLatencyMetricWithSinglePartitionReadCommand() throws IOException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        // Verify that the data producer was called with the correct datapoint
        ArgumentCaptor<QueryAnalyticsDatapoint> datapointCaptor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer, times(1)).produceDatapoint(datapointCaptor.capture());

        QueryAnalyticsDatapoint capturedDatapoint = datapointCaptor.getValue();
        assertNotNull(capturedDatapoint);
        assertEquals(Long.valueOf(100L), capturedDatapoint.getLatency()); // New field
        assertEquals("id = 12345", capturedDatapoint.getPartition()); // Partition key
        assertNotNull(capturedDatapoint.getTable());
        assertNotNull(capturedDatapoint.getKeyspace());
        assertNotNull(capturedDatapoint.getTimestamp());
    }

    public void testProcessLatencyMetricWithVariousInputs() throws IOException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        long[] latencies = {100L, 0L, -100L, 9223372036854775807L, -9223372036854775808L};
        for (long latency : latencies) {
            queryAnalyticsService.processLatencyMetric(latency, mockSinglePartitionReadCommand);
        }

        verify(mockDataProducer, times(latencies.length)).produceDatapoint(any());
    }

    public void testProcessLatencyMetricWithEdgeValues() throws IOException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        queryAnalyticsService.processLatencyMetric(0L, mockSinglePartitionReadCommand);
        queryAnalyticsService.processLatencyMetric(-1L, mockSinglePartitionReadCommand);
        queryAnalyticsService.processLatencyMetric(Long.MAX_VALUE, mockSinglePartitionReadCommand);

        verify(mockDataProducer, times(3)).produceDatapoint(any());
    }

    public void testProcessLatencyMetricWithVariousLatencyValues() throws IOException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        long[] latencyValues = {100L, 0L, 999999L, 1L, 50L, 200L};

        for (long latency : latencyValues) {
            queryAnalyticsService.processLatencyMetric(latency, mockSinglePartitionReadCommand);
        }

        verify(mockDataProducer, times(latencyValues.length)).produceDatapoint(any());
    }

    public void testProcessLatencyMetricWithErrorConditions() throws IOException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        queryAnalyticsService.dataProducer = null;

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        queryAnalyticsService.dataProducer = mockDataProducer;
        doThrow(new RuntimeException("Test exception")).when(mockDataProducer).produceDatapoint(any());

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);
    }

    public void testProcessLatencyMetricWithNullConfig() throws IOException
    {

        queryAnalyticsService.config = null;

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);


        verify(mockDataProducer, times(0)).produceDatapoint(any());
    }

    public void testDisabledConfigs() throws IOException
    {
        DatabaseDescriptor.setValueForConfig("query_analytics", mockConfig);
        when(mockConfig.isQueryAnalyticsEnabled()).thenReturn(false);
        when(mockConfig.getLogsEnabled()).thenReturn(false);

        // Update producer config for disabled test
        ParameterizedClass mockProducerConfig = new ParameterizedClass();
        mockProducerConfig.parameters = new HashMap<>();
        mockProducerConfig.parameters.put("kafka_topic", "cassandra-query-analytics");
        when(mockConfig.getProducer()).thenReturn(mockProducerConfig);

        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);
        when(mockReadCommand.metadata()).thenReturn(tableMetadata);

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        verify(mockConfig, times(2)).isQueryAnalyticsEnabled();
        verify(mockDataProducer, times(0)).produceDatapoint(any());
    }

    public void testCreateDatapoint() throws IOException
    {
        String tableName = "test";
        Long nanoTimeMetric = 567890987L;
        String keyspace = "testspace";
        String token = "token";
        long latency = 0L;
        String hostName = "host-123";
        String DC = "test";

        Map<String, Object> properties = new HashMap<>();

        QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
        tableName, nanoTimeMetric, keyspace, token, latency, DC, hostName, properties);

        assertNotNull(datapoint);
        assertEquals(tableName, datapoint.getTable());
        assertEquals(nanoTimeMetric, datapoint.getTimestamp());
        assertEquals(keyspace, datapoint.getKeyspace());
        assertEquals(Long.valueOf(0L), datapoint.getLatency());
        assertEquals(token, datapoint.getPartition());
        assertEquals(hostName, datapoint.getHost());
        assertEquals(DC, datapoint.getDC());
        // instance and cluster are not set by createDatapoint method
        assertNull(datapoint.getCluster());
        assertNull(datapoint.getInstance());
    }

    public void testCreateDatapointWithVariousInputs() throws IOException
    {
        Object[][] testCases = {
            {null, null, null, null, 100L, null, null, null},
            {"", 0L, "", "", 100L, "", "", new HashMap<>()},
            {"test", 567890987L, "testspace", "token", 0L, "test", "host-123", new HashMap<>()},
            {"test", 567890987L, "testspace", "token", -100L, "test", "host-123", new HashMap<>()},
            {"test", 567890987L, "testspace", "token", 9223372036854775807L, "test", "host-123", new HashMap<>()},
            {"test", 567890987L, "testspace", "token", -9223372036854775808L, "test", "host-123", new HashMap<>()}
        };

        for (Object[] testCase : testCases) {
            QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
                (String) testCase[0], (Long) testCase[1], (String) testCase[2],
                (String) testCase[3], (Long) testCase[4], (String) testCase[5],
                (String) testCase[6], (Map<String, Object>) testCase[7]);

            assertNotNull(datapoint);
            assertNotNull(datapoint.getProperties());
        }
    }

    public void testCreateDatapointWithVariousLatencyValues() throws IOException
    {
        long[] latencyValues = {0L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 999999L, 1L};

        for (long latency : latencyValues) {
            QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
                "test", 567890987L, "testspace", "token", latency, "test", "host-123", new HashMap<>());

            assertNotNull(datapoint);
            assertEquals(Long.valueOf(latency), datapoint.getLatency());
        }
    }

    public void testServiceInitialization() throws IOException
    {
        assertNotNull(QueryAnalyticsService.instance);
        assertNotNull(queryAnalyticsService);

        assertNotNull(queryAnalyticsService.config);

        if (queryAnalyticsService.config.getProducer() != null && queryAnalyticsService.config.getProducer().class_name != null) {
            assertNotNull("dataProducer should be created when producer config is provided", queryAnalyticsService.dataProducer);
        } else {
            assertNull("dataProducer should be null when no producer config is provided", queryAnalyticsService.dataProducer);
        }
    }

    public void testServiceInitializationWithoutProducerConfig() throws IOException
    {
        QueryAnalyticsConfig mockConfigNoProducer = mock(QueryAnalyticsConfig.class);
        when(mockConfigNoProducer.isQueryAnalyticsEnabled()).thenReturn(true);
        when(mockConfigNoProducer.getLogsEnabled()).thenReturn(true);
        when(mockConfigNoProducer.getProducer()).thenReturn(null);

        DatabaseDescriptor.setValueForConfig("query_analytics", mockConfigNoProducer);

        QueryAnalyticsService newService = new QueryAnalyticsService();
        newService.config = mockConfigNoProducer;

        assertNull("dataProducer should be null when no producer configuration is provided", newService.dataProducer);
    }

    public void testProcessLatencyMetricWithNoProducer() throws IOException
    {
        // Test that the service gracefully handles requests when no producer is configured
        QueryAnalyticsConfig mockConfigNoProducer = mock(QueryAnalyticsConfig.class);
        when(mockConfigNoProducer.isQueryAnalyticsEnabled()).thenReturn(true);
        when(mockConfigNoProducer.getLogsEnabled()).thenReturn(true);
        when(mockConfigNoProducer.getProducer()).thenReturn(null);

        // Set the config for this test
        queryAnalyticsService.config = mockConfigNoProducer;
        queryAnalyticsService.dataProducer = null; // Simulate no producer

        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        // This should not throw an exception even with no producer
        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        // Verify that no datapoint was produced (since there's no producer)
        verify(mockDataProducer, never()).produceDatapoint(any());
    }

    public void testProcessLatencyMetricWithValidCommand() throws IOException
    {

        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        ArgumentCaptor<QueryAnalyticsDatapoint> datapointCaptor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer, times(1)).produceDatapoint(datapointCaptor.capture());

        QueryAnalyticsDatapoint capturedDatapoint = datapointCaptor.getValue();
        assertNotNull(capturedDatapoint);
    }


    public void testCreateDataProducerWithInvalidClass() throws Exception
    {
        QueryAnalyticsDataProducer producer = QueryAnalyticsService.createDataProducer("com.nonexistent.Class");
        assertNull("Producer should be null when class doesn't exist", producer);
    }

    public void testCreateDataProducerWithIncompatibleClass() throws Exception
    {

        QueryAnalyticsDataProducer producer = QueryAnalyticsService.createDataProducer("java.lang.String");
        assertNull("Producer should be null when class doesn't implement interface", producer);
    }

    public void testCreateDataProducerWithValidClass() throws Exception
    {

        QueryAnalyticsDataProducer producer = QueryAnalyticsService.createDataProducer("org.apache.cassandra.service.QueryAnalyticsServiceTest$TestDataProducer");
        assertNotNull("Producer should be created for valid class", producer);
        assertTrue("Producer should be instance of TestDataProducer", producer instanceof TestDataProducer);
    }

    public void testCreateDatapointWithAllNullInputs() throws IOException
    {
        QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
            null, null, null, null, 0L, null, null, null);

        assertNotNull(datapoint);

        assertNull(datapoint.getTable());
        assertNull(datapoint.getTimestamp());
        assertNull(datapoint.getKeyspace());
        assertNull(datapoint.getPartition());
        assertNull(datapoint.getHost());
        assertNull(datapoint.getDC());
        assertNull(datapoint.getCluster()); // cluster is not set by createDatapoint method
        assertNull(datapoint.getInstance()); // instance is not set by createDatapoint method
        assertEquals(Long.valueOf(0L), datapoint.getLatency()); // Latency was passed as 0L
        assertNotNull(datapoint.getProperties()); // Should create empty map
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        reset(mockReadCommand, mockSinglePartitionReadCommand, mockDataProducer);
    }

    // Test implementation of QueryAnalyticsDataProducer for testing
    public static class TestDataProducer implements QueryAnalyticsDataProducer
    {
        private final Map<String, String> options;

        public TestDataProducer(Map<String, String> options)
        {
            this.options = options;
        }

        @Override
        public void produceDatapoint(QueryAnalyticsDatapoint datapoint)
        {
            assertNotNull("Options should not be null", options);
            assertEquals("cassandra-query-analytics", options.get("kafka_topic"));
            assertEquals("true", options.get("enabled"));
            assertEquals("true", options.get("logs_enabled"));
        }

        public Map<String, String> getOptions()
        {
            return options;
        }
    }
}
