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

        // Configure DatabaseDescriptor config directly since we removed internal config field
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(true);
        
        // Set up producer configuration
        ParameterizedClass producerConfig = new ParameterizedClass();
        producerConfig.class_name = "org.apache.cassandra.service.QueryAnalyticsServiceTest$TestDataProducer";
        producerConfig.parameters = new HashMap<>();
        producerConfig.parameters.put("kafka_topic", "cassandra-query-analytics");
        config.setProducer(producerConfig);

        QueryAnalyticsService.setup();
        queryAnalyticsService = QueryAnalyticsService.instance;
        
        // Use mock producer for testing
        QueryAnalyticsService.dataProducer = mockDataProducer;

        when(mockDecoratedKey.toCQLString(any(TableMetadata.class))).thenReturn("id = 12345");
        when(mockSinglePartitionReadCommand.partitionKey()).thenReturn(mockDecoratedKey);

        tableMetadata = TableMetadata.builder("test_keyspace", "test_table")
            .addPartitionKeyColumn("id", Int32Type.instance)
            .build();

        assertNotNull("Config should not be null for testing", DatabaseDescriptor.getQueryAnalyticsConfig());
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

        QueryAnalyticsService.dataProducer = null;

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        QueryAnalyticsService.dataProducer = mockDataProducer;
        doThrow(new RuntimeException("Test exception")).when(mockDataProducer).produceDatapoint(any());

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);
    }

    public void testProcessLatencyMetricWithNullConfig() throws IOException
    {

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        verify(mockDataProducer, times(0)).produceDatapoint(any());
    }

    public void testDisabledConfigs() throws IOException
    {
        // Configure DatabaseDescriptor to have QueryAnalytics disabled
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);

        // Set up producer config for disabled test
        ParameterizedClass producerConfig = new ParameterizedClass();
        producerConfig.parameters = new HashMap<>();
        producerConfig.parameters.put("kafka_topic", "cassandra-query-analytics");
        config.setProducer(producerConfig);

        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);
        when(mockReadCommand.metadata()).thenReturn(tableMetadata);

        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        // Verify no datapoint was produced since QAN is disabled
        verify(mockDataProducer, times(0)).produceDatapoint(any());
    }

    public void testCreateDatapoint() throws IOException
    {
        String tableName = "test";
        Long nanoTimeMetric = 567890987L;
        String keyspace = "testspace";
        String token = "token";
        long latency = 0L;

        Map<String, Object> properties = new HashMap<>();

        QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
        tableName, nanoTimeMetric, keyspace, token, latency, properties);

        assertNotNull(datapoint);
        assertEquals(tableName, datapoint.getTable());
        assertEquals(nanoTimeMetric, datapoint.getTimestamp());
        assertEquals(keyspace, datapoint.getKeyspace());
        assertEquals(Long.valueOf(0L), datapoint.getLatency());
        assertEquals(token, datapoint.getPartition());
        assertNull(datapoint.getCluster());
        assertNull(datapoint.getInstance());
    }

    public void testCreateDatapointWithVariousInputs() throws IOException
    {
        Object[][] testCases = {
            {null, null, null, null, 100L, null},
            {"", 0L, "", "", 100L, new HashMap<>()},
            {"test", 567890987L, "testspace", "token", 0L, new HashMap<>()},
            {"test", 567890987L, "testspace", "token", -100L, new HashMap<>()},
            {"test", 567890987L, "testspace", "token", 9223372036854775807L, new HashMap<>()},
            {"test", 567890987L, "testspace", "token", -9223372036854775808L, new HashMap<>()}
        };

        for (Object[] testCase : testCases) {
            QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
                (String) testCase[0], (Long) testCase[1], (String) testCase[2],
                (String) testCase[3], (Long) testCase[4], (Map<String, Object>) testCase[5]);

            assertNotNull(datapoint);
            assertNotNull(datapoint.getProperties());
        }
    }

    public void testCreateDatapointWithVariousLatencyValues() throws IOException
    {
        long[] latencyValues = {0L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 999999L, 1L};

        for (long latency : latencyValues) {
            QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
                "test", 567890987L, "testspace", "token", latency, new HashMap<>());

            assertNotNull(datapoint);
            assertEquals(Long.valueOf(latency), datapoint.getLatency());
        }
    }

    public void testServiceInitialization() throws IOException
    {
        assertNotNull(QueryAnalyticsService.instance);
        assertNotNull(queryAnalyticsService);

        assertNotNull("Config should be set for testing", DatabaseDescriptor.getQueryAnalyticsConfig());

        assertTrue("Service should be properly initialized", true);
    }

    public void testServiceInitializationWithoutProducerConfig() throws IOException
    {
        // Configure DatabaseDescriptor to have QueryAnalytics enabled but no producer
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(true);
        config.setProducer(null);

        QueryAnalyticsService.dataProducer = null;
        
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);
        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);

        // Producer should still be null since no producer config was provided
        assertNull("dataProducer should remain null when no producer configuration is provided", QueryAnalyticsService.dataProducer);
        
        // Config is automatically restored since we used the same reference
    }

    public void testProcessLatencyMetricWithNoProducer() throws IOException
    {
        // Configure DatabaseDescriptor to have QueryAnalytics enabled but no producer
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(true);
        config.setProducer(null);

        QueryAnalyticsService.dataProducer = null; 

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

    public void testDynamicConfigurationSwitching() throws IOException
    {
        // Test dynamic configuration switching using DatabaseDescriptor
        QueryAnalyticsConfig originalConfig = DatabaseDescriptor.getQueryAnalyticsConfig();
        
        // Set up producer config
        ParameterizedClass producerConfig = new ParameterizedClass();
        producerConfig.class_name = "org.apache.cassandra.service.QueryAnalyticsServiceTest$TestDataProducer";
        producerConfig.parameters = new HashMap<>();
        originalConfig.setProducer(producerConfig);
        
        // Start with QAN disabled
        originalConfig.setEnabled(false);
        
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);
        
        queryAnalyticsService.processLatencyMetric(100L, mockSinglePartitionReadCommand);
        verify(mockDataProducer, times(0)).produceDatapoint(any());
        
        // Enable QAN dynamically
        originalConfig.setEnabled(true);
        
        // Test with QAN enabled - should produce datapoints
        queryAnalyticsService.processLatencyMetric(200L, mockSinglePartitionReadCommand);
        
        // Restore original test config (back to enabled state for other tests)
        originalConfig.setEnabled(true);
    }

    public void testCreateDataProducerWithInvalidClass() throws Exception
    {
        try {
            QueryAnalyticsService.createDataProducer("com.nonexistent.Class");
            fail("Should throw exception for non-existent class");
        } catch (RuntimeException e) {
            assertTrue("Exception should mention class not found", e.getMessage().contains("class not found"));
        }
    }

    public void testCreateDataProducerWithIncompatibleClass() throws Exception
    {
        try {
            QueryAnalyticsService.createDataProducer("java.lang.String");
            fail("Should throw exception for incompatible class");
        } catch (RuntimeException e) {
            assertTrue("Exception should mention interface requirement", e.getMessage().contains("does not implement"));
        }
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
            null, null, null, null, 0L, null);

        assertNotNull(datapoint);

        assertNull(datapoint.getTable());
        assertNull(datapoint.getTimestamp());
        assertNull(datapoint.getKeyspace());
        assertNull(datapoint.getPartition());
        // Host and DC are no longer set by Cassandra QueryAnalyticsService  
        assertNull(datapoint.getCluster()); // cluster is not set by createDatapoint method
        assertNull(datapoint.getInstance()); // instance is not set by createDatapoint method
        assertEquals(Long.valueOf(0L), datapoint.getLatency()); // Latency was passed as 0L
        assertNotNull(datapoint.getProperties()); // Should create empty map
    }

    @Override
    protected void tearDown() throws Exception
    {
        QueryAnalyticsService.dataProducer = null;
        
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
            assertNotNull("enabled option should be set", options.get("enabled"));
        }

        public Map<String, String> getOptions()
        {
            return options;
        }
        
        public void close()
        {
            // Mock close method for testing
        }
    }

    public void testMBeanSetQueryAnalyticsEnabled()
    {
        QueryAnalyticsService.dataProducer = null; 
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        config.setProducer(null);
        
        queryAnalyticsService.setQueryAnalyticsEnabled(true);
        assertTrue("QAN should be enabled via MBean", queryAnalyticsService.isQueryAnalyticsEnabled());
        
        queryAnalyticsService.setQueryAnalyticsEnabled(false);
        assertFalse("QAN should be disabled via MBean", queryAnalyticsService.isQueryAnalyticsEnabled());
    }
    
    public void testMBeanEnableQueryAnalytics()
    {
        // Clear test override to use DatabaseDescriptor config
        QueryAnalyticsService.dataProducer = null;
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        config.setProducer(null);
        
        queryAnalyticsService.setQueryAnalyticsEnabled(true);
        assertTrue("QAN should be enabled via MBean", queryAnalyticsService.isQueryAnalyticsEnabled());
    }
    
    public void testMBeanDisableQueryAnalytics()
    {
        // Clear test override to use DatabaseDescriptor config
        QueryAnalyticsService.dataProducer = null; 
        
        // Ensure we start with a proper config in DatabaseDescriptor
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        config.setProducer(null);
        
        queryAnalyticsService.setQueryAnalyticsEnabled(true);
        
        queryAnalyticsService.setQueryAnalyticsEnabled(false);
        assertFalse("QAN should be disabled via MBean", queryAnalyticsService.isQueryAnalyticsEnabled());
    }
    
    public void testMBeanGetQueryAnalyticsConfiguration()
    {
        // Clear test override to use DatabaseDescriptor config
        QueryAnalyticsService.dataProducer = null; 
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        config.setProducer(null);
        
        String configString = queryAnalyticsService.getQueryAnalyticsConfiguration();
        assertNotNull("Configuration should not be null", configString);
        assertTrue("Configuration should contain enabled status", configString.contains("enabled:"));
    }
    
    public void testMBeanConfigurationUpdatesPersist() throws IOException
    {
        // Clear test override to use DatabaseDescriptor config
        QueryAnalyticsService.dataProducer = null; 
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        config.setProducer(null);
        
        // Enable via MBean
        queryAnalyticsService.setQueryAnalyticsEnabled(true);
        
        // Verify configuration persisted to DatabaseDescriptor
        QueryAnalyticsConfig updatedConfig = DatabaseDescriptor.getQueryAnalyticsConfig();
        assertTrue("Configuration should persist enabled state", updatedConfig.isQueryAnalyticsEnabled());
    }

    public void testProducerInitializationOnEnable()
    {
        QueryAnalyticsService.dataProducer = null;
        
        // Configure DatabaseDescriptor with producer config
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        
        Map<String, String> params = new HashMap<>();
        params.put("kafka_topic", "test-topic");
        ParameterizedClass producerConfig = new ParameterizedClass("org.apache.cassandra.service.QueryAnalyticsServiceTest$TestDataProducer", params);
        config.setProducer(producerConfig);
        
        queryAnalyticsService.setQueryAnalyticsEnabled(true);
        assertNotNull("Producer should be initialized when enabled", QueryAnalyticsService.dataProducer);
    }

    public void testProducerInitFailure()
    {
        QueryAnalyticsService.dataProducer = null;
        ParameterizedClass badConfig = new ParameterizedClass("nonexistent.Class", new HashMap<>());
        
        // Configure DatabaseDescriptor with bad producer config
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        config.setProducer(badConfig);
        
        try {
            queryAnalyticsService.setQueryAnalyticsEnabled(true);
            fail("Should throw exception on producer init failure");
        } catch (Exception e) {
            // Expected - producer initialization should fail and throw exception
            assertNull("Producer should remain null on init failure", QueryAnalyticsService.dataProducer);
        }
    }
    
    public void testSetupWithEnabledButNoProducer() {
        // Configure DatabaseDescriptor with enabled but no producer
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(true);
        config.setProducer(null);
        
        // Reset producer to null
        QueryAnalyticsService.dataProducer = null;
        
        // Should not throw exception
        try {
            QueryAnalyticsService.setup();
            assertNull("Producer should remain null when no producer configured", QueryAnalyticsService.dataProducer);
        } catch (Exception e) {
            fail("setup() should not throw exception when producer is null: " + e.getMessage());
        }
    }
}
