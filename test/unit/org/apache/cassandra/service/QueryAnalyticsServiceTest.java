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
import org.apache.cassandra.db.ReadResponse;
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
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.range.SingleRangeResponse;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.locator.InetAddressAndPort;

public class QueryAnalyticsServiceTest extends TestCase
{

    private QueryAnalyticsService queryAnalyticsService;


    @Mock
    private ReadCommand mockReadCommand;

    @Mock
    private SinglePartitionReadCommand mockSinglePartitionReadCommand;

    @Mock
    private ReadResponse mockReadResponse;

    private TableMetadata tableMetadata;

    @Mock
    private QueryAnalyticsDataProducer mockDataProducer;

    @Mock
    private DecoratedKey mockDecoratedKey;

    @Mock
    private Token mockToken;

    @Mock
    private PartitionRangeReadCommand mockRangeReadCommand;

    @Mock
    private DataRange mockDataRange;

    @Mock
    private AbstractBounds mockKeyRange;

    @Mock
    private DataResolver mockDataResolver;


    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        DatabaseDescriptor.daemonInitialization();

        // Configure DatabaseDescriptor config directly since we removed internal config field
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(true);
        
        // Set sampling ratio to 1.0 for deterministic test behavior (specific sampling tests override this)
        config.setSamplingRatio(1.0);
        
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

        queryAnalyticsService.processSinglePartitionReadMetric(100L, mockSinglePartitionReadCommand, mockReadResponse);

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
        
        // Verify payload sizes are present (actual values depend on serialization)
        assertNotNull("request_payload_size should be set", capturedDatapoint.getProperty("request_payload_size"));
        assertNotNull("response_payload_size should be set", capturedDatapoint.getProperty("response_payload_size"));
        
        // Verify query_type is always present
        assertEquals("query_type should be 'read' for single partition reads", "read", capturedDatapoint.getProperty("query_type"));
    }


    public void testQANDisabledScenarios() throws IOException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);

        // QAN disabled
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        queryAnalyticsService.processSinglePartitionReadMetric(100L, mockSinglePartitionReadCommand, mockReadResponse);
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        // Null checks for single partition read
        reset(mockDataProducer);
        config.setEnabled(true);
        queryAnalyticsService.processSinglePartitionReadMetric(100L, null, mockReadResponse);
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        reset(mockDataProducer);
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(null);
        queryAnalyticsService.processSinglePartitionReadMetric(100L, mockSinglePartitionReadCommand, mockReadResponse);
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        reset(mockDataProducer);
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);
        when(mockSinglePartitionReadCommand.partitionKey()).thenReturn(null);
        queryAnalyticsService.processSinglePartitionReadMetric(100L, mockSinglePartitionReadCommand, mockReadResponse);
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        // Null producer
        reset(mockDataProducer);
        when(mockSinglePartitionReadCommand.partitionKey()).thenReturn(mockDecoratedKey);
        QueryAnalyticsService.dataProducer = null;
        queryAnalyticsService.processSinglePartitionReadMetric(100L, mockSinglePartitionReadCommand, mockReadResponse);
        verify(mockDataProducer, never()).produceDatapoint(any());
        QueryAnalyticsService.dataProducer = mockDataProducer;
    }

    public void testCreateDatapoint() throws IOException
    {
        String tableName = "test";
        Long nanoTimeMetric = 567890987L;
        String keyspace = "testspace";
        String token = "token";

        // With properties
        Map<String, Object> properties = new HashMap<>();
        QueryAnalyticsDatapoint datapoint = QueryAnalyticsService.createDatapoint(
            tableName, nanoTimeMetric, keyspace, token, 0L, properties);
        assertNotNull(datapoint);
        assertEquals(tableName, datapoint.getTable());
        assertEquals(nanoTimeMetric, datapoint.getTimestamp());
        assertEquals(keyspace, datapoint.getKeyspace());
        assertEquals(Long.valueOf(0L), datapoint.getLatency());
        assertEquals(token, datapoint.getPartition());
        
        // With null properties
        datapoint = QueryAnalyticsService.createDatapoint(tableName, 100L, keyspace, token, 50L, null);
        assertNotNull(datapoint);
        assertEquals(Long.valueOf(50L), datapoint.getLatency());
    }



    public void testCreateDataProducer() throws Exception
    {
        // Invalid class
        try {
            QueryAnalyticsService.createDataProducer("com.nonexistent.Class");
            fail("Should throw for non-existent class");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("class not found"));
        }
        
        // Null/empty/whitespace class name
        assertNull(QueryAnalyticsService.createDataProducer(null));
        assertNull(QueryAnalyticsService.createDataProducer(""));
        assertNull(QueryAnalyticsService.createDataProducer("   "));
        
        // Incompatible class
        try {
            QueryAnalyticsService.createDataProducer("java.lang.String");
            fail("Should throw for incompatible class");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("does not implement"));
        }
        
        // Valid class
        QueryAnalyticsDataProducer producer = QueryAnalyticsService.createDataProducer(
            "org.apache.cassandra.service.QueryAnalyticsServiceTest$TestDataProducer");
        assertNotNull(producer);
    }

    @Override
    protected void tearDown() throws Exception
    {
        QueryAnalyticsService.dataProducer = null;
        
        super.tearDown();
        reset(mockReadCommand, mockSinglePartitionReadCommand, mockReadResponse, mockDataProducer);
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

    public void testMBeanOperations()
    {
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        
        // Test enable/disable
        config.setEnabled(false);
        queryAnalyticsService.setQueryAnalyticsEnabled(true);
        assertTrue(queryAnalyticsService.isQueryAnalyticsEnabled());
        
        queryAnalyticsService.setQueryAnalyticsEnabled(false);
        assertFalse(queryAnalyticsService.isQueryAnalyticsEnabled());
        
        // Test config string
        String configString = queryAnalyticsService.getQueryAnalyticsConfiguration();
        assertNotNull(configString);
        assertTrue(configString.contains("enabled:"));
        assertTrue(configString.contains("sampling_ratio:"));
        assertTrue(configString.contains("producer:"));
        
        // Test producer initialization on enable
        QueryAnalyticsService.dataProducer = null;
        queryAnalyticsService.setQueryAnalyticsEnabled(true);
        assertNotNull(QueryAnalyticsService.dataProducer);
        
        // Restore for other tests
        QueryAnalyticsService.dataProducer = mockDataProducer;
        config.setEnabled(true);
    }
    
    public void testSamplingRatioConfiguration()
    {
        // Test valid values 
        queryAnalyticsService.setQueryAnalyticsSamplingRatio(0.5);
        assertEquals(0.5, queryAnalyticsService.getQueryAnalyticsSamplingRatio(), 0.001);
        
        // Test invalid values throw exceptions
        try {
            DatabaseDescriptor.getQueryAnalyticsConfig().setSamplingRatio(-0.1);
            fail("Should throw for negative ratio");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("between 0.0 and 1.0"));
        }
    }
    
    public void testSamplingBehavior() throws IOException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(tableMetadata);
        
        // Test 0.0 sampling
        DatabaseDescriptor.getQueryAnalyticsConfig().setSamplingRatio(0.0);
        reset(mockDataProducer);
        for (int i = 0; i < 10; i++) {
            queryAnalyticsService.processSinglePartitionReadMetric(100L, mockSinglePartitionReadCommand, mockReadResponse);
        }
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        // Test 1.0 sampling
        DatabaseDescriptor.getQueryAnalyticsConfig().setSamplingRatio(1.0);
        reset(mockDataProducer);
        for (int i = 0; i < 10; i++) {
            queryAnalyticsService.processSinglePartitionReadMetric(100L, mockSinglePartitionReadCommand, mockReadResponse);
        }
        verify(mockDataProducer, times(10)).produceDatapoint(any());
    }
    
    public void testProcessWriteMetricSinglePartition() throws IOException
    {
        IMutation mockMutation = mock(IMutation.class);
        when(mockMutation.key()).thenReturn(mockDecoratedKey);
        
        // Test INSERT
        queryAnalyticsService.processWriteMetric(100L, StatementType.INSERT, tableMetadata, 
                                                 java.util.Collections.singletonList(mockMutation));
        ArgumentCaptor<QueryAnalyticsDatapoint> captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        QueryAnalyticsDatapoint dp = captor.getValue();
        assertEquals("insert", dp.getProperty("query_type"));
        assertEquals(Long.valueOf(0L), dp.getProperty("response_payload_size"));
        assertEquals("id = 12345", dp.getPartition());
        assertEquals(Long.valueOf(100L), dp.getLatency());
        
        // Test UPDATE and DELETE
        reset(mockDataProducer);
        queryAnalyticsService.processWriteMetric(100L, StatementType.UPDATE, tableMetadata, 
                                                 java.util.Collections.singletonList(mockMutation));
        captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        assertEquals("update", captor.getValue().getProperty("query_type"));
        
        reset(mockDataProducer);
        queryAnalyticsService.processWriteMetric(100L, StatementType.DELETE, tableMetadata, 
                                                 java.util.Collections.singletonList(mockMutation));
        captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        assertEquals("delete", captor.getValue().getProperty("query_type"));
        
        // Null/empty checks
        reset(mockDataProducer);
        queryAnalyticsService.processWriteMetric(100L, StatementType.INSERT, tableMetadata, null);
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        reset(mockDataProducer);
        queryAnalyticsService.processWriteMetric(100L, StatementType.INSERT, tableMetadata, 
                                                 java.util.Collections.emptyList());
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        reset(mockDataProducer);
        queryAnalyticsService.processWriteMetric(100L, StatementType.INSERT, null, 
                                                 java.util.Collections.singletonList(mockMutation));
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        // QAN disabled
        reset(mockDataProducer);
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        queryAnalyticsService.processWriteMetric(100L, StatementType.INSERT, tableMetadata, 
                                                 java.util.Collections.singletonList(mockMutation));
        verify(mockDataProducer, never()).produceDatapoint(any());
        config.setEnabled(true);
        
        // Sampling disabled
        reset(mockDataProducer);
        DatabaseDescriptor.getQueryAnalyticsConfig().setSamplingRatio(0.0);
        queryAnalyticsService.processWriteMetric(100L, StatementType.INSERT, tableMetadata, 
                                                 java.util.Collections.singletonList(mockMutation));
        verify(mockDataProducer, never()).produceDatapoint(any());
        DatabaseDescriptor.getQueryAnalyticsConfig().setSamplingRatio(1.0);
        
        // Exception handling
        reset(mockDataProducer);
        IMutation badMutation = mock(IMutation.class);
        when(badMutation.key()).thenThrow(new RuntimeException("Test exception"));
        queryAnalyticsService.processWriteMetric(100L, StatementType.INSERT, tableMetadata, 
                                                 java.util.Collections.singletonList(badMutation));
        verify(mockDataProducer, never()).produceDatapoint(any());
    }
    
    public void testProcessWriteMetricMultiplePartitions() throws IOException
    {
        java.util.List<IMutation> mutations = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            IMutation m = mock(IMutation.class);
            DecoratedKey k = mock(DecoratedKey.class);
            when(k.toCQLString(any())).thenReturn("id = 'key" + i + "'");
            when(m.key()).thenReturn(k);
            mutations.add(m);
        }
        
        queryAnalyticsService.processWriteMetric(300L, StatementType.INSERT, tableMetadata, mutations);
        
        ArgumentCaptor<QueryAnalyticsDatapoint> captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer, times(3)).produceDatapoint(captor.capture());
        
        java.util.List<QueryAnalyticsDatapoint> dps = captor.getAllValues();
        assertEquals(3, dps.size());
        
        // Verify latency divided (300 / 3 = 100) and partition keys
        for (int i = 0; i < 3; i++) {
            assertEquals(Long.valueOf(100L), dps.get(i).getLatency());
            assertEquals("id = 'key" + i + "'", dps.get(i).getPartition());
            assertEquals("insert", dps.get(i).getProperty("query_type"));
        }
    }
    
    
    // Helper to setup range read command mocks
    private void setupRangeReadMocks(String rangeString) {
        when(mockRangeReadCommand.metadata()).thenReturn(tableMetadata);
        when(mockRangeReadCommand.dataRange()).thenReturn(mockDataRange);
        when(mockDataRange.keyRange()).thenReturn(mockKeyRange);
        when(mockKeyRange.getString(any())).thenReturn(rangeString);
    }
    
    public void testProcessRangeReadMetric() throws IOException
    {
        setupRangeReadMocks("(token1, token2]");
        
        queryAnalyticsService.processRangeReadMetric(200L, mockRangeReadCommand);
        
        ArgumentCaptor<QueryAnalyticsDatapoint> captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        
        QueryAnalyticsDatapoint dp = captor.getValue();
        assertEquals("range", dp.getProperty("query_type"));
        assertEquals("(token1, token2]", dp.getPartition());
        assertEquals(Long.valueOf(200L), dp.getLatency());
        assertEquals(Long.valueOf(0L), dp.getProperty("response_payload_size")); // No resolvers
    }
    
    public void testProcessRangeReadMetricNullChecks() throws IOException
    {
        // Null command
        queryAnalyticsService.processRangeReadMetric(100L, null);
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        // Null metadata
        reset(mockDataProducer);
        when(mockRangeReadCommand.metadata()).thenReturn(null);
        queryAnalyticsService.processRangeReadMetric(100L, mockRangeReadCommand);
        verify(mockDataProducer, never()).produceDatapoint(any());
        
        // QAN disabled
        reset(mockDataProducer);
        setupRangeReadMocks("(token1, token2]");
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        config.setEnabled(false);
        queryAnalyticsService.processRangeReadMetric(100L, mockRangeReadCommand);
        verify(mockDataProducer, never()).produceDatapoint(any());
        config.setEnabled(true);
    }
    
    public void testRangeReadResponseSizeCalculation() throws IOException
    {
        setupRangeReadMocks("(token1, token2]");
        
        // Empty/null range responses
        queryAnalyticsService.processRangeReadMetric(400L, mockRangeReadCommand, 
                                                     java.util.Collections.emptyList());
        ArgumentCaptor<QueryAnalyticsDatapoint> captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        assertEquals(Long.valueOf(0L), captor.getValue().getProperty("response_payload_size"));
        
        // Non-digest response
        reset(mockDataProducer);
        when(mockReadResponse.isDigestResponse()).thenReturn(false);
        InetAddressAndPort testAddress = InetAddressAndPort.getLocalHost();
        Message<ReadResponse> nonDigestMessage = Message.builder(Verb.READ_RSP, mockReadResponse)
                                                         .from(testAddress)
                                                         .build();
        when(mockDataResolver.getResponses()).thenReturn(java.util.Collections.singletonList(nonDigestMessage));
        @SuppressWarnings("unchecked")
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver = 
            (DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead>) mockDataResolver;
        SingleRangeResponse rangeResponse = mock(SingleRangeResponse.class);
        when(rangeResponse.getResolver()).thenReturn(resolver);
        java.util.List<SingleRangeResponse> rangeResponses = java.util.Collections.singletonList(rangeResponse);
        queryAnalyticsService.processRangeReadMetric(400L, mockRangeReadCommand, rangeResponses);
        captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        assertNotNull(captor.getValue().getProperty("response_payload_size"));
        
        // Digest response (skipped)
        reset(mockDataProducer);
        when(mockReadResponse.isDigestResponse()).thenReturn(true);
        Message<ReadResponse> digestMessage = Message.builder(Verb.READ_RSP, mockReadResponse)
                                                      .from(testAddress)
                                                      .build();
        when(mockDataResolver.getResponses()).thenReturn(java.util.Collections.singletonList(digestMessage));
        queryAnalyticsService.processRangeReadMetric(400L, mockRangeReadCommand, rangeResponses);
        captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        assertEquals(Long.valueOf(0L), captor.getValue().getProperty("response_payload_size"));
        
        // Exception handling
        reset(mockDataProducer);
        when(mockDataResolver.getResponses()).thenThrow(new RuntimeException("Test exception"));
        queryAnalyticsService.processRangeReadMetric(400L, mockRangeReadCommand, rangeResponses);
        captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        assertEquals(Long.valueOf(0L), captor.getValue().getProperty("response_payload_size"));
    }
    
    public void testRangeReadWithResolverExtraction() throws IOException
    {
        // Test the code path that extracts resolvers from SingleRangeResponse objects
        setupRangeReadMocks("(token1, token2]");
        
        // Create SingleRangeResponse objects with resolvers (mimicking RangeCommandIterator)
        @SuppressWarnings("unchecked")
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver1 = 
            mock(DataResolver.class);
        @SuppressWarnings("unchecked")
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver2 = 
            mock(DataResolver.class);
        
        when(mockReadResponse.isDigestResponse()).thenReturn(false);
        InetAddressAndPort testAddress = InetAddressAndPort.getLocalHost();
        Message<ReadResponse> message1 = Message.builder(Verb.READ_RSP, mockReadResponse).from(testAddress).build();
        Message<ReadResponse> message2 = Message.builder(Verb.READ_RSP, mockReadResponse).from(testAddress).build();
        
        when(resolver1.getResponses()).thenReturn(java.util.Collections.singletonList(message1));
        when(resolver2.getResponses()).thenReturn(java.util.Collections.singletonList(message2));
        
        // Create SingleRangeResponse objects (mimicking RangeCommandIterator's allRangeResponses)
        SingleRangeResponse rangeResponse1 = mock(SingleRangeResponse.class);
        SingleRangeResponse rangeResponse2 = mock(SingleRangeResponse.class);
        when(rangeResponse1.getResolver()).thenReturn(resolver1);
        when(rangeResponse2.getResolver()).thenReturn(resolver2);
        
        java.util.List<SingleRangeResponse> rangeResponses = new java.util.ArrayList<>();
        rangeResponses.add(rangeResponse1);
        rangeResponses.add(rangeResponse2);
        
        queryAnalyticsService.processRangeReadMetric(400L, mockRangeReadCommand, rangeResponses);
        ArgumentCaptor<QueryAnalyticsDatapoint> captor = ArgumentCaptor.forClass(QueryAnalyticsDatapoint.class);
        verify(mockDataProducer).produceDatapoint(captor.capture());
        assertNotNull(captor.getValue().getProperty("response_payload_size"));
    }
}
