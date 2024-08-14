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

import com.uber.data.heatpipe.HeatpipeEncoder;
import com.uber.data.heatpipe.HeatpipeFactory;
import com.uber.stream.java.kafka.rest.client.KafkaMessage;
import com.uber.stream.java.kafka.rest.client.KafkaMessageProducer;
import com.uber.stream.java.kafka.rest.client.KafkaRestClientException;
import junit.framework.TestCase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.schema.TableMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.*;

public class QueryAnalyticsServiceTest extends TestCase
{

    private QueryAnalyticsService queryAnalyticsService;

    @Mock
    private QueryAnalyticsConfig mockConfig;

    @Mock
    private ReadCommand mockReadCommand;

    @Mock
    private SinglePartitionReadCommand mockSinglePartitionReadCommand;

    @Mock
    private TableMetadata mockTableMetadata;

    @Mock
    private HeatpipeFactory mockHeatpipeFactory;

    @Mock
    private HeatpipeEncoder mockHeatpipeEncoder;

    @Mock
    private KafkaMessageProducer mockProducer;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setValueForConfig("query_analytics", mockConfig);
        when(mockConfig.isQueryAnalyticsEnabled()).thenReturn(true);
        when(mockConfig.getKafkaTopic()).thenReturn("hp-cstar-qan");
        when(mockConfig.getLogsEnabled()).thenReturn(true);
        QueryAnalyticsService.setup();

        queryAnalyticsService = QueryAnalyticsService.instance;
        KafkaHandler.instance.heatpipeFactory = mockHeatpipeFactory;
        KafkaHandler.instance.producer = mockProducer;
        KafkaHandler.instance.encoder = mockHeatpipeEncoder;
    }

    public void testProcessLatencyMetricWithSinglePartitionReadCommand() throws IOException, KafkaRestClientException
    {
        when(mockSinglePartitionReadCommand.metadata()).thenReturn(mockTableMetadata);

        byte[] encodedMessage = new byte[]{ 1, 2, 3, 4 };
        when(mockHeatpipeEncoder.encode(anyMap())).thenReturn(encodedMessage);

        queryAnalyticsService.processLatencyMetric("metric1", "100", mockSinglePartitionReadCommand);

        assertNotNull(queryAnalyticsService.config);
        verify(mockHeatpipeEncoder, times(1)).encode(anyMap());

        ArgumentCaptor<KafkaMessage> kafkaMessageCaptor = ArgumentCaptor.forClass(KafkaMessage.class);
        verify(mockProducer).produce(eq("hp-cstar-qan"), kafkaMessageCaptor.capture());
        Object kafkaMessage = kafkaMessageCaptor.getValue().getValue();
        assertEquals(encodedMessage, kafkaMessage);
    }

    public void testCreateDataMap()
    {
        String tableName = "test";
        Long nanoTimeMetric = 567890987L;
        String keyspace = "testspace";
        String metricName = "test";
        String token = "token";
        String value = "0";
        String hostName = "host-123";
        String cName = "clusterName";
        String DC = "test";

        Map<String, Object> dataMap = QueryAnalyticsService.createDataMap(
        tableName, nanoTimeMetric, keyspace, metricName, token, value, DC, hostName, cName);

        assertNotNull(dataMap);
        assertEquals(tableName, dataMap.get("table"));
        assertEquals(nanoTimeMetric, dataMap.get("nano_time"));
        assertEquals(keyspace, dataMap.get("keyspace"));
        assertNull(dataMap.get("node"));
        assertEquals(metricName, dataMap.get("name"));
        assertEquals(value, dataMap.get("value"));
        assertEquals(token, dataMap.get("token"));
        assertEquals(hostName, dataMap.get("host"));
        assertEquals(DC, dataMap.get("dc"));
        assertEquals(cName, dataMap.get("cluster"));
    }

    public void testDisabledConfigs() throws IOException, KafkaRestClientException
    {
        DatabaseDescriptor.setValueForConfig("query_analytics", mockConfig);
        when(mockConfig.isQueryAnalyticsEnabled()).thenReturn(false);
        when(mockConfig.getKafkaTopic()).thenReturn("hp-cstar-qan");
        when(mockConfig.getLogsEnabled()).thenReturn(false);

        when(mockSinglePartitionReadCommand.metadata()).thenReturn(mockTableMetadata);

        when(mockReadCommand.metadata()).thenReturn(mockTableMetadata);

        byte[] encodedMessage = new byte[]{ 1, 2, 3, 4 };
        when(mockHeatpipeEncoder.encode(anyMap())).thenReturn(encodedMessage);

        queryAnalyticsService.processLatencyMetric("metric1", "100", mockSinglePartitionReadCommand);

        verify(mockConfig, times(1)).isQueryAnalyticsEnabled();
        verify(mockHeatpipeEncoder, times(0)).encode(anyMap());
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        reset(mockReadCommand, mockSinglePartitionReadCommand);
    }
}
