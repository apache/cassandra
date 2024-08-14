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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.config.YamlConfigurationLoader;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.uber.data.heatpipe.HeatpipeEncoder;
import com.uber.data.heatpipe.HeatpipeFactory;
import com.uber.stream.java.kafka.rest.client.KafkaMessage;
import com.uber.stream.java.kafka.rest.client.KafkaMessageProducer;
import com.uber.stream.java.kafka.rest.client.KafkaRestClientException;
import junit.framework.TestCase;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

public class KafkaHandlerTest extends TestCase
{

    private KafkaMessageProducer mockProducer;
    private HeatpipeFactory mockHeatpipeFactory;
    private HeatpipeEncoder mockEncoder;
    private KafkaHandler testHandler;

    static {
        DatabaseDescriptor.daemonInitialization();
        try
        {
            KafkaHandler.setup();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (KafkaRestClientException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setUp() throws IOException, KafkaRestClientException
    {
        mockProducer = Mockito.mock(KafkaMessageProducer.class);
        mockHeatpipeFactory = Mockito.mock(HeatpipeFactory.class);
        mockEncoder = Mockito.mock(HeatpipeEncoder.class);
        when(mockHeatpipeFactory.getLatestVersionNumberForTopic("hp-cstar-qan")).thenReturn(1);
        when(mockHeatpipeFactory.getHeatpipeEncoder("hp-cstar-qan", 1)).thenReturn(mockEncoder);

        testHandler = KafkaHandler.instance;
        testHandler.heatpipeFactory = mockHeatpipeFactory;
        testHandler.producer = mockProducer;
        testHandler.encoder = (mockEncoder);
    }

    @Test
    public void testSingletonInitialization()
    {
        KafkaHandler handler1 = KafkaHandler.instance;
        KafkaHandler handler2 = KafkaHandler.instance;

        assertSame(handler1, handler2);
        assertNotNull(handler1.producer);
        assertNotNull(handler1.encoder);
    }

    @Test
    public void testHeatPipeConfig()
    {
        Properties prop = KafkaHandler.getHeatpipeProperties();
        assertEquals(prop.get("heatpipe.app_id"), "hp-cstar-qan");
    }

    @Test
    public void testSendToKafka() throws IOException, KafkaRestClientException
    {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("table", "test");
        dataMap.put("nano_time", 1234312);
        dataMap.put("keyspace", "test");
        dataMap.put("token", "test");

        byte[] encodedMessage = new byte[]{ 1, 2, 3, 4 };
        when(mockEncoder.encode(dataMap)).thenReturn(encodedMessage);

        testHandler.sendToKafka(dataMap);

        verify(mockEncoder, times(1)).encode(dataMap);

        ArgumentCaptor<KafkaMessage> kafkaMessageCaptor = ArgumentCaptor.forClass(KafkaMessage.class);
        verify(mockProducer).produce(eq("hp-cstar-qan"), kafkaMessageCaptor.capture());
        Object kafkaMessage = kafkaMessageCaptor.getValue().getValue();
        assertEquals(encodedMessage, kafkaMessage);
    }

    @Test
    public void testConfigurationsTest()
    {
        Config config = new Config();

        Map<String, Object> map = ImmutableMap.<String, Object>builder().put("query_analytics.enabled", false).put("query_analytics.kafka_topic", "Test").put("query_analytics.logs_enabled", true).build();

        Config updated = YamlConfigurationLoader.updateFromMap(map, true, config);

        assert updated == config : "Config pointers do not match";
        assertFalse(config.query_analytics.isQueryAnalyticsEnabled().booleanValue());
        assertEquals(config.query_analytics.getKafkaTopic(), "Test");
        assertTrue(config.query_analytics.getLogsEnabled().booleanValue());
    }
}
