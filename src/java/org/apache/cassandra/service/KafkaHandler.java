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

import com.uber.data.heatpipe.configuration.Heatpipe4JConfig;
import com.uber.data.heatpipe.configuration.PropertiesHeatpipeConfiguration;
import com.uber.stream.java.kafka.rest.client.KafkaMessage;
import com.uber.stream.java.kafka.rest.client.KafkaMessageProducer;
import com.uber.stream.java.kafka.rest.client.KafkaProducerFactory;
import com.uber.stream.java.kafka.rest.client.KafkaRestClientException;
import com.uber.data.heatpipe.HeatpipeEncoder;
import com.uber.data.heatpipe.HeatpipeFactory;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaHandler
{
    public static final KafkaHandler instance = new KafkaHandler();
    private KafkaProducerFactory factory;
    @VisibleForTesting
    protected KafkaMessageProducer producer;
    private static final Logger logger = LoggerFactory.getLogger(KafkaHandler.class);
    @VisibleForTesting
    protected QueryAnalyticsConfig config;
    @VisibleForTesting
    protected HeatpipeFactory heatpipeFactory;
    @VisibleForTesting
    protected HeatpipeEncoder encoder;

    public static void setup() throws IOException, KafkaRestClientException
    {
        instance.config = DatabaseDescriptor.getQueryAnalyticsConfig();

        logger.debug("KafkaClient is created for topic {} ", instance.config.getKafkaTopic());

        instance.factory = new KafkaProducerFactory();

        Properties props = getHeatpipeProperties();
        Heatpipe4JConfig config = new PropertiesHeatpipeConfiguration(props);

        instance.heatpipeFactory = new HeatpipeFactory(config);
        instance.producer = instance.factory.getProducer();
        instance.encoder = instance.heatpipeFactory.getHeatpipeEncoder(
        instance.config.getKafkaTopic(), instance.heatpipeFactory.getLatestVersionNumberForTopic(instance.config.getKafkaTopic()));
    }

    @VisibleForTesting
    protected KafkaHandler()
    {
    }

    @VisibleForTesting
    protected static Properties getHeatpipeProperties()
    {
        Properties props = new Properties();
        props.setProperty("heatpipe.app_id", instance.config.getKafkaTopic());
        return props;
    }

    public void sendToKafka(Map<String, Object> data) throws IOException, KafkaRestClientException
    {
        logger.debug("Sending data to topic: {}", config.getKafkaTopic());

        byte[] enc = encoder.encode(data);

        producer.produce(config.getKafkaTopic(), KafkaMessage.of(enc));

        logger.debug("Data successfully sent to topic: {}", config.getKafkaTopic());

        if (config.getLogsEnabled())
        {
            logger.info(data.toString());
        }
    }
}

