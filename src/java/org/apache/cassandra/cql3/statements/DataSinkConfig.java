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
package org.apache.cassandra.cql3.statements;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Configuration abstraction for data sink protocols.
 * Each protocol (Kafka, Pulsar, etc.) defines its own allowlist of configuration parameters
 * and default values to prevent injection of dangerous parameters.
 */
public abstract class DataSinkConfig
{
    /**
     * Returns the set of allowed configuration parameters for this sink protocol.
     *
     * This allowlist prevents injection of dangerous parameters like class loaders.
     */
    public abstract Set<String> getAllowedParameters();

    /**
     * Returns the default configuration for this sink protocol.
     * @param host the host from the URI
     * @param port the port from the URI
     * @return default configuration map
     */
    public abstract Map<String, String> getDefaults(String host, int port);

    /**
     * Factory method to get the appropriate config for a protocol scheme.
     * @param protocol the protocol scheme (e.g., "kafka")
     * @return the corresponding DataSinkConfig implementation
     * @throws InvalidRequestException if the protocol is not supported
     */
    public static DataSinkConfig getProtocol(String protocol) throws InvalidRequestException
    {
        if (protocol == null)
            throw new InvalidRequestException("Protocol cannot be null");

        switch (protocol.toLowerCase())
        {
            case "kafka":
                return new KafkaDataSinkConfig();
            default:
                throw new InvalidRequestException(
                    String.format("Unsupported data sink protocol: '%s'. Supported protocols: kafka", protocol)
                );
        }
    }

    /**
     * Kafka data sink configuration.
     * Only allows safe Kafka producer parameters, explicitly blocking any class-loading
     * parameters that could enable arbitrary code execution.
     */
    static class KafkaDataSinkConfig extends DataSinkConfig
    {
        /**
         * Allowlist of common Kafka producer configuration parameters.
         * Excludes problematic parameters, e.g.:
         * - interceptor.classes (can execute arbitrary code)
         *  - metric.reporters (can execute arbitrary code)
         * - partitioner.class (can load malicious classes)
         * - *.class params
         */
        private static final Set<String> KAFKA_PARAMS_ALLOWLIST = ImmutableSet.of(
            // Connection settings
            "bootstrap.servers",

            // Serialization (hardcoded serializers only, no *.class params)
            "key.serializer",
            "value.serializer",

            // Reliability settings
            "acks",
            "retries",
            "retry.backoff.ms",
            "max.in.flight.requests.per.connection",
            "enable.idempotence",

            // Performance tuning
            "batch.size",
            "linger.ms",
            "buffer.memory",
            "compression.type",
            "max.request.size",

            // Timeout settings
            "request.timeout.ms",
            "delivery.timeout.ms",
            "metadata.max.age.ms",

            // Security settings
            "security.protocol",
            "sasl.mechanism",
            "sasl.jaas.config",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.key.password",

            // Internal metadata (Cassandra-specific)
            "protocol",
            "sink_type"
        );

        @Override
        public Set<String> getAllowedParameters()
        {
            return KAFKA_PARAMS_ALLOWLIST;
        }

        @Override
        public Map<String, String> getDefaults(String host, int port)
        {
            Map<String, String> defaults = new HashMap<>();

            // Set required Kafka configuration
            defaults.put("bootstrap.servers", host + ':' + (port > 0 ? port : 9092));
            defaults.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            defaults.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            // Set recommended defaults
            defaults.put("acks", "all");
            defaults.put("retries", "3");
            defaults.put("compression.type", "snappy");
            defaults.put("batch.size", "16384");
            defaults.put("protocol", "kafka");
            defaults.put("sink_type", "kafka");

            return defaults;
        }
    }
}